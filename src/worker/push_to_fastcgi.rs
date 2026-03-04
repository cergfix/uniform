use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use tokio::sync::watch;

use crate::types::row::OwnedRow;
use crate::types::value::Value;
use crate::util::logging;
use crate::worker::base::{WorkerConfig, WorkerLoop};

// FastCGI protocol constants
const FCGI_VERSION: u8 = 1;
const FCGI_BEGIN_REQUEST: u8 = 1;
const FCGI_END_REQUEST: u8 = 3;
const FCGI_PARAMS: u8 = 4;
const FCGI_STDIN: u8 = 5;
const FCGI_STDOUT: u8 = 6;
const FCGI_RESPONDER: u16 = 1;

/// PUSH_TO_FASTCGI worker — sends requests to a FastCGI backend.
pub struct PushToFastcgiWorker {
    pub config: WorkerConfig,
    pub query: String,
    pub dest_table: String,
    pub reply_table: String,
    pub host: String,
    pub port: u16,
    pub script_filename: String,
    pub two_way: bool,
}

impl PushToFastcgiWorker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        table: String,
        query: String,
        dest_table: String,
        reply_table: String,
        host: String,
        port: u16,
        script_filename: String,
        two_way: bool,
        frequency_ms: u64,
        burst: i32,
        max_runs: i64,
        terminator: watch::Receiver<bool>,
    ) -> Self {
        PushToFastcgiWorker {
            config: WorkerConfig {
                name,
                table,
                frequency_ms,
                burst,
                max_runs,
                terminator,
            },
            query,
            dest_table,
            reply_table,
            host,
            port,
            script_filename,
            two_way,
        }
    }
}

impl WorkerLoop for PushToFastcgiWorker {
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String> {
        for row in &rows {
            // Build CGI environment from row columns
            let mut env: HashMap<String, String> = HashMap::new();

            for (key, val) in &row.columns {
                let s = match val {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => String::new(),
                };
                env.insert(key.clone(), s);
            }

            // Ensure SCRIPT_FILENAME is set
            if !self.script_filename.is_empty() {
                env.entry("SCRIPT_FILENAME".into())
                    .or_insert_with(|| self.script_filename.clone());
            }

            // Extract body from u_body (base64 encoded)
            let body = env
                .get("u_body")
                .and_then(|b64| BASE64.decode(b64).ok())
                .unwrap_or_default();

            let method = env
                .get("REQUEST_METHOD")
                .cloned()
                .unwrap_or_else(|| "GET".into());

            // Connect and send FastCGI request
            let addr = format!("{}:{}", self.host, self.port);
            let mut stream =
                TcpStream::connect(&addr).map_err(|e| format!("FastCGI connect error: {}", e))?;
            stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
            stream.set_write_timeout(Some(Duration::from_secs(30))).ok();

            let request_id: u16 = 1;

            // Send FCGI_BEGIN_REQUEST
            let begin_body = build_begin_request(FCGI_RESPONDER, 0);
            write_record(&mut stream, FCGI_BEGIN_REQUEST, request_id, &begin_body)?;

            // Send FCGI_PARAMS
            let params_data = encode_params(&env);
            write_record(&mut stream, FCGI_PARAMS, request_id, &params_data)?;
            // Empty FCGI_PARAMS to signal end
            write_record(&mut stream, FCGI_PARAMS, request_id, &[])?;

            // Send FCGI_STDIN (body for POST, empty for GET)
            if method.to_uppercase() != "GET" && !body.is_empty() {
                write_record(&mut stream, FCGI_STDIN, request_id, &body)?;
            }
            // Empty FCGI_STDIN to signal end
            write_record(&mut stream, FCGI_STDIN, request_id, &[])?;

            // Read response if two-way mode
            if self.two_way && !self.reply_table.is_empty() {
                match read_fcgi_response(&mut stream) {
                    Ok(response_body) => {
                        if let Some(table) = crate::store::registry::get_table(&self.reply_table) {
                            let mut reply_row = OwnedRow::new();
                            // Link reply to original request via u_reply_id
                            if let Some(Value::String(id)) = row.columns.get("u_id") {
                                reply_row
                                    .columns
                                    .insert("u_reply_id".into(), Value::String(id.clone()));
                            }
                            reply_row.columns.insert(
                                "u_body".into(),
                                Value::String(BASE64.encode(&response_body)),
                            );
                            let _ = table.insert(reply_row.columns);
                        }
                    }
                    Err(e) => {
                        if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                            logging::log(&format!(
                                "WORKER {}: FastCGI response error: {}",
                                self.config.name, e
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn config(&self) -> &WorkerConfig {
        &self.config
    }
}

/// Build FCGI_BEGIN_REQUEST body (8 bytes).
fn build_begin_request(role: u16, flags: u8) -> [u8; 8] {
    let r = role.to_be_bytes();
    [r[0], r[1], flags, 0, 0, 0, 0, 0]
}

/// Build a FastCGI record header (8 bytes).
fn build_header(rec_type: u8, request_id: u16, content_length: u16, padding: u8) -> [u8; 8] {
    let ri = request_id.to_be_bytes();
    let cl = content_length.to_be_bytes();
    [
        FCGI_VERSION,
        rec_type,
        ri[0],
        ri[1],
        cl[0],
        cl[1],
        padding,
        0,
    ]
}

/// Write a complete FastCGI record to the stream.
fn write_record(
    stream: &mut TcpStream,
    rec_type: u8,
    request_id: u16,
    content: &[u8],
) -> Result<(), String> {
    let content_len = content.len();
    let padding = (8 - (content_len % 8)) % 8;

    let header = build_header(rec_type, request_id, content_len as u16, padding as u8);
    stream
        .write_all(&header)
        .map_err(|e| format!("write header: {}", e))?;

    if !content.is_empty() {
        stream
            .write_all(content)
            .map_err(|e| format!("write content: {}", e))?;
    }

    if padding > 0 {
        let pad = vec![0u8; padding];
        stream
            .write_all(&pad)
            .map_err(|e| format!("write padding: {}", e))?;
    }

    Ok(())
}

/// Encode FastCGI name-value pairs.
fn encode_params(params: &HashMap<String, String>) -> Vec<u8> {
    let mut buf = Vec::new();
    for (name, value) in params {
        encode_length(&mut buf, name.len());
        encode_length(&mut buf, value.len());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(value.as_bytes());
    }
    buf
}

/// Encode a FastCGI variable-length integer (1 or 4 bytes).
fn encode_length(buf: &mut Vec<u8>, len: usize) {
    if len < 128 {
        buf.push(len as u8);
    } else {
        buf.push(((len >> 24) as u8) | 0x80);
        buf.push((len >> 16) as u8);
        buf.push((len >> 8) as u8);
        buf.push(len as u8);
    }
}

/// Read FastCGI response records, collecting STDOUT content.
fn read_fcgi_response(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut stdout_data = Vec::new();
    let mut header_buf = [0u8; 8];

    loop {
        if stream.read_exact(&mut header_buf).is_err() {
            break;
        }

        let rec_type = header_buf[1];
        let content_length = u16::from_be_bytes([header_buf[4], header_buf[5]]) as usize;
        let padding_length = header_buf[6] as usize;

        let total = content_length + padding_length;
        let mut content = vec![0u8; total];
        if total > 0 && stream.read_exact(&mut content).is_err() {
            break;
        }

        match rec_type {
            FCGI_STDOUT => {
                if content_length > 0 {
                    stdout_data.extend_from_slice(&content[..content_length]);
                }
            }
            FCGI_END_REQUEST => {
                break;
            }
            _ => {}
        }
    }

    Ok(stdout_data)
}
