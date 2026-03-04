use std::collections::HashMap;
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::query::insert_pipeline;
use crate::server::connection::Connection;
use crate::store::registry::Server;
use crate::types::row::OwnedRow;
use crate::types::value::Value;
use crate::util::logging;

// FastCGI protocol constants
const FCGI_BEGIN_REQUEST: u8 = 1;
const FCGI_ABORT_REQUEST: u8 = 2;
const FCGI_END_REQUEST: u8 = 3;
const FCGI_PARAMS: u8 = 4;
const FCGI_STDIN: u8 = 5;
const FCGI_STDOUT: u8 = 6;
#[allow(dead_code)]
const FCGI_STDERR: u8 = 7;

const FCGI_RESPONDER: u16 = 1;
const FCGI_REQUEST_COMPLETE: u8 = 0;

const FCGI_KEEP_CONN: u8 = 1;
const FCGI_HEADER_LEN: usize = 8;

/// FastCGI record header.
struct FcgiHeader {
    version: u8,
    rec_type: u8,
    request_id: u16,
    content_length: u16,
    padding_length: u8,
}

/// Parse a FastCGI record header from 8 bytes.
fn parse_header(buf: &[u8]) -> FcgiHeader {
    FcgiHeader {
        version: buf[0],
        rec_type: buf[1],
        request_id: u16::from_be_bytes([buf[2], buf[3]]),
        content_length: u16::from_be_bytes([buf[4], buf[5]]),
        padding_length: buf[6],
    }
}

/// Build a FastCGI record header.
fn build_header(rec_type: u8, request_id: u16, content_length: u16, padding_length: u8) -> [u8; 8] {
    let cl = content_length.to_be_bytes();
    let ri = request_id.to_be_bytes();
    [1, rec_type, ri[0], ri[1], cl[0], cl[1], padding_length, 0]
}

/// Parse FastCGI name-value pairs from a params body.
fn parse_params(data: &[u8]) -> HashMap<String, String> {
    let mut params = HashMap::new();
    let mut pos = 0;

    while pos < data.len() {
        // Name length
        let (name_len, consumed) = read_fcgi_length(data, pos);
        pos += consumed;
        if pos >= data.len() {
            break;
        }

        // Value length
        let (value_len, consumed) = read_fcgi_length(data, pos);
        pos += consumed;

        if pos + name_len + value_len > data.len() {
            break;
        }

        let name = String::from_utf8_lossy(&data[pos..pos + name_len]).to_string();
        pos += name_len;

        let value = String::from_utf8_lossy(&data[pos..pos + value_len]).to_string();
        pos += value_len;

        params.insert(name, value);
    }

    params
}

/// Read a FastCGI variable-length integer (1 or 4 bytes).
fn read_fcgi_length(data: &[u8], pos: usize) -> (usize, usize) {
    if pos >= data.len() {
        return (0, 1);
    }
    if data[pos] >> 7 == 0 {
        // 1-byte length
        (data[pos] as usize, 1)
    } else if pos + 4 <= data.len() {
        // 4-byte length
        let len = ((data[pos] & 0x7F) as usize) << 24
            | (data[pos + 1] as usize) << 16
            | (data[pos + 2] as usize) << 8
            | data[pos + 3] as usize;
        (len, 4)
    } else {
        (0, 1)
    }
}

/// Handle a FastCGI connection.
pub async fn handle_connection(server: Arc<Server>, conn: Arc<Mutex<Connection>>) {
    let mut header_buf = [0u8; FCGI_HEADER_LEN];
    let mut params_data: Vec<u8> = Vec::new();
    let mut stdin_data: Vec<u8> = Vec::new();
    let mut request_id: u16 = 0;
    let mut keep_conn = false;

    // Clone stream Arc once for all I/O
    let stream_arc = {
        let c = conn.lock().await;
        c.get_stream().clone()
    };

    loop {
        {
            let mut c = conn.lock().await;
            if server.is_shutting_down() || c.terminated {
                break;
            }
            c.state_text = "READ_FCGI".to_string();
        }

        // Reset for new request
        params_data.clear();
        stdin_data.clear();
        let mut got_begin = false;
        let mut params_done = false;
        let mut stdin_done = false;
        let mut terminated = false;

        // Read records until we have a complete request
        loop {
            // Read header
            let n = {
                let mut stream = stream_arc.lock().await;
                match stream.read_exact(&mut header_buf).await {
                    Ok(_) => FCGI_HEADER_LEN,
                    Err(_) => 0,
                }
            };

            if n == 0 {
                terminated = true;
                break;
            }

            let header = parse_header(&header_buf);

            // Validate version
            if header.version != 1 {
                if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                    logging::log(&format!(
                        "FASTCGI SERVER: unsupported version {}",
                        header.version
                    ));
                }
                terminated = true;
                break;
            }

            // Read content + padding
            let total_len = header.content_length as usize + header.padding_length as usize;
            let mut content = vec![0u8; total_len];
            if total_len > 0 {
                let read_ok = {
                    let mut stream = stream_arc.lock().await;
                    stream.read_exact(&mut content).await.is_ok()
                };
                if !read_ok {
                    terminated = true;
                    break;
                }
            }

            let content_data = &content[..header.content_length as usize];

            match header.rec_type {
                FCGI_BEGIN_REQUEST => {
                    if content_data.len() >= 8 {
                        let role = u16::from_be_bytes([content_data[0], content_data[1]]);
                        let flags = content_data[2];
                        keep_conn = (flags & FCGI_KEEP_CONN) != 0;
                        request_id = header.request_id;
                        got_begin = true;

                        if role != FCGI_RESPONDER {
                            if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                                logging::log(&format!("FASTCGI SERVER: unsupported role {}", role));
                            }
                            send_end_request(&stream_arc, request_id, 0, 3).await;
                            continue;
                        }
                    }
                }
                FCGI_PARAMS => {
                    if header.content_length == 0 {
                        params_done = true;
                    } else {
                        params_data.extend_from_slice(content_data);
                    }
                }
                FCGI_STDIN => {
                    if header.content_length == 0 {
                        stdin_done = true;
                    } else {
                        stdin_data.extend_from_slice(content_data);
                    }
                }
                FCGI_ABORT_REQUEST => {
                    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                        logging::log("FASTCGI SERVER: abort request");
                    }
                    terminated = true;
                    break;
                }
                _ => {
                    // Unknown record type, skip
                }
            }

            // If we have all parts, process the request
            if got_begin && params_done && stdin_done {
                break;
            }
        }

        if terminated {
            let mut c = conn.lock().await;
            c.terminated = true;
            break;
        }

        if !params_done {
            break;
        }

        // Process the request
        let start = std::time::Instant::now();
        {
            let mut c = conn.lock().await;
            c.state_executing = true;
            c.state_text = "RUNNING_FCGI".to_string();
        }

        let params = parse_params(&params_data);

        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log("FASTCGI SERVER: begin request");
        }

        // Build row from params + body (same as Go version)
        let mut row = OwnedRow::new();

        for (k, v) in &params {
            if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                logging::log(&format!("FASTCGI SERVER: request param, {}={}", k, v));
            }

            if let Ok(f) = v.parse::<f64>() {
                row.columns.insert(k.clone(), Value::Number(f));
            } else if v.eq_ignore_ascii_case("true") {
                row.columns.insert(k.clone(), Value::Bool(true));
            } else if v.eq_ignore_ascii_case("false") {
                row.columns.insert(k.clone(), Value::Bool(false));
            } else {
                row.columns.insert(k.clone(), Value::String(v.clone()));
            }
        }

        // Encode body as base64 u_body
        if !stdin_data.is_empty() {
            row.columns
                .insert("u_body".into(), Value::String(BASE64.encode(&stdin_data)));
        }

        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log("FASTCGI SERVER: running insert");
        }

        // Run insert pipeline
        let metadata = {
            let c = conn.lock().await;
            c.metadata.clone()
        };
        let (ok, err_str, result, insert_id) =
            insert_pipeline::insert(&server.table, &mut row, &metadata, false, None);

        {
            let c = conn.lock().await;
            if c.terminated {
                break;
            }
        }

        // Build FastCGI response
        let mut response_headers: Vec<(String, String)> = Vec::new();
        let mut status_code = 200u16;
        let mut response_body: Vec<u8> = Vec::new();

        response_headers.push(("u-reply-id".into(), insert_id.clone()));

        if let (true, Some(result_rows)) = (ok, result) {
            if !result_rows.is_empty() {
                let first_row = &result_rows[0];

                if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                    logging::log("FASTCGI SERVER: RPUSH OK, reading response and setting headers.");
                }

                // Set response headers from row columns (underscores → dashes for HTTP)
                for (k, v) in &first_row.columns {
                    let k_lower = k.to_lowercase();
                    if k_lower != "u_body" && k_lower != "status" {
                        let val_str = match v {
                            Value::String(s) => s.clone(),
                            Value::Number(n) => n.to_string(),
                            Value::Bool(b) => b.to_string(),
                            Value::Null => continue,
                        };
                        let header_name = k.replace('_', "-");
                        response_headers.push((header_name, val_str));
                    }
                }

                // Status code
                if let Some(status_val) = first_row.columns.get("Status") {
                    match status_val {
                        Value::Number(n) => {
                            status_code = *n as u16;
                        }
                        Value::String(s) => {
                            let parts: Vec<&str> = s.split_whitespace().collect();
                            if let Some(code_str) = parts.first() {
                                status_code = code_str.parse().unwrap_or(500);
                            }
                        }
                        _ => {}
                    }
                }

                // Body (base64 decoded u_body)
                if let Some(Value::String(body_b64)) = first_row.columns.get("u_body") {
                    match BASE64.decode(body_b64.as_bytes()) {
                        Ok(decoded) => response_body = decoded,
                        Err(_) => {
                            if logging::get_log_level() >= logging::LOG_LEVEL_CRIT {
                                logging::log(
                                    "FASTCGI SERVER: unable to parse u_body, sending empty body",
                                );
                            }
                        }
                    }
                }
            } else {
                // Empty response
                if logging::get_log_level() >= logging::LOG_LEVEL_CRIT {
                    logging::log(
                        "FASTCGI SERVER: request success OK, but response is empty. Assuming 1_WAY mode.",
                    );
                }
                status_code = 504;
            }
        } else {
            if logging::get_log_level() >= logging::LOG_LEVEL_CRIT {
                logging::log(&format!(
                    "FASTCGI SERVER: status false or empty response, {}",
                    err_str
                ));
            }
            status_code = 500;
        }

        // Build HTTP response for FCGI STDOUT
        let mut http_response = format!("Status: {}\r\n", status_code);
        for (k, v) in &response_headers {
            http_response.push_str(&format!("{}: {}\r\n", k, v));
        }
        http_response.push_str("\r\n");

        let mut stdout_data = http_response.into_bytes();
        stdout_data.extend_from_slice(&response_body);

        // Send FCGI_STDOUT records (chunk if > 65535)
        send_fcgi_stream(&stream_arc, FCGI_STDOUT, request_id, &stdout_data).await;
        // Empty FCGI_STDOUT to signal end
        send_fcgi_stream(&stream_arc, FCGI_STDOUT, request_id, &[]).await;
        // Send FCGI_END_REQUEST
        send_end_request(&stream_arc, request_id, 0, FCGI_REQUEST_COMPLETE).await;

        // Log latency
        let elapsed = start.elapsed();
        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log(&format!(
                "FASTCGI SERVER: request completed in {:?}, id={}",
                elapsed, insert_id
            ));
        }

        {
            let mut c = conn.lock().await;
            c.state_executing = false;
            c.state_text = "IDLE".to_string();
        }

        if !keep_conn {
            break;
        }
    }
}

/// Send a FCGI stream (STDOUT/STDERR) in chunks.
async fn send_fcgi_stream(
    stream_arc: &Arc<Mutex<TcpStream>>,
    rec_type: u8,
    request_id: u16,
    data: &[u8],
) {
    let mut stream = stream_arc.lock().await;

    if data.is_empty() {
        // Send empty record to signal end of stream
        let header = build_header(rec_type, request_id, 0, 0);
        let _ = stream.write_all(&header).await;
        return;
    }

    let mut offset = 0;
    while offset < data.len() {
        let chunk_len = std::cmp::min(data.len() - offset, 65535);
        let padding = (8 - (chunk_len % 8)) % 8;
        let header = build_header(rec_type, request_id, chunk_len as u16, padding as u8);
        let _ = stream.write_all(&header).await;
        let _ = stream.write_all(&data[offset..offset + chunk_len]).await;
        if padding > 0 {
            let pad = vec![0u8; padding];
            let _ = stream.write_all(&pad).await;
        }
        offset += chunk_len;
    }
}

/// Send FCGI_END_REQUEST record.
async fn send_end_request(
    stream_arc: &Arc<Mutex<TcpStream>>,
    request_id: u16,
    app_status: u32,
    protocol_status: u8,
) {
    let mut stream = stream_arc.lock().await;
    let header = build_header(FCGI_END_REQUEST, request_id, 8, 0);
    let as_bytes = app_status.to_be_bytes();
    let body = [
        as_bytes[0],
        as_bytes[1],
        as_bytes[2],
        as_bytes[3],
        protocol_status,
        0,
        0,
        0,
    ];
    let _ = stream.write_all(&header).await;
    let _ = stream.write_all(&body).await;
}
