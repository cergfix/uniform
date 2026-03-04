use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::config::vars;
use crate::query::engine;
use crate::server::connection::Connection;
use crate::store::registry::{Server, SHUTDOWN_TX};
use crate::types::query_response::QueryType;
use crate::types::row::OwnedRow;
use crate::util::logging;

/// Handle a Redis protocol connection.
pub async fn handle_connection(server: Arc<Server>, conn: Arc<Mutex<Connection>>) {
    let mut buf = vec![0u8; server.buffer_size];

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
            c.state_text = "READ_PACKET".to_string();
        }

        // Read from the stream (no conn lock held during I/O)
        let n = {
            let mut stream = stream_arc.lock().await;
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(_) => break,
            }
        };

        let start = std::time::Instant::now();
        {
            let mut c = conn.lock().await;
            c.state_executing = true;
            c.state_text = "RUNNING_CMD".to_string();
        }

        let raw_cmd = String::from_utf8_lossy(&buf[..n]).to_string();

        if raw_cmd.is_empty() || raw_cmd == "\r\n" {
            let mut c = conn.lock().await;
            c.state_executing = false;
            continue;
        }

        // Parse RESP protocol
        let cmd = parse_resp(&raw_cmd);

        // Extract metadata, run query, put metadata back
        let mut metadata = {
            let mut c = conn.lock().await;
            c.query = cmd.clone();
            std::mem::take(&mut c.metadata)
        };
        let result = engine::run_query(&cmd, Some(&server), &mut metadata, None);
        {
            let mut c = conn.lock().await;
            c.metadata = metadata;
        }

        // Send response
        let response = if !result.success {
            format!("-ERR {}\r\n", result.err)
        } else {
            match result.query_type {
                QueryType::Number => {
                    format!(":{}\r\n", result.value_int)
                }
                QueryType::String => {
                    format!("+{}\r\n", result.value_str)
                }
                QueryType::Result => {
                    if result.result.is_empty() {
                        "$-1\r\n".to_string()
                    } else {
                        encode_result_array(&result.result)
                    }
                }
                QueryType::Bool => {
                    if result.success {
                        "+OK\r\n".to_string()
                    } else {
                        format!("-ERR {}\r\n", result.err)
                    }
                }
            }
        };

        // Write response
        {
            let mut stream = stream_arc.lock().await;
            if let Err(e) = stream.write_all(response.as_bytes()).await {
                if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                    logging::log(&format!("REDIS: write error: {}", e));
                }
                break;
            }
        }

        // Publish latency probe
        let elapsed = start.elapsed();
        crate::util::metrics::publish_server_latency_probe(&server, elapsed, "");

        // Check for QUIT/EXIT
        let cmd_upper = cmd.to_uppercase();
        let is_quit = cmd_upper.starts_with("QUIT") || cmd_upper.starts_with("EXIT");
        let is_shutdown = cmd_upper.starts_with("SHUTDOWN");

        // Update connection state
        {
            let mut c = conn.lock().await;
            c.query = String::new();
            c.state_executing = false;
            c.last_activity = chrono::Utc::now();
            if !vars::persist_metadata() {
                c.metadata = OwnedRow::new();
            }
        }

        if is_quit {
            break;
        }

        if is_shutdown {
            if let Some(tx) = SHUTDOWN_TX.lock().as_ref() {
                let _ = tx.send(true);
            }
            break;
        }
    }
}

/// Parse a RESP protocol command.
/// Handles both inline and array formats.
fn parse_resp(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }

    if raw.starts_with('*') {
        // Array format: *N\r\n$len\r\ndata\r\n...
        let parts: Vec<&str> = raw.split("\r\n").collect();
        let mut cmd = String::new();

        let mut i = 1;
        while i < parts.len() {
            if parts[i].starts_with('$') {
                // Next part is the data
                i += 1;
                if i < parts.len() && !parts[i].is_empty() {
                    if cmd.is_empty() {
                        cmd = parts[i].to_string();
                    } else {
                        cmd.push(' ');
                        cmd.push_str(parts[i]);
                    }
                }
            }
            i += 1;
        }
        cmd
    } else {
        // Inline format — strip trailing \r\n
        raw.trim_end_matches("\r\n")
            .trim_end_matches('\n')
            .to_string()
    }
}

/// Encode a result set as a RESP array of JSON bulk strings.
fn encode_result_array(rows: &[OwnedRow]) -> String {
    let mut msg = format!("*{}\r\n", rows.len());
    for row in rows {
        match row.to_json() {
            Ok(json_str) => {
                msg.push_str(&format!("${}\r\n{}\r\n", json_str.len(), json_str));
            }
            Err(e) => {
                if logging::get_log_level() >= logging::LOG_LEVEL_CRIT {
                    logging::log(&format!("Error encoding row for redis reply: {}", e));
                }
            }
        }
    }
    msg
}
