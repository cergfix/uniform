use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::vars;
use crate::query::engine;
use crate::server::connection::Connection;
use crate::store::registry::Server;
use crate::types::query_response::QueryType;
use crate::types::row::OwnedRow;
use crate::util::logging;

/// Handle a Redis protocol connection.
pub async fn handle_connection(server: Arc<Server>, mut conn: Connection) {
    let mut buf = vec![0u8; server.buffer_size];

    loop {
        if server.is_shutting_down() || conn.terminated {
            break;
        }

        conn.state_text = "READ_PACKET".to_string();

        // Read from the stream
        let n = {
            let mut stream = conn.get_stream().lock().await;
            match stream.read(&mut buf).await {
                Ok(0) => break, // Connection closed
                Ok(n) => n,
                Err(_) => break,
            }
        };

        let start = std::time::Instant::now();
        conn.state_executing = true;
        conn.state_text = "RUNNING_CMD".to_string();

        let raw_cmd = String::from_utf8_lossy(&buf[..n]).to_string();

        if raw_cmd.is_empty() || raw_cmd == "\r\n" {
            conn.state_executing = false;
            continue;
        }

        // Parse RESP protocol
        let cmd = parse_resp(&raw_cmd);

        conn.query = cmd.clone();

        // Execute query — extract metadata to avoid simultaneous &mut/& borrow of conn
        let mut metadata = std::mem::replace(&mut conn.metadata, OwnedRow::new());
        let result = engine::run_query(&cmd, Some(&server), &mut metadata, None);
        conn.metadata = metadata;

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
            let mut stream = conn.get_stream().lock().await;
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

        conn.query = String::new();
        conn.state_executing = false;

        // Check for QUIT/EXIT
        let cmd_upper = cmd.to_uppercase();
        if cmd_upper.starts_with("QUIT") || cmd_upper.starts_with("EXIT") {
            break;
        }

        // Check for SHUTDOWN
        if cmd_upper.starts_with("SHUTDOWN") {
            // TODO: signal global shutdown
            break;
        }

        // Reset metadata if not persistent
        if !vars::persist_metadata() {
            conn.metadata = OwnedRow::new();
        }

        conn.last_activity = chrono::Utc::now();
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
