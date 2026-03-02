use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::vars;
use crate::query::insert_pipeline;
use crate::server::connection::Connection;
use crate::store::registry::Server;
use crate::types::row::OwnedRow;
use crate::types::value::Value;
use crate::util::logging;

/// Handle an HTTP connection.
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
                Ok(0) => break,
                Ok(n) => n,
                Err(_) => break,
            }
        };

        let start = std::time::Instant::now();
        conn.state_executing = true;
        conn.state_text = "RUNNING_CMD".to_string();

        let request = String::from_utf8_lossy(&buf[..n]).to_string();

        if request.is_empty() || request == "\r\n" {
            conn.state_executing = false;
            continue;
        }

        // Parse HTTP request
        let chunks: Vec<&str> = request.splitn(2, "\r\n\r\n").collect();
        let header_section = chunks[0];
        let body = if chunks.len() > 1 { chunks[1] } else { "" };

        let headers: Vec<&str> = header_section.split("\r\n").collect();

        let mut row = OwnedRow::new();

        // Set CGI defaults
        row.columns
            .insert("HTTPS".into(), Value::String(String::new()));
        row.columns
            .insert("PHP_SELF".into(), Value::String(String::new()));
        row.columns
            .insert("QUERY_STRING".into(), Value::String(String::new()));
        row.columns
            .insert("SCRIPT_FILENAME".into(), Value::String(String::new()));
        row.columns
            .insert("DOCUMENT_ROOT".into(), Value::String(String::new()));
        row.columns
            .insert("REQUEST_METHOD".into(), Value::String("GET".into()));
        row.columns
            .insert("CONTENT_TYPE".into(), Value::String(String::new()));
        row.columns
            .insert("REQUEST_URI".into(), Value::String(String::new()));
        row.columns
            .insert("DOCUMENT_URI".into(), Value::String(String::new()));
        row.columns.insert(
            "SERVER_PROTOCOL".into(),
            Value::String("HTTP/1.1".into()),
        );
        row.columns.insert(
            "SERVER_SOFTWARE".into(),
            Value::String(format!("{}/{}", vars::APP_NAME, vars::version())),
        );
        row.columns
            .insert("SERVER_NAME".into(), Value::String(String::new()));
        row.columns
            .insert("HTTP_USER_AGENT".into(), Value::String(String::new()));
        row.columns.insert(
            "HTTP_X_UNIFORM".into(),
            Value::String(format!("{}/{}", vars::APP_NAME, vars::version())),
        );
        row.columns
            .insert("GATEWAY_INTERFACE".into(), Value::String("CGI/1.1".into()));

        // Extract local/remote addresses
        let local_parts: Vec<&str> = conn.local_addr.rsplitn(2, ':').collect();
        if local_parts.len() == 2 {
            row.columns.insert(
                "SERVER_ADDR".into(),
                Value::String(local_parts[1].to_string()),
            );
            row.columns.insert(
                "SERVER_PORT".into(),
                Value::String(local_parts[0].to_string()),
            );
        }

        let remote_parts: Vec<&str> = conn.remote_addr.rsplitn(2, ':').collect();
        if remote_parts.len() == 2 {
            row.columns.insert(
                "REMOTE_ADDR".into(),
                Value::String(remote_parts[1].to_string()),
            );
            row.columns.insert(
                "REMOTE_PORT".into(),
                Value::String(remote_parts[0].to_string()),
            );
        }

        row.columns
            .insert("HTTP_HOST".into(), Value::String(String::new()));

        // Parse headers
        for header in &headers {
            if let Some(colon_pos) = header.find(':') {
                let key = filter_header_name(&header[..colon_pos]).to_uppercase();
                let value = header[colon_pos + 1..].trim().to_string();

                if let Ok(f) = value.parse::<f64>() {
                    row.columns.insert(key, Value::Number(f));
                } else if value == "true" {
                    row.columns.insert(key, Value::Bool(true));
                } else if value == "false" {
                    row.columns.insert(key, Value::Bool(false));
                } else {
                    row.columns.insert(key, Value::String(value));
                }
            } else if header.starts_with("GET ") || header.starts_with("POST ") {
                let parts: Vec<&str> = header.split_whitespace().collect();
                if parts.len() >= 2 {
                    let method = parts[0];
                    let location = parts[1];
                    let http_proto = if parts.len() >= 3 {
                        parts[2]
                    } else {
                        "HTTP/1.1"
                    };

                    let location_parts: Vec<&str> = location.splitn(2, '?').collect();
                    let path = location_parts[0].replace("/../", "/");
                    let query_string = if location_parts.len() > 1 {
                        location_parts[1]
                    } else {
                        ""
                    };

                    row.columns
                        .insert("REQUEST_URI".into(), Value::String(location.to_string()));
                    row.columns
                        .insert("SCRIPT_NAME".into(), Value::String(path.clone()));
                    row.columns
                        .insert("DOCUMENT_URI".into(), Value::String(path.clone()));

                    if !server.script_filename.is_empty() {
                        row.columns.insert(
                            "SCRIPT_FILENAME".into(),
                            Value::String(server.script_filename.clone()),
                        );
                    } else {
                        row.columns.insert(
                            "SCRIPT_FILENAME".into(),
                            Value::String(format!("{}/{}", server.document_root, path)),
                        );
                    }

                    row.columns.insert(
                        "DOCUMENT_ROOT".into(),
                        Value::String(server.document_root.clone()),
                    );
                    row.columns.insert(
                        "PATH_TRANSLATED".into(),
                        Value::String(server.document_root.clone()),
                    );
                    row.columns.insert(
                        "SERVER_PROTOCOL".into(),
                        Value::String(http_proto.to_string()),
                    );
                    row.columns.insert(
                        "REQUEST_METHOD".into(),
                        Value::String(method.to_string()),
                    );
                    row.columns.insert(
                        "QUERY_STRING".into(),
                        Value::String(query_string.to_string()),
                    );
                }
            }
        }

        // Set SERVER_NAME from HTTP_HOST
        if let Some(host) = row.columns.get("HTTP_HOST").cloned() {
            row.columns.insert("SERVER_NAME".into(), host);
        }

        let server_protocol = match row.columns.get("SERVER_PROTOCOL") {
            Some(Value::String(s)) => s.clone(),
            _ => "HTTP/1.1".to_string(),
        };

        // Check gzip support
        let use_gzip = server.gzip
            && matches!(
                row.columns.get("HTTP_ACCEPT_ENCODING"),
                Some(Value::String(s)) if s.to_lowercase().contains("gzip")
            );

        // HTTPS redirect check
        if server.https_redirect && !server.https_redirect_header.is_empty() {
            let header_key = filter_header_name(&server.https_redirect_header).to_uppercase();
            if let Some(Value::String(val)) = row.columns.get(&header_key) {
                if *val == server.https_redirect_on {
                    let host = match row.columns.get("HTTP_HOST") {
                        Some(Value::String(s)) => s.clone(),
                        _ => String::new(),
                    };
                    let uri = match row.columns.get("REQUEST_URI") {
                        Some(Value::String(s)) => s.clone(),
                        _ => String::new(),
                    };
                    let msg = format!(
                        "{} 302\r\nServer: {}\r\nLocation: https://{}{}\r\n\r\n",
                        server_protocol,
                        vars::APP_NAME,
                        host,
                        uri
                    );
                    {
                        let mut stream = conn.get_stream().lock().await;
                        let _ = stream.write_all(msg.as_bytes()).await;
                    }
                    let elapsed = start.elapsed();
                    crate::util::metrics::publish_server_latency_probe(&server, elapsed, "");
                    conn.state_executing = false;
                    break;
                }
            }
        }

        // robots.txt
        let doc_uri = match row.columns.get("DOCUMENT_URI") {
            Some(Value::String(s)) => s.to_lowercase(),
            _ => String::new(),
        };
        if doc_uri == "/robots.txt" {
            let msg = format!(
                "{} 200\r\nServer: {}\r\n\r\n{}\r\n",
                server_protocol,
                vars::APP_NAME,
                server.robots
            );
            {
                let mut stream = conn.get_stream().lock().await;
                let _ = stream.write_all(msg.as_bytes()).await;
            }
            let elapsed = start.elapsed();
            crate::util::metrics::publish_server_latency_probe(&server, elapsed, "");
            conn.state_executing = false;
            break;
        }

        // Base64 encode body
        row.columns
            .insert("Fb_body".into(), Value::String(BASE64.encode(body)));

        // Insert into table via pipeline
        let metadata = conn.metadata.clone();
        let (success, err_str, result, insert_id) =
            insert_pipeline::insert(&server.table, &mut row, &metadata, false, None);

        if !success {
            let msg = format!(
                "{} 500\r\nServer: {}\r\nFb_reply_id: {}\r\n\r\nSystem error.\r\n",
                server_protocol,
                vars::APP_NAME,
                insert_id
            );
            let mut stream = conn.get_stream().lock().await;
            let _ = stream.write_all(msg.as_bytes()).await;

            if logging::get_log_level() >= logging::LOG_LEVEL_CRIT {
                logging::log(&format!(
                    "HTTP SERVER: replying status 500 -- {}",
                    err_str
                ));
            }
        } else if let Some(ref rows) = result {
            if !rows.is_empty() {
                let response =
                    build_http_response(&server_protocol, &rows[0], use_gzip);
                let mut stream = conn.get_stream().lock().await;
                let _ = stream.write_all(response.as_bytes()).await;
            } else {
                // 504 - empty response (1_WAY mode)
                let msg = format!(
                    "{} 504\r\nServer: {}\r\nFb_reply_id: {}\r\n\r\n",
                    server_protocol,
                    vars::APP_NAME,
                    insert_id
                );
                let mut stream = conn.get_stream().lock().await;
                let _ = stream.write_all(msg.as_bytes()).await;
            }
        } else {
            // No result - 504
            let msg = format!(
                "{} 504\r\nServer: {}\r\nFb_reply_id: {}\r\n\r\n",
                server_protocol,
                vars::APP_NAME,
                insert_id
            );
            let mut stream = conn.get_stream().lock().await;
            let _ = stream.write_all(msg.as_bytes()).await;
        }

        let elapsed = start.elapsed();
        crate::util::metrics::publish_server_latency_probe(&server, elapsed, &insert_id);

        conn.state_executing = false;
        conn.last_activity = chrono::Utc::now();

        // HTTP/1.0 style: one request per connection
        break;
    }
}

/// Build HTTP response from a result row.
fn build_http_response(protocol: &str, row: &OwnedRow, use_gzip: bool) -> String {
    let mut msg = String::new();

    // Status line
    if let Some(status) = row.columns.get("Status") {
        msg.push_str(&format!("{} {}\r\n", protocol, status.to_string_repr()));
    } else {
        msg.push_str(&format!("{} 200\r\n", protocol));
    }

    msg.push_str(&format!(
        "Server: {}/{}\r\n",
        vars::APP_NAME,
        vars::version()
    ));

    // Headers from row columns
    for (key, val) in &row.columns {
        let key_lower = key.to_lowercase();
        if key_lower != "fb_body" && key_lower != "status" {
            msg.push_str(&format!("{}: {}\r\n", key, val.to_string_repr()));
        }
    }

    if use_gzip {
        msg.push_str("Content-Encoding: gzip\r\n");
    }

    msg.push_str("\r\n");

    // Body
    if let Some(Value::String(fb_body)) = row.columns.get("Fb_body") {
        if let Ok(decoded) = BASE64.decode(fb_body) {
            let body_str = String::from_utf8_lossy(&decoded);
            if use_gzip {
                if let Ok(compressed) = compress_http_message(&body_str) {
                    // Write raw compressed bytes
                    msg.push_str(&compressed);
                }
            } else {
                msg.push_str(&body_str);
            }
        }
    }

    msg
}

/// Gzip compress an HTTP response body.
fn compress_http_message(msg: &str) -> Result<String, String> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(msg.as_bytes())
        .map_err(|e| format!("gzip write: {}", e))?;
    let compressed = encoder
        .finish()
        .map_err(|e| format!("gzip finish: {}", e))?;
    Ok(String::from_utf8_lossy(&compressed).into_owned())
}

/// Convert HTTP header names to CGI-style (replace - with _).
fn filter_header_name(name: &str) -> String {
    name.replace('-', "_")
}
