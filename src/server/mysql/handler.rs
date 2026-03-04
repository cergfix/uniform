use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::config::vars;
use crate::query::engine;
use crate::server::connection::Connection;
use crate::server::mysql::{constants::*, protocol, resultset};
use crate::store::registry::Server;
use crate::types::query_response::QueryType;
use crate::types::row::OwnedRow;
use crate::util::logging;

/// Handle a MySQL protocol connection.
pub async fn handle_connection(server: Arc<Server>, conn: Arc<Mutex<Connection>>) {
    let connection_id = server.connection_count.load(Ordering::Relaxed) as u32;

    // Clone stream Arc once for all I/O
    let stream_arc = {
        let c = conn.lock().await;
        c.get_stream().clone()
    };

    // Send handshake
    let handshake = protocol::build_handshake(connection_id);
    {
        let mut stream = stream_arc.lock().await;
        if let Err(e) = protocol::write_packet(&mut stream, 0, &handshake).await {
            logging::log(&format!("MYSQL: handshake write error: {}", e));
            return;
        }
        if let Err(e) = stream.flush().await {
            logging::log(&format!("MYSQL: handshake flush error: {}", e));
            return;
        }
    }

    // Read handshake response (we don't validate auth)
    {
        let mut stream = stream_arc.lock().await;
        match protocol::read_packet(&mut stream).await {
            Ok(_) => {}
            Err(e) => {
                logging::log(&format!("MYSQL: handshake response read error: {}", e));
                return;
            }
        }
    }

    // Send OK after handshake
    {
        let ok = protocol::build_ok_packet(0, 0);
        let mut stream = stream_arc.lock().await;
        if let Err(e) = protocol::write_packet(&mut stream, 2, &ok).await {
            logging::log(&format!("MYSQL: OK write error: {}", e));
            return;
        }
        if let Err(e) = stream.flush().await {
            logging::log(&format!("MYSQL: OK flush error: {}", e));
            return;
        }
    }

    // Main command loop
    loop {
        // Lock #1: check terminated + extract metadata
        let metadata = {
            let mut c = conn.lock().await;
            if server.is_shutting_down() || c.terminated {
                break;
            }
            std::mem::take(&mut c.metadata)
        };

        // Read command packet
        let (seq_id, payload) = {
            let mut stream = stream_arc.lock().await;
            match protocol::read_packet(&mut stream).await {
                Ok(p) => p,
                Err(_) => {
                    // Restore metadata before breaking
                    let mut c = conn.lock().await;
                    c.metadata = metadata;
                    break;
                }
            }
        };

        if payload.is_empty() {
            let mut c = conn.lock().await;
            c.metadata = metadata;
            break;
        }

        let cmd_byte = payload[0];

        match cmd_byte {
            COM_QUIT => {
                let mut c = conn.lock().await;
                c.metadata = metadata;
                break;
            }
            COM_PING => {
                let ok = protocol::build_ok_packet(0, 0);
                let mut stream = stream_arc.lock().await;
                let _ = protocol::write_packet(&mut stream, seq_id + 1, &ok).await;
                let _ = stream.flush().await;
            }
            COM_INIT_DB => {
                let ok = protocol::build_ok_packet(0, 0);
                let mut stream = stream_arc.lock().await;
                let _ = protocol::write_packet(&mut stream, seq_id + 1, &ok).await;
                let _ = stream.flush().await;
            }
            COM_QUERY => {
                let query = String::from_utf8_lossy(&payload[1..]).to_string();

                let start = std::time::Instant::now();
                let mut metadata = metadata;
                let result = engine::run_query(&query, Some(&server), &mut metadata, None);
                let elapsed = start.elapsed();

                let mut stream = stream_arc.lock().await;

                if !result.success {
                    let err = protocol::build_err_packet(1064, &result.err);
                    let _ = protocol::write_packet(&mut stream, seq_id + 1, &err).await;
                    let _ = stream.flush().await;
                } else {
                    match result.query_type {
                        QueryType::Result => {
                            let _ = resultset::write_result_set(
                                &mut stream,
                                seq_id + 1,
                                &result.result,
                            )
                            .await;
                        }
                        _ => {
                            let ok = protocol::build_ok_packet(result.affected_rows as u64, 0);
                            let _ = protocol::write_packet(&mut stream, seq_id + 1, &ok).await;
                            let _ = stream.flush().await;
                        }
                    }
                }

                drop(stream);
                crate::util::metrics::publish_server_latency_probe(&server, elapsed, "");

                // Lock #2: restore metadata + update last_activity
                {
                    let mut c = conn.lock().await;
                    if !vars::persist_metadata() {
                        c.metadata = OwnedRow::new();
                    } else {
                        c.metadata = metadata;
                    }
                    c.last_activity = chrono::Utc::now();
                }
                continue;
            }
            _ => {
                // Unknown command — send error
                let err = protocol::build_err_packet(1047, "Unknown command");
                let mut stream = stream_arc.lock().await;
                let _ = protocol::write_packet(&mut stream, seq_id + 1, &err).await;
                let _ = stream.flush().await;
            }
        }

        // Lock #2: restore metadata + update last_activity (for non-query commands)
        {
            let mut c = conn.lock().await;
            if !vars::persist_metadata() {
                c.metadata = OwnedRow::new();
            } else {
                c.metadata = metadata;
            }
            c.last_activity = chrono::Utc::now();
        }
    }
}
