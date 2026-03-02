use std::sync::atomic::Ordering;
use std::sync::Arc;


use crate::config::vars;
use crate::query::engine;
use crate::server::connection::Connection;
use crate::server::mysql::{constants::*, protocol, resultset};
use crate::store::registry::Server;
use crate::types::query_response::QueryType;
use crate::types::row::OwnedRow;
use crate::util::logging;

/// Handle a MySQL protocol connection.
pub async fn handle_connection(server: Arc<Server>, mut conn: Connection) {
    let connection_id = server.connection_count.load(Ordering::Relaxed) as u32;

    // Send handshake
    let handshake = protocol::build_handshake(connection_id);
    {
        let mut stream = conn.get_stream().lock().await;
        if let Err(e) = protocol::write_packet(&mut *stream, 0, &handshake).await {
            logging::log(&format!("MYSQL: handshake write error: {}", e));
            return;
        }
    }

    // Read handshake response (we don't validate auth)
    {
        let mut stream = conn.get_stream().lock().await;
        match protocol::read_packet(&mut *stream).await {
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
        let mut stream = conn.get_stream().lock().await;
        if let Err(e) = protocol::write_packet(&mut *stream, 2, &ok).await {
            logging::log(&format!("MYSQL: OK write error: {}", e));
            return;
        }
    }

    // Main command loop
    loop {
        if server.is_shutting_down() || conn.terminated {
            break;
        }

        // Read command packet
        let (seq_id, payload) = {
            let mut stream = conn.get_stream().lock().await;
            match protocol::read_packet(&mut *stream).await {
                Ok(p) => p,
                Err(_) => break,
            }
        };

        if payload.is_empty() {
            break;
        }

        let cmd_byte = payload[0];

        match cmd_byte {
            COM_QUIT => break,
            COM_PING => {
                let ok = protocol::build_ok_packet(0, 0);
                let mut stream = conn.get_stream().lock().await;
                let _ = protocol::write_packet(&mut *stream, seq_id + 1, &ok).await;
            }
            COM_INIT_DB => {
                let ok = protocol::build_ok_packet(0, 0);
                let mut stream = conn.get_stream().lock().await;
                let _ = protocol::write_packet(&mut *stream, seq_id + 1, &ok).await;
            }
            COM_QUERY => {
                let query = String::from_utf8_lossy(&payload[1..]).to_string();

                let start = std::time::Instant::now();
                // Extract metadata to avoid simultaneous &mut/& borrow of conn
                let mut metadata = std::mem::replace(&mut conn.metadata, OwnedRow::new());
                let result = engine::run_query(&query, Some(&server), &mut metadata, None);
                conn.metadata = metadata;
                let elapsed = start.elapsed();

                let mut stream = conn.get_stream().lock().await;

                if !result.success {
                    let err = protocol::build_err_packet(1064, &result.err);
                    let _ = protocol::write_packet(&mut *stream, seq_id + 1, &err).await;
                } else {
                    match result.query_type {
                        QueryType::Result => {
                            let _ = resultset::write_result_set(
                                &mut *stream,
                                seq_id + 1,
                                &result.result,
                            )
                            .await;
                        }
                        _ => {
                            let ok = protocol::build_ok_packet(
                                result.affected_rows as u64,
                                0,
                            );
                            let _ = protocol::write_packet(&mut *stream, seq_id + 1, &ok).await;
                        }
                    }
                }

                crate::util::metrics::publish_server_latency_probe(&server, elapsed, "");
            }
            _ => {
                // Unknown command — send error
                let err = protocol::build_err_packet(1047, "Unknown command");
                let mut stream = conn.get_stream().lock().await;
                let _ = protocol::write_packet(&mut *stream, seq_id + 1, &err).await;
            }
        }

        if !vars::persist_metadata() {
            conn.metadata = OwnedRow::new();
        }

        conn.last_activity = chrono::Utc::now();
    }
}
