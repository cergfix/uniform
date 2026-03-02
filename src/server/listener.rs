use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::net::TcpListener;

use crate::server::connection::Connection;
use crate::store::registry::{Server, ServerProtocol};
use crate::util::logging;

/// Start the TCP listener for a server.
pub async fn start_listener(server: Arc<Server>) {
    let listener = match TcpListener::bind(&server.bind).await {
        Ok(l) => l,
        Err(e) => {
            logging::log(&format!(
                "SERVER: couldn't bind to {}: {}",
                server.bind, e
            ));
            return;
        }
    };

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!(
            "SERVER: listening on {} (protocol={})",
            server.bind,
            server.protocol.as_str()
        ));
    }

    // Spawn connection cleanup daemon
    let server_cleanup = server.clone();
    tokio::spawn(async move {
        conn_cleanup(server_cleanup).await;
    });

    loop {
        if server.is_shutting_down() {
            break;
        }

        match listener.accept().await {
            Ok((stream, addr)) => {
                server
                    .connection_count
                    .fetch_add(1, Ordering::Relaxed);

                if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                    logging::log(&format!(
                        "SERVER: new connection from {}, proto={}",
                        addr,
                        server.protocol.as_str()
                    ));
                }

                let server_clone = server.clone();
                tokio::spawn(async move {
                    request_handler(server_clone, stream).await;
                });
            }
            Err(e) => {
                if !server.is_shutting_down() {
                    logging::log(&format!("SERVER: accept error: {}", e));
                }
            }
        }
    }
}

/// Handle a single connection — dispatch to protocol-specific handler.
async fn request_handler(server: Arc<Server>, stream: tokio::net::TcpStream) {
    let conn = Connection::new(stream, &server);

    match server.protocol {
        ServerProtocol::Redis => {
            crate::server::redis::handle_connection(server.clone(), conn).await;
        }
        ServerProtocol::MySQL => {
            crate::server::mysql::handler::handle_connection(server.clone(), conn).await;
        }
        ServerProtocol::Http => {
            crate::server::http::handle_connection(server.clone(), conn).await;
        }
        ServerProtocol::FastCgi => {
            crate::server::fastcgi::handle_connection(server.clone(), conn).await;
        }
    }

    server
        .connection_count
        .fetch_sub(1, Ordering::Relaxed);

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!(
            "SERVER: connection closed, count={}",
            server.connection_count.load(Ordering::Relaxed)
        ));
    }
}

/// Connection cleanup daemon — closes timed-out connections.
async fn conn_cleanup(server: Arc<Server>) {
    loop {
        if server.is_shutting_down() {
            break;
        }
        // TODO: track connections in a DashMap and clean up timed-out ones
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
