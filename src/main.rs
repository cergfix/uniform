use std::io::BufRead;

use uniform::config::vars;
use uniform::query::engine;
use uniform::store::registry;
use uniform::store::worker;
use uniform::types::row::OwnedRow;
use uniform::util::logging;

#[tokio::main]
async fn main() {
    // Print banner
    println!();
    println!(" _   _  _  _  ___  ___  ___  ___  __  __ ");
    println!("| | | || \\| ||_ _|| __|| _ || _ \\|  \\/  |");
    println!("| |_| || .  | | | | _| |   ||   /| |\\/| |");
    println!(" \\___/ |_|\\_||___||_|  |_|_||_|_\\|_|  |_|");
    println!();
    println!("Welcome to Uniform.");
    println!("Version: {}", vars::version());
    println!();
    println!("{}", vars::COPYRIGHT);
    println!();
    println!("Knowledge base: {}", vars::KNOWLEDGE_BASE_URL);
    println!("Support: {}", vars::CONTACT);
    println!("Web: {}", vars::WEB_URL);

    if !vars::client().is_empty() {
        println!();
        println!("Build association: {}", vars::client());
        println!();
    }

    println!("- - -");
    println!();

    // Set default log level
    logging::set_log_level(logging::LOG_LEVEL_ERROR);

    println!("SYSTEM_EVENT - Up and running!");

    // Set up shutdown signal handler
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    // Signal handler
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("Failed to set up SIGTERM handler");
        let mut sighup =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
                .expect("Failed to set up SIGHUP handler");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let _ = shutdown_tx_clone.send(true);
            }
            _ = sigterm.recv() => {
                let _ = shutdown_tx_clone.send(true);
            }
            _ = sighup.recv() => {
                // SIGHUP: could reload config, for now just continue
            }
        }
    });

    // Start stdin REPL in a background thread (stdin is blocking)
    let shutdown_tx_stdin = shutdown_tx.clone();
    std::thread::spawn(move || {
        stdin_routine(shutdown_tx_stdin);
    });

    // Wait for shutdown signal
    let _ = shutdown_rx.changed().await;

    println!("SYSTEM_EVENT - shutting down ..");

    // Graceful shutdown
    shutdown_servers();
    worker::shutdown_workers();

    println!("SYSTEM_EVENT - shutdown ok.");
}

/// Stdin REPL — reads commands from stdin and executes them.
/// Matches Go's initStdinRoutine().
fn stdin_routine(shutdown_tx: tokio::sync::watch::Sender<bool>) {
    let stdin = std::io::stdin();
    let reader = stdin.lock();
    let mut metadata = OwnedRow::new();

    for line in reader.lines() {
        match line {
            Ok(query) => {
                let query = query.trim().to_string();
                if query.is_empty() {
                    continue;
                }

                // Check for shutdown
                if query.to_uppercase() == "SHUTDOWN" {
                    let _ = shutdown_tx.send(true);
                    break;
                }

                let result = engine::run_query(&query, None, &mut metadata, None);

                // For stdin, print errors if any
                if !result.success && !result.err.is_empty() {
                    eprintln!("ERROR: {}", result.err);
                }

                // Reset metadata if not persistent
                if !vars::persist_metadata() {
                    metadata = OwnedRow::new();
                }
            }
            Err(_) => break, // EOF or error
        }
    }
}

/// Shut down all servers (HTTP/FastCGI first, then Redis/MySQL).
fn shutdown_servers() {
    use registry::{ServerProtocol, SERVERS};

    // First: HTTP/FastCGI
    let mut http_servers = Vec::new();
    let mut redis_servers = Vec::new();

    for entry in SERVERS.iter() {
        let s = entry.value();
        match s.protocol {
            ServerProtocol::Http | ServerProtocol::FastCgi => {
                http_servers.push(s.name.clone());
            }
            ServerProtocol::Redis | ServerProtocol::MySQL => {
                redis_servers.push(s.name.clone());
            }
        }
    }

    for name in &http_servers {
        if let Some(s) = registry::get_server(name) {
            s.set_shutting_down();
        }
    }

    // Then: Redis/MySQL
    for name in &redis_servers {
        if let Some(s) = registry::get_server(name) {
            s.set_shutting_down();
        }
    }
}
