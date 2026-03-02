
use tokio::sync::watch;

use crate::types::row::OwnedRow;
use crate::util::logging;
use crate::worker::base::{WorkerConfig, WorkerLoop};

/// PUSH_TO_FASTCGI worker — sends requests to a FastCGI backend.
pub struct PushToFastcgiWorker {
    pub config: WorkerConfig,
    pub query: String,
    pub dest_table: String,
    pub host: String,
    pub port: u16,
    pub script_filename: String,
}

impl PushToFastcgiWorker {
    pub fn new(
        name: String,
        table: String,
        query: String,
        dest_table: String,
        host: String,
        port: u16,
        script_filename: String,
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
            host,
            port,
            script_filename,
        }
    }
}

impl WorkerLoop for PushToFastcgiWorker {
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String> {
        // FastCGI client protocol implementation
        // For each row, send as a FastCGI request and optionally capture response
        for row in &rows {
            let json_str = row.to_json().map_err(|e| format!("JSON error: {}", e))?;

            // TODO: implement FastCGI client protocol
            // For now, log the attempt
            if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                logging::log(&format!(
                    "WORKER {}: FastCGI request to {}:{} ({})",
                    self.config.name,
                    self.host,
                    self.port,
                    json_str.len()
                ));
            }
        }
        Ok(())
    }

    fn config(&self) -> &WorkerConfig {
        &self.config
    }
}
