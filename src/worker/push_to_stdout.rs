use tokio::sync::watch;

use crate::types::row::OwnedRow;
use crate::util::logging;
use crate::worker::base::{WorkerConfig, WorkerLoop};

/// PUSH_TO_STDOUT worker — pops rows and prints them to stdout as JSON.
pub struct PushToStdoutWorker {
    pub config: WorkerConfig,
}

impl PushToStdoutWorker {
    pub fn new(
        name: String,
        table: String,
        frequency_ms: u64,
        burst: i32,
        max_runs: i64,
        terminator: watch::Receiver<bool>,
    ) -> Self {
        PushToStdoutWorker {
            config: WorkerConfig {
                name,
                table,
                frequency_ms,
                burst,
                max_runs,
                terminator,
            },
        }
    }
}

impl WorkerLoop for PushToStdoutWorker {
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String> {
        for row in &rows {
            match row.to_json() {
                Ok(json_str) => {
                    println!("{}", json_str);
                }
                Err(e) => {
                    if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                        logging::log(&format!(
                            "WORKER {}: JSON encode error: {}",
                            self.config.name, e
                        ));
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
