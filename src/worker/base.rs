use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::store::registry;
use crate::types::row::OwnedRow;
use crate::util::logging;

/// Common worker loop parameters.
pub struct WorkerConfig {
    pub name: String,
    pub table: String,
    pub frequency_ms: u64,
    pub burst: i32,
    pub max_runs: i64,
    pub terminator: watch::Receiver<bool>,
}

/// Trait for worker implementations.
pub trait WorkerLoop: Send + Sync + 'static {
    /// Process a batch of rows. Called on each iteration.
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String>;

    /// Get the worker configuration.
    fn config(&self) -> &WorkerConfig;
}

/// Run a worker loop (generic for all worker types).
pub async fn run_worker_loop<W: WorkerLoop>(worker: Arc<W>) {
    let config = worker.config();
    let mut runs: i64 = 0;
    let mut terminator = config.terminator.clone();

    loop {
        // Check termination
        if *terminator.borrow() {
            break;
        }

        // Check max_runs
        if config.max_runs > 0 && runs >= config.max_runs {
            break;
        }

        // Pop rows from table
        if let Some(table) = registry::get_table(&config.table) {
            let mut rows = Vec::new();
            let burst = if config.burst > 0 {
                config.burst as usize
            } else {
                1
            };

            for _ in 0..burst {
                if let Some(row) = table.pop_one() {
                    rows.push(row);
                } else {
                    break;
                }
            }

            if !rows.is_empty() {
                match worker.process(rows) {
                    Ok(()) => {
                        runs += 1;
                    }
                    Err(e) => {
                        if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                            logging::log(&format!(
                                "WORKER {}: error: {}",
                                config.name, e
                            ));
                        }
                    }
                }
            }
        }

        // Sleep for frequency interval, or wake on termination
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(config.frequency_ms)) => {}
            _ = terminator.changed() => {
                if *terminator.borrow() {
                    break;
                }
            }
        }
    }

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!("WORKER {}: stopped", config.name));
    }
}
