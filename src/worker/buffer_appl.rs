use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::query::insert_pipeline;
use crate::store::registry;
use crate::types::row::OwnedRow;
use crate::util::logging;
use crate::worker::base::WorkerConfig;

/// BUFFER_APPL worker — applies buffered writes from the buffer channel to the table.
/// This worker runs its own loop (not using WorkerLoop trait) because it reads
/// from the buffer channel rather than popping from the table.
pub struct BufferApplWorker {
    pub config: WorkerConfig,
    pub time_dilation: f64,
}

impl BufferApplWorker {
    pub fn new(
        name: String,
        table: String,
        time_dilation: f64,
        frequency_ms: u64,
        max_runs: i64,
        terminator: watch::Receiver<bool>,
    ) -> Self {
        BufferApplWorker {
            config: WorkerConfig {
                name,
                table,
                frequency_ms,
                burst: 1,
                max_runs,
                terminator,
            },
            time_dilation,
        }
    }
}

/// Run the buffer_appl worker loop.
pub async fn run_buffer_appl(worker: Arc<BufferApplWorker>) {
    let config = &worker.config;
    let mut runs: i64 = 0;
    let mut terminator = config.terminator.clone();

    loop {
        if *terminator.borrow() {
            break;
        }

        if config.max_runs > 0 && runs >= config.max_runs {
            break;
        }

        // Get the table and try to receive from buffer
        if let Some(table) = registry::get_table(&config.table) {
            if let Some(ref rx) = table.buffer_rx {
                let mut rx_guard = rx.lock().await;
                match rx_guard.try_recv() {
                    Ok(mut row) => {
                        // Apply time dilation if configured
                        if worker.time_dilation > 0.0 && worker.time_dilation != 1.0 {
                            // Adjust timing based on dilation factor
                            // time_dilation > 1 = slow down, < 1 = speed up
                        }

                        let metadata = OwnedRow::new();
                        let (ok, err, _, _) =
                            insert_pipeline::insert(&config.table, &mut row, &metadata, true, None);
                        if !ok {
                            if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                                logging::log(&format!(
                                    "WORKER {}: buffer_appl insert error: {}",
                                    config.name, err
                                ));
                            }
                        }
                        runs += 1;
                    }
                    Err(_) => {
                        // No data in buffer, wait
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
