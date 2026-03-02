
use tokio::sync::watch;

use crate::query::engine;
use crate::server::connection::Connection;
use crate::types::row::OwnedRow;
use crate::util::logging;
use crate::worker::base::{WorkerConfig, WorkerLoop};

/// EXEC_QUERY worker — pops rows from source table and executes a configured query.
pub struct ExecQueryWorker {
    pub config: WorkerConfig,
    pub query: String,
}

impl ExecQueryWorker {
    pub fn new(
        name: String,
        table: String,
        query: String,
        frequency_ms: u64,
        burst: i32,
        max_runs: i64,
        terminator: watch::Receiver<bool>,
    ) -> Self {
        ExecQueryWorker {
            config: WorkerConfig {
                name,
                table,
                frequency_ms,
                burst,
                max_runs,
                terminator,
            },
            query,
        }
    }
}

impl WorkerLoop for ExecQueryWorker {
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String> {
        let mut local_conn = Connection::new_local();

        for row in &rows {
            // Build query from template if needed, or use configured query
            let q = crate::util::template::build_string_template(&self.query, &row.columns);

            let result = engine::run_query(&q, None, &mut local_conn.metadata, None);

            if !result.success {
                if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                    logging::log(&format!(
                        "WORKER {}: EXEC_QUERY error: {}",
                        self.config.name, result.err
                    ));
                }
            }
        }

        Ok(())
    }

    fn config(&self) -> &WorkerConfig {
        &self.config
    }
}
