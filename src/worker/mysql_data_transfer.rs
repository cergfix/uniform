
use tokio::sync::watch;

use crate::query::engine;
use crate::server::connection::Connection;
use crate::types::row::OwnedRow;
use crate::types::value::Value;
use crate::util::logging;
use crate::worker::base::{WorkerConfig, WorkerLoop};

/// Transfer mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMode {
    OneWayPush,
    OneWayPull,
    TwoWayPush,
    TwoWayPull,
    IoMixPush,
    IoMixPull,
}

impl TransferMode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "1_WAY_PUSH" => Some(TransferMode::OneWayPush),
            "1_WAY_PULL" => Some(TransferMode::OneWayPull),
            "2_WAY_PUSH" => Some(TransferMode::TwoWayPush),
            "2_WAY_PULL" => Some(TransferMode::TwoWayPull),
            "IO_MIX_PUSH" => Some(TransferMode::IoMixPush),
            "IO_MIX_PULL" => Some(TransferMode::IoMixPull),
            _ => None,
        }
    }
}

/// MYSQL_DATA_TRANSFER worker — bidirectional data sync with remote MySQL.
pub struct MysqlDataTransferWorker {
    pub config: WorkerConfig,
    pub query: String,
    pub dest_table: String,
    pub mode: TransferMode,
    pub remote_dsn: String,
    pub remote_table: String,
    pub reply_table: String,
}

impl MysqlDataTransferWorker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        table: String,
        query: String,
        dest_table: String,
        mode: TransferMode,
        remote_dsn: String,
        remote_table: String,
        reply_table: String,
        frequency_ms: u64,
        burst: i32,
        max_runs: i64,
        terminator: watch::Receiver<bool>,
    ) -> Self {
        MysqlDataTransferWorker {
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
            mode,
            remote_dsn,
            remote_table,
            reply_table,
        }
    }
}

impl WorkerLoop for MysqlDataTransferWorker {
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String> {
        let mut local_conn = Connection::new_local();

        for row in &rows {
            match self.mode {
                TransferMode::OneWayPush | TransferMode::TwoWayPush | TransferMode::IoMixPush => {
                    // Build INSERT query for remote MySQL
                    let insert_query = build_insert_query(&self.remote_table, row);

                    // TODO: Execute on remote MySQL using sqlx
                    // For now, log the attempt
                    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                        logging::log(&format!(
                            "WORKER {}: MySQL push to {} ({})",
                            self.config.name, self.remote_dsn, insert_query.len()
                        ));
                    }
                }
                TransferMode::OneWayPull | TransferMode::TwoWayPull | TransferMode::IoMixPull => {
                    // Execute configured query locally
                    let q =
                        crate::util::template::build_string_template(&self.query, &row.columns);
                    let result = engine::run_query(&q, None, &mut local_conn.metadata, None);

                    if !result.success {
                        if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                            logging::log(&format!(
                                "WORKER {}: local query error: {}",
                                self.config.name, result.err
                            ));
                        }
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

/// Build an INSERT query from an OwnedRow.
fn build_insert_query(table: &str, row: &OwnedRow) -> String {
    let mut columns = Vec::new();
    let mut values = Vec::new();

    for (key, val) in &row.columns {
        columns.push(format!("`{}`", key));
        match val {
            Value::String(s) => {
                values.push(format!("'{}'", crate::util::escape::mysql_escape(s)));
            }
            Value::Number(n) => {
                values.push(n.to_string());
            }
            Value::Bool(b) => {
                values.push(if *b { "1".into() } else { "0".into() });
            }
            Value::Null => {
                values.push("NULL".into());
            }
        }
    }

    format!(
        "INSERT INTO `{}` ({}) VALUES ({})",
        table,
        columns.join(", "),
        values.join(", ")
    )
}
