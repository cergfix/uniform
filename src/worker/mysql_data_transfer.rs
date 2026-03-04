use std::time::Duration;

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
    pub fn parse(s: &str) -> Option<Self> {
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
    pub database_name: String,
    pub user: String,
    pub password: String,
    pub host: String,
    pub max_request_time_ms: u64,
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
        database_name: String,
        user: String,
        password: String,
        host: String,
        max_request_time_ms: u64,
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
            database_name,
            user,
            password,
            host,
            max_request_time_ms,
        }
    }

    /// Build the MySQL connection string for sqlx.
    fn connection_url(&self) -> String {
        if !self.remote_dsn.is_empty() {
            return self.remote_dsn.clone();
        }
        if !self.user.is_empty() && !self.password.is_empty() {
            format!(
                "mysql://{}:{}@{}/{}",
                self.user, self.password, self.host, self.database_name
            )
        } else {
            format!("mysql://{}/{}", self.host, self.database_name)
        }
    }

    /// Execute a query on the remote MySQL server, returning rows.
    #[allow(dead_code)]
    async fn remote_query(&self, sql: &str) -> Result<Vec<OwnedRow>, String> {
        use sqlx::mysql::MySqlPoolOptions;
        use sqlx::{Column, Row};

        let pool = MySqlPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(self.max_request_time_ms))
            .connect(&self.connection_url())
            .await
            .map_err(|e| format!("MySQL connect error: {}", e))?;

        let rows = sqlx::query(sql)
            .fetch_all(&pool)
            .await
            .map_err(|e| format!("MySQL query error: {}", e))?;

        let mut result = Vec::new();
        for row in &rows {
            let mut owned = OwnedRow::new();
            for col in row.columns() {
                let name = col.name().to_string();
                let val: Option<String> = row.try_get(col.ordinal()).ok();
                match val {
                    Some(s) => {
                        owned.columns.insert(name, Value::String(s));
                    }
                    None => {
                        owned.columns.insert(name, Value::Null);
                    }
                }
            }
            result.push(owned);
        }

        pool.close().await;
        Ok(result)
    }

    /// Execute a non-SELECT statement on the remote MySQL server.
    async fn remote_exec(&self, sql: &str) -> Result<u64, String> {
        use sqlx::mysql::MySqlPoolOptions;

        let pool = MySqlPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_millis(self.max_request_time_ms))
            .connect(&self.connection_url())
            .await
            .map_err(|e| format!("MySQL connect error: {}", e))?;

        let result = sqlx::query(sql)
            .execute(&pool)
            .await
            .map_err(|e| format!("MySQL exec error: {}", e))?;

        let affected = result.rows_affected();
        pool.close().await;
        Ok(affected)
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

                    // Execute on remote MySQL using sqlx (blocking via tokio::runtime)
                    let rt = tokio::runtime::Handle::try_current();
                    let exec_result = match rt {
                        Ok(handle) => std::thread::scope(|_| {
                            tokio::task::block_in_place(|| {
                                handle.block_on(self.remote_exec(&insert_query))
                            })
                        }),
                        Err(_) => Err("No tokio runtime available".into()),
                    };

                    if let Err(e) = exec_result {
                        if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                            logging::log(&format!(
                                "WORKER {}: MySQL push error: {}",
                                self.config.name, e
                            ));
                        }
                        // Fallback: insert into dest_table locally
                        if !self.dest_table.is_empty() {
                            let q = format!(
                                "INSERT INTO {} (error) VALUES ('{}')",
                                self.dest_table,
                                crate::util::escape::mysql_escape(&e)
                            );
                            engine::run_query(&q, None, &mut local_conn.metadata, None);
                        }
                    }
                }
                TransferMode::OneWayPull | TransferMode::TwoWayPull | TransferMode::IoMixPull => {
                    // Execute configured query locally
                    let q = crate::util::template::build_string_template(&self.query, &row.columns);
                    let result = engine::run_query(&q, None, &mut local_conn.metadata, None);

                    if !result.success && logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                        logging::log(&format!(
                            "WORKER {}: local query error: {}",
                            self.config.name, result.err
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

/// Build an INSERT query from an OwnedRow, skipping internal metadata columns.
fn build_insert_query(table: &str, row: &OwnedRow) -> String {
    let mut columns = Vec::new();
    let mut values = Vec::new();

    for (key, val) in &row.columns {
        // Skip internal columns that don't exist on the remote table.
        if key.starts_with("x_uniform_") || key == "u_created_at" {
            continue;
        }
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
