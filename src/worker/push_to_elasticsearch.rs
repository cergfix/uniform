
use tokio::sync::watch;

use crate::types::row::OwnedRow;
use crate::types::value::Value;
use crate::util::logging;
use crate::worker::base::{WorkerConfig, WorkerLoop};

/// PUSH_TO_ELASTICSEARCH worker — indexes documents in Elasticsearch.
pub struct PushToElasticsearchWorker {
    pub config: WorkerConfig,
    pub query: String,
    pub dest_table: String,
    pub host: String,
    pub es_index: String,
    pub es_type: String,
}

impl PushToElasticsearchWorker {
    pub fn new(
        name: String,
        table: String,
        query: String,
        dest_table: String,
        host: String,
        es_index: String,
        es_type: String,
        frequency_ms: u64,
        burst: i32,
        max_runs: i64,
        terminator: watch::Receiver<bool>,
    ) -> Self {
        PushToElasticsearchWorker {
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
            es_index,
            es_type,
        }
    }
}

impl WorkerLoop for PushToElasticsearchWorker {
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String> {
        let client = reqwest::blocking::Client::new();

        for row in &rows {
            let json_str = row.to_json().map_err(|e| format!("JSON error: {}", e))?;

            let fb_id = match row.columns.get("Fb_id") {
                Some(Value::String(s)) => s.clone(),
                _ => uuid::Uuid::new_v4().to_string(),
            };

            let url = format!(
                "{}/{}/{}/{}",
                self.host, self.es_index, self.es_type, fb_id
            );

            match client
                .put(&url)
                .header("Content-Type", "application/json")
                .body(json_str)
                .send()
            {
                Ok(resp) => {
                    if let Ok(body) = resp.text() {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                            let result = json
                                .get("result")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            if result != "created" && result != "updated" {
                                if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                                    logging::log(&format!(
                                        "WORKER {}: ES error: {}",
                                        self.config.name, body
                                    ));
                                }
                                fallback_insert(&self.dest_table, row);
                            }
                        }
                    }
                }
                Err(e) => {
                    if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                        logging::log(&format!(
                            "WORKER {}: HTTP error: {}",
                            self.config.name, e
                        ));
                    }
                    fallback_insert(&self.dest_table, row);
                }
            }
        }
        Ok(())
    }

    fn config(&self) -> &WorkerConfig {
        &self.config
    }
}

fn fallback_insert(dest_table: &str, row: &OwnedRow) {
    if dest_table.is_empty() {
        return;
    }
    if let Some(table) = crate::store::registry::get_table(dest_table) {
        let _ = table.insert(row.columns.clone());
    }
}
