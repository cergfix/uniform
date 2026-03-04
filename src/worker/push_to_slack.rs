use tokio::sync::watch;

use crate::types::row::OwnedRow;
use crate::util::logging;
use crate::worker::base::{WorkerConfig, WorkerLoop};

/// PUSH_TO_SLACK worker — posts messages to Slack channels.
pub struct PushToSlackWorker {
    pub config: WorkerConfig,
    pub query: String,
    pub dest_table: String,
    pub token: String,
    pub channel: String,
    pub text: String,
    pub text_only: bool,
}

impl PushToSlackWorker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        table: String,
        query: String,
        dest_table: String,
        token: String,
        channel: String,
        text: String,
        text_only: bool,
        frequency_ms: u64,
        burst: i32,
        max_runs: i64,
        terminator: watch::Receiver<bool>,
    ) -> Self {
        PushToSlackWorker {
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
            token,
            channel,
            text,
            text_only,
        }
    }
}

impl WorkerLoop for PushToSlackWorker {
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String> {
        let client = reqwest::blocking::Client::new();

        for row in &rows {
            let json_str = row.to_json().map_err(|e| format!("JSON error: {}", e))?;

            let url = if self.text_only {
                let msg_text =
                    crate::util::template::build_string_template(&self.text, &row.columns);
                let encoded = urlencoding::encode(&msg_text);
                format!(
                    "https://www.slack.com/api/chat.postMessage?token={}&channel={}&text={}",
                    self.token, self.channel, encoded
                )
            } else {
                let row_id = match row.columns.get("u_id") {
                    Some(crate::types::value::Value::String(s)) => s.clone(),
                    _ => "unknown".into(),
                };
                let filename = format!("{}-{}.json", self.config.name, row_id);
                let encoded_content = urlencoding::encode(&json_str);
                format!(
                    "https://www.slack.com/api/files.upload?token={}&channels={}&content={}&filename={}",
                    self.token, self.channel, encoded_content, filename
                )
            };

            match client.get(&url).send() {
                Ok(resp) => {
                    if let Ok(body) = resp.text() {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                            if json.get("ok") != Some(&serde_json::Value::Bool(true)) {
                                if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                                    logging::log(&format!(
                                        "WORKER {}: Slack API error: {}",
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
                        logging::log(&format!("WORKER {}: HTTP error: {}", self.config.name, e));
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

/// Insert row into fallback/dest table on error.
fn fallback_insert(dest_table: &str, row: &OwnedRow) {
    if dest_table.is_empty() {
        return;
    }
    if let Some(table) = crate::store::registry::get_table(dest_table) {
        let _ = table.insert(row.columns.clone());
    }
}
