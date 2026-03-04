use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::types::row::OwnedRow;
use crate::types::value::Value;
use crate::util::logging;
use crate::worker::base::{WorkerConfig, WorkerLoop};

/// PUSH_TO_PAGERDUTY worker — sends incident alerts to PagerDuty.
pub struct PushToPagerDutyWorker {
    pub config: WorkerConfig,
    pub query: String,
    pub dest_table: String,
    pub source: String,
    pub severity: String,
    pub routing_key: String,
    pub link: String,
    pub link_text: String,
}

impl PushToPagerDutyWorker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        table: String,
        query: String,
        dest_table: String,
        source: String,
        severity: String,
        routing_key: String,
        link: String,
        link_text: String,
        frequency_ms: u64,
        burst: i32,
        max_runs: i64,
        terminator: watch::Receiver<bool>,
    ) -> Self {
        PushToPagerDutyWorker {
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
            source,
            severity,
            routing_key,
            link,
            link_text,
        }
    }
}

impl WorkerLoop for PushToPagerDutyWorker {
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String> {
        let client = reqwest::blocking::Client::new();

        for row in &rows {
            // Extract summary from "message" field
            let summary = match row.columns.get("message") {
                Some(Value::String(s)) => {
                    if s.len() > 1000 {
                        s[..1000].to_string()
                    } else {
                        s.clone()
                    }
                }
                _ => "Uniform alert".into(),
            };

            // SHA256 dedup key
            let full_message = match row.columns.get("message") {
                Some(Value::String(s)) => s.clone(),
                _ => summary.clone(),
            };
            let mut hasher = Sha256::new();
            hasher.update(full_message.as_bytes());
            let dedup_key = hex::encode(hasher.finalize());

            // Timestamp
            let timestamp = match row.columns.get("u_created_at") {
                Some(Value::String(s)) => s.clone(),
                _ => chrono::Utc::now().to_rfc3339(),
            };

            // Build PagerDuty event payload
            let mut payload = serde_json::json!({
                "routing_key": self.routing_key,
                "dedup_key": dedup_key,
                "event_action": "trigger",
                "payload": {
                    "summary": summary,
                    "timestamp": timestamp,
                    "source": self.source,
                    "severity": self.severity,
                    "custom_details": row.columns
                }
            });

            // Add links if configured
            if !self.link.is_empty() {
                let link_href =
                    crate::util::template::build_string_template(&self.link, &row.columns);
                let link_text = if self.link_text.is_empty() {
                    link_href.clone()
                } else {
                    crate::util::template::build_string_template(&self.link_text, &row.columns)
                };
                payload["links"] = serde_json::json!([{"href": link_href, "text": link_text}]);
            }

            match client
                .post("https://events.pagerduty.com/v2/enqueue")
                .json(&payload)
                .send()
            {
                Ok(resp) => {
                    if let Ok(body) = resp.text() {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                            if json.get("status")
                                != Some(&serde_json::Value::String("success".into()))
                            {
                                if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                                    logging::log(&format!(
                                        "WORKER {}: PagerDuty error: {}",
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

fn fallback_insert(dest_table: &str, row: &OwnedRow) {
    if dest_table.is_empty() {
        return;
    }
    if let Some(table) = crate::store::registry::get_table(dest_table) {
        let _ = table.insert(row.columns.clone());
    }
}
