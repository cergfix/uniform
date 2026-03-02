use std::io::Write;

use tokio::sync::watch;

use crate::types::row::OwnedRow;
use crate::util::logging;
use crate::worker::base::{WorkerConfig, WorkerLoop};

/// PUSH_TO_FILE worker — pops rows and writes them to a file as JSON.
/// Supports optional gzip compression and AES encryption.
pub struct PushToFileWorker {
    pub config: WorkerConfig,
    pub file_path: String,
    pub format: String, // "json" or "sql"
    pub gzip: bool,
    pub encrypt_key: String,
}

impl PushToFileWorker {
    pub fn new(
        name: String,
        table: String,
        file_path: String,
        format: String,
        gzip: bool,
        encrypt_key: String,
        frequency_ms: u64,
        burst: i32,
        max_runs: i64,
        terminator: watch::Receiver<bool>,
    ) -> Self {
        PushToFileWorker {
            config: WorkerConfig {
                name,
                table,
                frequency_ms,
                burst,
                max_runs,
                terminator,
            },
            file_path,
            format,
            gzip,
            encrypt_key,
        }
    }
}

impl WorkerLoop for PushToFileWorker {
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String> {
        let row_count = rows.len();
        let mut output = String::new();

        for row in &rows {
            match row.to_json() {
                Ok(json_str) => {
                    output.push_str(&json_str);
                    output.push('\n');
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

        if output.is_empty() {
            return Ok(());
        }

        let mut data = output.into_bytes();

        // Optional gzip compression
        if self.gzip {
            use flate2::write::GzEncoder;
            use flate2::Compression;

            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(&data)
                .map_err(|e| format!("gzip error: {}", e))?;
            data = encoder
                .finish()
                .map_err(|e| format!("gzip finish: {}", e))?;
        }

        // Optional AES encryption
        if !self.encrypt_key.is_empty() {
            let encrypted = crate::crypto::aes_cbc::encrypt(&data, &self.encrypt_key)
                .map_err(|e| format!("encrypt error: {}", e))?;
            data = encrypted.into_bytes();
        }

        // Generate filename with timestamp
        let timestamp = chrono::Utc::now().format("%Y%m%d_%H%M%S");
        let hostname = hostname::get()
            .map(|h: std::ffi::OsString| h.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "unknown".into());
        let filename = format!(
            "{}_{}_{}_{}.dat",
            self.config.name, hostname, timestamp, row_count
        );
        let full_path = if self.file_path.is_empty() {
            filename
        } else {
            format!("{}/{}", self.file_path, filename)
        };

        // Write to file
        std::fs::write(&full_path, &data).map_err(|e| format!("file write error: {}", e))?;

        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log(&format!(
                "WORKER {}: wrote {} rows to {}",
                self.config.name,
                row_count,
                full_path
            ));
        }

        Ok(())
    }

    fn config(&self) -> &WorkerConfig {
        &self.config
    }
}
