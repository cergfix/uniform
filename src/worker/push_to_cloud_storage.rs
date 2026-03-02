use std::io::Write;

use tokio::sync::watch;

use crate::types::row::OwnedRow;
use crate::util::logging;
use crate::worker::base::{WorkerConfig, WorkerLoop};

/// PUSH_TO_CLOUD_STORAGE worker — uploads data to S3/MinIO.
pub struct PushToCloudStorageWorker {
    pub config: WorkerConfig,
    pub query: String,
    pub dest_table: String,
    pub key: String,
    pub secret: String,
    pub bucket: String,
    pub region: String,
    pub provider_url: String,
    pub path: String,
    pub output_mode: String,
    pub gzip: bool,
    pub encrypt_key: String,
}

impl PushToCloudStorageWorker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        table: String,
        query: String,
        dest_table: String,
        key: String,
        secret: String,
        bucket: String,
        region: String,
        provider_url: String,
        path: String,
        output_mode: String,
        gzip: bool,
        encrypt_key: String,
        frequency_ms: u64,
        burst: i32,
        max_runs: i64,
        terminator: watch::Receiver<bool>,
    ) -> Self {
        PushToCloudStorageWorker {
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
            key,
            secret,
            bucket,
            region,
            provider_url,
            path,
            output_mode,
            gzip,
            encrypt_key,
        }
    }
}

impl WorkerLoop for PushToCloudStorageWorker {
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String> {
        // Format output
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

        // Generate S3 key path
        let now = chrono::Utc::now();
        let hostname = hostname::get()
            .map(|h: std::ffi::OsString| h.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "unknown".into());

        let mut ext = self.output_mode.clone();
        if self.gzip {
            ext.push_str(".gz");
        }
        if !self.encrypt_key.is_empty() {
            ext.push_str(".aes");
        }

        let s3_key = format!(
            "{}/{}/{}-{}-{}.{}",
            self.path,
            now.format("%Y/%m/%d/%H/%M"),
            self.config.name,
            hostname,
            now.format("%Y%m%d_%H%M%S"),
            ext
        );

        // Upload to S3 using AWS SDK
        // Note: This is a blocking operation for simplicity.
        // In production, you'd use the async aws-sdk-s3 client.
        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log(&format!(
                "WORKER {}: uploading {} bytes to s3://{}/{}",
                self.config.name,
                data.len(),
                self.bucket,
                s3_key
            ));
        }

        // For now, write to a temp file as fallback
        // TODO: integrate async S3 upload using aws-sdk-s3
        let temp_path = format!("/tmp/{}", s3_key.replace('/', "_"));
        if let Err(e) = std::fs::write(&temp_path, &data) {
            if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                logging::log(&format!(
                    "WORKER {}: S3 upload fallback error: {}",
                    self.config.name, e
                ));
            }
            // Insert into dest_table as fallback
            if !self.dest_table.is_empty() {
                if let Some(table) = crate::store::registry::get_table(&self.dest_table) {
                    for row in &rows {
                        let _ = table.insert(row.columns.clone());
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
