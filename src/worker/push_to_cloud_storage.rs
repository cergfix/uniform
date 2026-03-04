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

    /// Upload data to S3 using aws-sdk-s3.
    async fn s3_upload(&self, s3_key: &str, data: Vec<u8>) -> Result<(), String> {
        use aws_config::Region;
        use aws_sdk_s3::primitives::ByteStream;

        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(Region::new(self.region.clone()));

        if !self.provider_url.is_empty() {
            config_loader = config_loader.endpoint_url(&self.provider_url);
        }

        // Use explicit credentials if provided
        if !self.key.is_empty() && !self.secret.is_empty() {
            let creds = aws_sdk_s3::config::Credentials::new(
                &self.key,
                &self.secret,
                None,
                None,
                "uniform",
            );
            config_loader = config_loader.credentials_provider(creds);
        }

        let sdk_config = config_loader.load().await;
        let client = aws_sdk_s3::Client::new(&sdk_config);

        let content_type = if self.output_mode == "json" {
            "application/json"
        } else {
            "application/octet-stream"
        };

        client
            .put_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .body(ByteStream::from(data))
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| format!("S3 upload error: {}", e))?;

        Ok(())
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

        // Generate S3 key path (matches Go: path/YYYY/MM/DD/HH/mm/name-hostname-timestamp.ext)
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

        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log(&format!(
                "WORKER {}: uploading {} bytes to s3://{}/{}",
                self.config.name,
                data.len(),
                self.bucket,
                s3_key
            ));
        }

        // Upload to S3 (blocking via tokio runtime)
        let upload_result = match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(self.s3_upload(&s3_key, data.clone()))
            }),
            Err(_) => Err("No tokio runtime available".into()),
        };

        if let Err(e) = upload_result {
            if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                logging::log(&format!(
                    "WORKER {}: S3 upload error: {}",
                    self.config.name, e
                ));
            }
            // Fallback: insert into dest_table
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
