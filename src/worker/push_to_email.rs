
use lettre::message::{header::ContentType, Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};

use crate::config::vars;
use crate::types::row::OwnedRow;
use crate::types::value::Value;
use crate::util::logging;
use crate::worker::base::{WorkerConfig, WorkerLoop};

/// PUSH_TO_EMAIL worker — sends email notifications via SMTP.
pub struct PushToEmailWorker {
    pub config: WorkerConfig,
    pub query: String,
    pub dest_table: String,
    pub to: String,
    pub from: String,
    pub from_name: String,
    pub subject: String,
    pub text: String,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_user: String,
    pub smtp_password: String,
    pub gzip: bool,
    pub text_only: bool,
    pub encrypt_key: String,
}

impl WorkerLoop for PushToEmailWorker {
    fn process(&self, rows: Vec<OwnedRow>) -> Result<(), String> {
        let row_count = rows.len();

        // Build email body
        let mut json_payload = String::new();
        let mut sample_message = String::new();

        for row in &rows {
            if let Ok(json_str) = row.to_json() {
                json_payload.push_str(&json_str);
                json_payload.push('\n');
            }
            if sample_message.is_empty() {
                if let Some(Value::String(msg)) = row.columns.get("message") {
                    sample_message = if msg.len() > 1000 {
                        msg[..1000].to_string()
                    } else {
                        msg.clone()
                    };
                }
            }
        }

        let cols = rows
            .first()
            .map(|r| &r.columns)
            .cloned()
            .unwrap_or_default();
        let subject = crate::util::template::build_string_template(&self.subject, &cols);

        let html_body = format!(
            "<html><body>\
            <h3>{} Notification</h3>\
            <p>Message count: {}</p>\
            <p>{}</p>\
            <hr/>\
            <p><small>{}/{}</small></p>\
            </body></html>",
            vars::APP_NAME,
            row_count,
            sample_message,
            vars::APP_NAME,
            vars::version()
        );

        let plain_body = format!(
            "{} Notification\nMessage count: {}\n{}\n",
            vars::APP_NAME,
            row_count,
            sample_message
        );

        // Parse recipients
        let recipients: Vec<&str> = self.to.split(',').map(|s| s.trim()).collect();
        if recipients.is_empty() {
            return Err("No recipients specified".into());
        }

        let from_mailbox: Mailbox = format!("{} <{}>", self.from_name, self.from)
            .parse()
            .map_err(|e: lettre::address::AddressError| format!("Invalid from address: {}", e))?;
        let to_mailbox: Mailbox = recipients[0]
            .parse()
            .map_err(|e: lettre::address::AddressError| format!("Invalid to address: {}", e))?;

        let mut builder = Message::builder()
            .from(from_mailbox)
            .to(to_mailbox)
            .subject(&subject);

        // Add CC recipients
        for recipient in recipients.iter().skip(1) {
            if let Ok(cc) = recipient.parse::<Mailbox>() {
                builder = builder.cc(cc);
            }
        }

        let multipart = MultiPart::alternative()
            .singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_PLAIN)
                    .body(plain_body),
            )
            .singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_HTML)
                    .body(html_body),
            );

        let email = builder
            .multipart(multipart)
            .map_err(|e| format!("Email build error: {}", e))?;

        // Send via SMTP
        let creds = Credentials::new(self.smtp_user.clone(), self.smtp_password.clone());

        let mailer = SmtpTransport::relay(&self.smtp_host)
            .map_err(|e| format!("SMTP relay error: {}", e))?
            .credentials(creds)
            .port(self.smtp_port)
            .build();

        match mailer.send(&email) {
            Ok(_) => {
                if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                    logging::log(&format!(
                        "WORKER {}: email sent to {}",
                        self.config.name, self.to
                    ));
                }
            }
            Err(e) => {
                if logging::get_log_level() >= logging::LOG_LEVEL_ERROR {
                    logging::log(&format!(
                        "WORKER {}: SMTP error: {}",
                        self.config.name, e
                    ));
                }
                // Fallback
                if !self.dest_table.is_empty() {
                    if let Some(table) = crate::store::registry::get_table(&self.dest_table) {
                        for row in &rows {
                            let _ = table.insert(row.columns.clone());
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
