use std::sync::atomic::{AtomicI32, Ordering};

use chrono::Utc;
use parking_lot::RwLock;

pub const LOG_LEVEL_CRIT: i32 = 0;
pub const LOG_LEVEL_ERROR: i32 = 1;
pub const LOG_LEVEL_DEBUG: i32 = 2;

static LOG_LEVEL: AtomicI32 = AtomicI32::new(LOG_LEVEL_ERROR);
static LOG_DEST: once_cell::sync::Lazy<RwLock<String>> =
    once_cell::sync::Lazy::new(|| RwLock::new(String::new()));

pub fn set_log_level(level: i32) -> i32 {
    if level == LOG_LEVEL_CRIT || level == LOG_LEVEL_ERROR || level == LOG_LEVEL_DEBUG {
        LOG_LEVEL.store(level, Ordering::Relaxed);
    }
    LOG_LEVEL.load(Ordering::Relaxed)
}

pub fn get_log_level() -> i32 {
    LOG_LEVEL.load(Ordering::Relaxed)
}

pub fn get_log_level_string() -> String {
    match LOG_LEVEL.load(Ordering::Relaxed) {
        LOG_LEVEL_CRIT => "CRIT".to_string(),
        LOG_LEVEL_ERROR => "ERROR".to_string(),
        LOG_LEVEL_DEBUG => "DEBUG".to_string(),
        _ => "UNKNOWN".to_string(),
    }
}

pub fn get_log_level_from_string(level: &str) -> i32 {
    match level.to_uppercase().as_str() {
        "CRIT" => LOG_LEVEL_CRIT,
        "ERROR" => LOG_LEVEL_ERROR,
        "DEBUG" => LOG_LEVEL_DEBUG,
        _ => -1,
    }
}

pub fn is_valid_log_level_string(level: &str) -> bool {
    matches!(
        level.to_uppercase().as_str(),
        "CRIT" | "ERROR" | "DEBUG"
    )
}

pub fn set_log_dest(dest: &str) -> String {
    *LOG_DEST.write() = dest.to_string();
    dest.to_string()
}

pub fn get_log_dest() -> String {
    LOG_DEST.read().clone()
}

/// Log a message — matches Go's Log() behavior.
/// If log level is DEBUG, prints to stdout.
/// If log dest is set to a table name, inserts into that table.
pub fn log(s: &str) {
    let timestamp = Utc::now();

    // Truncate messages that are too long
    let msg = if s.len() > 100000 {
        format!("{}... (truncated)", &s[..100000])
    } else {
        s.to_string()
    };

    // Debug always goes to stdout
    if get_log_level() == LOG_LEVEL_DEBUG {
        let formatted = format!(
            "{} - {}",
            timestamp.format("%Y-%m-%dT%H:%M:%S%.3f%:z"),
            msg
        );
        println!("{}", formatted);
    }

    // If dest is a table and level is above debug, insert into table
    let dest = get_log_dest();
    if !dest.is_empty() && get_log_level() < LOG_LEVEL_DEBUG {
        let level_str = get_log_level_string();

        // Insert into log table
        use crate::store::registry;
        use crate::types::value::Value;

        if let Some(table) = registry::get_table(&dest) {
            let mut columns = std::collections::HashMap::new();
            columns.insert(
                "Fb_created".to_string(),
                Value::String(timestamp.format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string()),
            );
            columns.insert("Message".to_string(), Value::String(msg));
            columns.insert("Log_level".to_string(), Value::String(level_str));
            let _ = table.insert(columns);
        }
    }
}
