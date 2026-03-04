use std::sync::atomic::Ordering;

use crate::config::vars;
use crate::query::command::ShowFilter;
use crate::query::condition;
use crate::store::registry::{PROCS, SERVERS, TABLES, WORKERS};
use crate::types::query_response::QueryResponse;
use crate::types::row::OwnedRow;
use crate::types::value::Value;
use crate::util::logging;

/// Execute a SHOW command.
pub fn show_cmd(what: &str, filter: Option<&ShowFilter>) -> QueryResponse {
    let rows = match what {
        "tables" => show_tables(),
        "servers" => show_servers(),
        "workers" => show_workers(),
        "procs" => show_procs(),
        "connections" => show_connections(),
        "variables" => show_variables(),
        "indexes" => show_indexes(),
        "role" => show_role(),
        "legal" => show_legal(),
        "metadata" => {
            // Metadata is per-connection, we return empty for now
            Vec::new()
        }
        _ => return QueryResponse::err(format!("Unknown SHOW target: {}", what)),
    };

    // Apply filter
    let filtered = match filter {
        Some(ShowFilter::Like(pattern)) => {
            let col = like_filter_column(what);
            filter_rows_like(&rows, &col, pattern)
        }
        Some(ShowFilter::Where(where_str)) => filter_rows_where(&rows, where_str),
        None => rows,
    };

    QueryResponse::ok_result(filtered)
}

fn show_tables() -> Vec<OwnedRow> {
    let mut rows = Vec::new();
    for entry in TABLES.iter() {
        let t = entry.value();
        let mut row = OwnedRow::new();
        row.columns
            .insert("Tables_in_main".into(), Value::String(t.name.clone()));
        row.columns
            .insert("Rows".into(), Value::Number(t.len() as f64));
        row.columns
            .insert("Capacity".into(), Value::String(t.capacity_str()));
        row.columns
            .insert("ForceLimit".into(), Value::Number(t.force_limit as f64));
        row.columns
            .insert("ForceOffset".into(), Value::Number(t.force_offset as f64));
        row.columns.insert(
            "MaxScanTimeMs".into(),
            Value::Number(t.max_scan_time_ms as f64),
        );
        row.columns.insert(
            "LongPollMaxClients".into(),
            Value::Number(t.long_poll_max_clients as f64),
        );
        row.columns
            .insert("BufferSize".into(), Value::Number(t.buffer_max_size as f64));
        row.columns.insert("Silent".into(), Value::Bool(t.silent));
        row.columns
            .insert("Indexes".into(), Value::Number(t.indexes.len() as f64));
        rows.push(row);
    }
    rows
}

fn show_servers() -> Vec<OwnedRow> {
    let mut rows = Vec::new();
    for entry in SERVERS.iter() {
        let s = entry.value();
        let mut row = OwnedRow::new();
        row.columns
            .insert("Name".into(), Value::String(s.name.clone()));
        row.columns.insert(
            "Protocol".into(),
            Value::String(s.protocol.as_str().to_string()),
        );
        row.columns
            .insert("Bind".into(), Value::String(s.bind.clone()));
        row.columns.insert(
            "Clients".into(),
            Value::Number(s.connection_count.load(Ordering::Relaxed) as f64),
        );
        row.columns
            .insert("TimeoutMs".into(), Value::Number(s.timeout_ms as f64));
        row.columns
            .insert("BufferSize".into(), Value::Number(s.buffer_size as f64));
        row.columns
            .insert("ForceLimit".into(), Value::Number(s.force_limit as f64));
        row.columns
            .insert("ForceOffset".into(), Value::Number(s.force_offset as f64));
        row.columns.insert(
            "MaxScanTimeMs".into(),
            Value::Number(s.max_scan_time_ms as f64),
        );
        row.columns
            .insert("LatencyTargetMs".into(), Value::Number(s.latency_target_ms));
        row.columns
            .insert("Table".into(), Value::String(s.table.clone()));
        rows.push(row);
    }
    rows
}

fn show_workers() -> Vec<OwnedRow> {
    let mut rows = Vec::new();
    for entry in WORKERS.iter() {
        let w = entry.value();
        let mut row = OwnedRow::new();
        row.columns
            .insert("Name".into(), Value::String(w.name.clone()));
        row.columns
            .insert("Class".into(), Value::String(w.class.as_str().to_string()));
        // Copy worker data columns
        for (k, v) in &w.data.columns {
            row.columns.insert(k.clone(), v.clone());
        }
        rows.push(row);
    }
    rows
}

fn show_procs() -> Vec<OwnedRow> {
    let procs = PROCS.read();
    let mut rows = Vec::new();
    for p in procs.iter() {
        let mut row = OwnedRow::new();
        row.columns
            .insert("Name".into(), Value::String(p.name.clone()));
        row.columns.insert(
            "Type".into(),
            Value::String(p.proc_type.as_str().to_string()),
        );
        row.columns
            .insert("Src".into(), Value::String(p.src.clone()));
        row.columns
            .insert("Dest".into(), Value::String(p.dest.clone()));
        row.columns
            .insert("Table".into(), Value::String(p.table.clone()));
        row.columns.insert("Enabled".into(), Value::Bool(p.enabled));
        row.columns.insert(
            "CaseQuery".into(),
            Value::String(p.case_query_string.clone()),
        );
        row.columns
            .insert("Patch".into(), Value::String(p.patch_string.clone()));
        row.columns
            .insert("PostWaitMs".into(), Value::Number(p.post_wait_ms as f64));
        row.columns
            .insert("WaitMs".into(), Value::Number(p.wait_ms as f64));
        rows.push(row);
    }
    rows
}

fn show_connections() -> Vec<OwnedRow> {
    let mut rows = Vec::new();
    for server_entry in SERVERS.iter() {
        let s = server_entry.value();
        for conn_entry in s.connections.iter() {
            if let Ok(c) = conn_entry.value().try_lock() {
                let mut row = OwnedRow::new();
                row.columns
                    .insert("Server".into(), Value::String(s.name.clone()));
                row.columns
                    .insert("RemoteAddr".into(), Value::String(c.remote_addr.clone()));
                row.columns
                    .insert("LocalAddr".into(), Value::String(c.local_addr.clone()));
                row.columns.insert(
                    "Protocol".into(),
                    Value::String(s.protocol.as_str().to_string()),
                );
                row.columns
                    .insert("Created".into(), Value::String(c.created.to_rfc3339()));
                row.columns.insert(
                    "LastActivity".into(),
                    Value::String(c.last_activity.to_rfc3339()),
                );
                row.columns
                    .insert("State".into(), Value::String(c.state_text.clone()));
                row.columns
                    .insert("Query".into(), Value::String(c.query.clone()));
                rows.push(row);
            }
        }
    }
    rows
}

fn show_variables() -> Vec<OwnedRow> {
    let mut rows = Vec::new();

    let vars_list: Vec<(&str, Value)> = vec![
        ("LOG_LEVEL", Value::String(logging::get_log_level_string())),
        (
            "SELECT_CACHE",
            Value::String(if vars::select_cache_enabled() {
                "ON".into()
            } else {
                "OFF".into()
            }),
        ),
        ("LOG_DEST", Value::String(logging::get_log_dest())),
        (
            "ROLE",
            Value::String(crate::util::cluster::get_cluster_role()),
        ),
        (
            "WORKERS_FORCE_SHUTDOWN",
            Value::String(if vars::workers_force_shutdown() {
                "ON".into()
            } else {
                "OFF".into()
            }),
        ),
        (
            "PERSIST_METADATA",
            Value::String(if vars::persist_metadata() {
                "ON".into()
            } else {
                "OFF".into()
            }),
        ),
        ("VERSION", Value::String(vars::version().to_string())),
    ];

    for (name, value) in vars_list {
        let mut row = OwnedRow::new();
        row.columns
            .insert("Variable_name".into(), Value::String(name.to_string()));
        row.columns.insert("Value".into(), value);
        rows.push(row);
    }
    rows
}

fn show_indexes() -> Vec<OwnedRow> {
    let mut rows = Vec::new();
    for entry in TABLES.iter() {
        let t = entry.value();
        for (idx_name, idx) in &t.indexes {
            let mut row = OwnedRow::new();
            row.columns
                .insert("Name".into(), Value::String(idx_name.clone()));
            row.columns
                .insert("Table".into(), Value::String(t.name.clone()));
            row.columns
                .insert("Column".into(), Value::String(idx.column.clone()));
            row.columns.insert("Unique".into(), Value::Bool(idx.unique));
            row.columns
                .insert("Entries".into(), Value::Number(idx.len() as f64));
            rows.push(row);
        }
    }
    rows
}

fn show_role() -> Vec<OwnedRow> {
    let mut row = OwnedRow::new();
    row.columns
        .insert("Variable_name".into(), Value::String("ROLE".into()));
    row.columns.insert(
        "Value".into(),
        Value::String(crate::util::cluster::get_cluster_role()),
    );
    vec![row]
}

fn show_legal() -> Vec<OwnedRow> {
    let items: Vec<(&str, &str)> =
        vec![("VERSION", vars::version()), ("COPYRIGHT", vars::COPYRIGHT)];
    items
        .into_iter()
        .map(|(name, value)| {
            let mut row = OwnedRow::new();
            row.columns
                .insert("Variable_name".into(), Value::String(name.to_string()));
            row.columns
                .insert("Value".into(), Value::String(value.to_string()));
            row
        })
        .collect()
}

fn like_filter_column(what: &str) -> String {
    match what {
        "tables" => "Tables_in_main".to_string(),
        "servers" | "workers" | "procs" => "Name".to_string(),
        "connections" => "RemoteAddr".to_string(),
        "variables" | "role" | "legal" => "Variable_name".to_string(),
        _ => String::new(),
    }
}

fn filter_rows_like(rows: &[OwnedRow], column: &str, pattern: &str) -> Vec<OwnedRow> {
    if column.is_empty() {
        return rows.to_vec();
    }
    let re_pattern = pattern.replace('%', ".*").replace('_', ".");
    let re = match regex::Regex::new(&format!("(?i)^{}$", re_pattern)) {
        Ok(r) => r,
        Err(_) => return rows.to_vec(),
    };

    rows.iter()
        .filter(|row| {
            row.columns
                .get(column)
                .map(|v| re.is_match(&v.to_string_repr()))
                .unwrap_or(false)
        })
        .cloned()
        .collect()
}

fn filter_rows_where(rows: &[OwnedRow], where_str: &str) -> Vec<OwnedRow> {
    rows.iter()
        .filter(|row| condition::row_meets_conditions_simple(where_str, &row.columns))
        .cloned()
        .collect()
}
