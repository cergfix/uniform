use std::sync::Arc;

use crate::store::index::SecondaryIndex;
use crate::store::proc::{Proc, ProcType};
use crate::store::registry::{Server, ServerProtocol, PROCS, SERVERS, TABLES};
use crate::store::table::Table;
use crate::store::worker::{Worker, WorkerClass};
use crate::types::query_response::QueryResponse;
use crate::util::logging;

/// CREATE TABLE name (options)
/// Options JSON: Size, ForceLimit, ForceOffset, MaxScanTimeMs, BufferSize,
///               LongPollMaxClients, Silent, BufferLongPollMs
pub fn create_table_cmd(name: &str, options_str: &str) -> QueryResponse {
    if name.starts_with('$') {
        return QueryResponse::err(
            "Object names starting with $ symbol are reserved for system objects only.",
        );
    }

    if TABLES.contains_key(name) {
        return QueryResponse::err("Table already exists.");
    }

    // Parse options JSON
    let opts: serde_json::Value = if options_str.is_empty() {
        serde_json::json!({})
    } else {
        match serde_json::from_str(options_str) {
            Ok(v) => v,
            Err(e) => {
                return QueryResponse::err(format!("Invalid JSON options: {}", e));
            }
        }
    };

    let table_size = opts
        .get("Size")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    let force_limit = opts.get("ForceLimit").and_then(|v| v.as_i64()).unwrap_or(-1) as i32;
    let force_offset = opts.get("ForceOffset").and_then(|v| v.as_i64()).unwrap_or(-1) as i32;
    let max_scan_time_ms = opts.get("MaxScanTimeMs").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let buffer_size = opts.get("BufferSize").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let long_poll_max = opts
        .get("LongPollMaxClients")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let silent = opts.get("Silent").and_then(|v| v.as_bool()).unwrap_or(false);
    let buffer_long_poll_ms = opts
        .get("BufferLongPollMs")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;

    let mut table = Table::new(name.to_string(), table_size);
    table.force_limit = force_limit;
    table.force_offset = force_offset;
    table.max_scan_time_ms = max_scan_time_ms;
    table.long_poll_max_clients = long_poll_max;
    table.silent = silent;
    table.buffer_max_size = buffer_size;
    table.buffer_long_poll_ms = buffer_long_poll_ms;

    // Set up buffer channel if buffer_size > 0
    if buffer_size > 0 {
        let (tx, rx) = tokio::sync::mpsc::channel(buffer_size);
        table.buffer_tx = Some(tx);
        table.buffer_rx = Some(tokio::sync::Mutex::new(rx));
    }

    TABLES.insert(name.to_string(), Arc::new(table));

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!("CREATE TABLE: created table {}", name));
    }

    QueryResponse::ok_bool()
}

/// CREATE INDEX name (options)
/// Options JSON: Table, Column, Unique
pub fn create_index_cmd(name: &str, options_str: &str) -> QueryResponse {
    if name.starts_with('$') {
        return QueryResponse::err(
            "Object names starting with $ symbol are reserved for system objects only.",
        );
    }

    let opts: serde_json::Value = if options_str.is_empty() {
        return QueryResponse::err("Index options are required.");
    } else {
        match serde_json::from_str(options_str) {
            Ok(v) => v,
            Err(e) => {
                return QueryResponse::err(format!("Invalid JSON options: {}", e));
            }
        }
    };

    let table_name = match opts.get("Table").and_then(|v| v.as_str()) {
        Some(t) => t.to_string(),
        None => return QueryResponse::err("Table is required for CREATE INDEX."),
    };

    let column = match opts.get("Column").and_then(|v| v.as_str()) {
        Some(c) => c.to_string(),
        None => return QueryResponse::err("Column is required for CREATE INDEX."),
    };

    let unique = opts.get("Unique").and_then(|v| v.as_bool()).unwrap_or(false);

    // Get the table and add the index
    // We need to remove and re-insert since Table is behind Arc
    if let Some((_, table_arc)) = TABLES.remove(&table_name) {
        // We need to get a mutable reference — since Arc doesn't allow that,
        // we reconstruct. This is safe because CREATE INDEX is a DDL operation
        // that should be serialized.
        let table_ref = Arc::try_unwrap(table_arc).unwrap_or_else(|_arc| {
            // Table is still referenced — this shouldn't happen during DDL
            // For safety, we just log and work with a clone approach
            logging::log("WARNING: CREATE INDEX — table has multiple references");
            // We can't really clone a Table with SkipMap, so we need a different approach
            // For now, panic — this indicates a design issue
            panic!("Cannot add index to table with active references");
        });

        let mut table = table_ref;
        let index = SecondaryIndex::new(name.to_string(), column, unique);
        table.add_index(index);

        TABLES.insert(table_name, Arc::new(table));

        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log(&format!("CREATE INDEX: created index {} on table", name));
        }

        QueryResponse::ok_bool()
    } else {
        QueryResponse::err(format!("Table doesn't exist: {}", table_name))
    }
}

/// CREATE SERVER protocol name (options)
pub fn create_server_cmd(protocol: ServerProtocol, name: &str, options_str: &str) -> QueryResponse {
    if name.starts_with('$') {
        return QueryResponse::err(
            "Object names starting with $ symbol are reserved for system objects only.",
        );
    }

    if SERVERS.contains_key(name) {
        return QueryResponse::err("Server already exists");
    }

    let opts: serde_json::Value = if options_str.is_empty() {
        serde_json::json!({})
    } else {
        match serde_json::from_str(options_str) {
            Ok(v) => v,
            Err(e) => {
                return QueryResponse::err(format!("Invalid JSON options: {}", e));
            }
        }
    };

    let bind = match opts.get("Bind").and_then(|v| v.as_str()) {
        Some(b) => b.to_string(),
        None => return QueryResponse::err("Bind address is required."),
    };

    let timeout_ms = opts.get("TimeoutMs").and_then(|v| v.as_i64()).unwrap_or(30000) as i32;
    let buffer_size = opts
        .get("BufferSize")
        .and_then(|v| v.as_u64())
        .unwrap_or(65536) as usize;
    let force_limit = opts.get("ForceLimit").and_then(|v| v.as_i64()).unwrap_or(-1) as i32;
    let force_offset = opts.get("ForceOffset").and_then(|v| v.as_i64()).unwrap_or(-1) as i32;
    let max_scan_time_ms = opts.get("MaxScanTimeMs").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let latency_target_ms = opts
        .get("LatencyTargetMs")
        .and_then(|v| v.as_f64())
        .unwrap_or(-1.0);

    let table = opts
        .get("Table")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // HTTP requires a Table
    if protocol == ServerProtocol::Http && table.is_empty() {
        return QueryResponse::err("Table variable is mandatory in case of HTTP server.");
    }

    let mut server = Server::new(name.to_string(), bind, protocol, timeout_ms, buffer_size);
    server.force_limit = force_limit;
    server.force_offset = force_offset;
    server.max_scan_time_ms = max_scan_time_ms;
    server.latency_target_ms = latency_target_ms;
    server.table = table;

    // HTTP-specific options
    server.script_filename = opts
        .get("ScriptFilename")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    server.document_root = opts
        .get("DocumentRoot")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    server.https_redirect = opts
        .get("HttpsRedirect")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    server.https_redirect_header = opts
        .get("HttpsRedirectHeader")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    server.https_redirect_on = opts
        .get("HttpsRedirectOn")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    server.robots = opts
        .get("Robots")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    server.gzip = opts.get("Gzip").and_then(|v| v.as_bool()).unwrap_or(false);

    let server_arc = Arc::new(server);

    // Start the TCP listener
    let server_clone = server_arc.clone();
    tokio::spawn(async move {
        crate::server::listener::start_listener(server_clone).await;
    });

    SERVERS.insert(name.to_string(), server_arc);

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!(
            "CREATE SERVER: created {} server {}",
            protocol.as_str(),
            name
        ));
    }

    QueryResponse::ok_bool()
}

/// CREATE PROC type name (options)
pub fn create_proc_cmd(proc_type: ProcType, name: &str, options_str: &str) -> QueryResponse {
    if name.starts_with('$') {
        return QueryResponse::err(
            "Object names starting with $ symbol are reserved for system objects only.",
        );
    }

    let opts: serde_json::Value = if options_str.is_empty() {
        serde_json::json!({})
    } else {
        match serde_json::from_str(options_str) {
            Ok(v) => v,
            Err(e) => {
                return QueryResponse::err(format!("Invalid JSON options: {}", e));
            }
        }
    };

    let mut proc = Proc::new(name.to_string(), proc_type);

    proc.src = opts
        .get("Src")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.dest = opts
        .get("Dest")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.table = opts
        .get("Table")
        .or_else(|| opts.get("Src"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.case_query_string = opts
        .get("CaseQuery")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.patch_string = opts
        .get("Patch")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.post_wait_ms = opts
        .get("PostWaitMs")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    proc.enabled = opts
        .get("Enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    proc.wait_ms = opts
        .get("WaitMs")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;

    // Encryption/decryption
    proc.encrypt_key = opts
        .get("EncryptKey")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.encrypt_fields = opts
        .get("EncryptFields")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.decrypt_key = opts
        .get("DecryptKey")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.decrypt_fields = opts
        .get("DecryptFields")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Gzip
    proc.gzip_fields = opts
        .get("GzipFields")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.gunzip_fields = opts
        .get("GunzipFields")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Read reduce
    proc.reduce_key = opts
        .get("ReduceKey")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.reduce_to_latest = opts
        .get("ReduceToLatest")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Auto-reply
    proc.reply_status = opts
        .get("ReplyStatus")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.reply_body = opts
        .get("ReplyBody")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Time-based
    proc.start_time_string = opts
        .get("StartTime")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.end_time_string = opts
        .get("EndTime")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Extended case query method
    if let Some(method) = opts.get("ExtCaseQueryMethod").and_then(|v| v.as_str()) {
        match method.to_uppercase().as_str() {
            "SUCCESS" => proc.ext_case_query_method = crate::store::proc::ExtCaseQueryMethod::Success,
            _ => proc.ext_case_query_method = crate::store::proc::ExtCaseQueryMethod::Count,
        }
    }

    // Parse patch data if present
    if !proc.patch_string.is_empty() {
        if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_str::<serde_json::Value>(&proc.patch_string)
        {
            let patch: std::collections::HashMap<String, crate::types::value::Value> = map
                .into_iter()
                .map(|(k, v)| (k, crate::types::value::Value::from(v)))
                .collect();
            proc.patch_data = Some(patch);
        }
    }

    // Add to global procs list
    PROCS.write().push(proc);

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!(
            "CREATE PROC: created {} proc {}",
            proc_type.as_str(),
            name
        ));
    }

    QueryResponse::ok_bool()
}

/// START WORKER class name (options)
pub fn start_worker_cmd(class: WorkerClass, name: &str, options_str: &str) -> QueryResponse {
    use crate::store::registry::WORKERS;

    if name.starts_with('$') {
        return QueryResponse::err(
            "Object names starting with $ symbol are reserved for system objects only.",
        );
    }

    if WORKERS.contains_key(name) {
        return QueryResponse::err("Worker already exists");
    }

    let opts: serde_json::Value = if options_str.is_empty() {
        serde_json::json!({})
    } else {
        match serde_json::from_str(options_str) {
            Ok(v) => v,
            Err(e) => {
                return QueryResponse::err(format!("Invalid JSON options: {}", e));
            }
        }
    };

    // Create worker data row from options
    let mut data = crate::types::row::OwnedRow::new();
    if let serde_json::Value::Object(map) = opts {
        for (k, v) in map {
            data.columns
                .insert(k, crate::types::value::Value::from(v));
        }
    }

    // Store worker class in data
    data.columns.insert(
        "WorkerClass".into(),
        crate::types::value::Value::String(class.as_str().to_string()),
    );

    let (tx, _rx) = tokio::sync::watch::channel(false);
    let worker = Worker::new(name.to_string(), class, data, tx);

    // TODO: Phase 5 — spawn the actual worker loop based on class
    // For now, just register the worker

    WORKERS.insert(name.to_string(), Arc::new(worker));

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!(
            "START WORKER: started {} worker {}",
            class.as_str(),
            name
        ));
    }

    QueryResponse::ok_bool()
}
