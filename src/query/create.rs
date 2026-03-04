use std::sync::Arc;

use crate::store::index::SecondaryIndex;
use crate::store::proc::{Proc, ProcType};
use crate::store::registry::{Server, ServerProtocol, PROCS, SERVERS, TABLES};
use crate::store::table::Table;
use crate::store::worker::{Worker, WorkerClass};
use crate::types::query_response::QueryResponse;
use crate::util::json::normalize_json_keys;
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
            Ok(v) => normalize_json_keys(v),
            Err(e) => {
                return QueryResponse::err(format!("Invalid JSON options: {}", e));
            }
        }
    };

    let table_size = opts
        .get("size")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);
    let force_limit = opts
        .get("forcelimit")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1) as i32;
    let force_offset = opts
        .get("forceoffset")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1) as i32;
    let max_scan_time_ms = opts
        .get("maxscantimems")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let buffer_size = opts.get("buffersize").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let long_poll_max = opts
        .get("longpollmaxclients")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as usize;
    let silent = opts
        .get("silent")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let buffer_long_poll_ms = opts
        .get("bufferlongpollms")
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
            Ok(v) => normalize_json_keys(v),
            Err(e) => {
                return QueryResponse::err(format!("Invalid JSON options: {}", e));
            }
        }
    };

    let table_name = match opts.get("table").and_then(|v| v.as_str()) {
        Some(t) => t.to_string(),
        None => return QueryResponse::err("Table is required for CREATE INDEX."),
    };

    let column = match opts.get("column").and_then(|v| v.as_str()) {
        Some(c) => c.to_string(),
        None => return QueryResponse::err("Column is required for CREATE INDEX."),
    };

    let unique = opts
        .get("unique")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

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
            Ok(v) => normalize_json_keys(v),
            Err(e) => {
                return QueryResponse::err(format!("Invalid JSON options: {}", e));
            }
        }
    };

    let bind = match opts.get("bind").and_then(|v| v.as_str()) {
        Some(b) => b.to_string(),
        None => return QueryResponse::err("Bind address is required."),
    };

    let timeout_ms = opts
        .get("timeoutms")
        .and_then(|v| v.as_i64())
        .unwrap_or(30000) as i32;
    let buffer_size = opts
        .get("buffersize")
        .and_then(|v| v.as_u64())
        .unwrap_or(65536) as usize;
    let force_limit = opts
        .get("forcelimit")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1) as i32;
    let force_offset = opts
        .get("forceoffset")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1) as i32;
    let max_scan_time_ms = opts
        .get("maxscantimems")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let latency_target_ms = opts
        .get("latencytargetms")
        .and_then(|v| v.as_f64())
        .unwrap_or(-1.0);

    let table = opts
        .get("table")
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
        .get("scriptfilename")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    server.document_root = opts
        .get("documentroot")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    server.https_redirect = opts
        .get("httpsredirect")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    server.https_redirect_header = opts
        .get("httpsredirectheader")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    server.https_redirect_on = opts
        .get("httpsredirecton")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    server.robots = opts
        .get("robots")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    server.gzip = opts.get("gzip").and_then(|v| v.as_bool()).unwrap_or(false);

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
            Ok(v) => normalize_json_keys(v),
            Err(e) => {
                return QueryResponse::err(format!("Invalid JSON options: {}", e));
            }
        }
    };

    let mut proc = Proc::new(name.to_string(), proc_type);

    proc.src = opts
        .get("src")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.dest = opts
        .get("dest")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.table = opts
        .get("table")
        .or_else(|| opts.get("src"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.case_query_string = opts
        .get("casequery")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.patch_string = opts
        .get("patch")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.post_wait_ms = opts.get("postwaitms").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    proc.enabled = opts
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    proc.wait_ms = opts.get("waitms").and_then(|v| v.as_i64()).unwrap_or(0) as i32;

    // Encryption/decryption
    proc.encrypt_key = opts
        .get("encryptkey")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.encrypt_fields = opts
        .get("encryptfields")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.decrypt_key = opts
        .get("decryptkey")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.decrypt_fields = opts
        .get("decryptfields")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Gzip
    proc.gzip_fields = opts
        .get("gzipfields")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.gunzip_fields = opts
        .get("gunzipfields")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Read reduce
    proc.reduce_key = opts
        .get("reducekey")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.reduce_to_latest = opts
        .get("reducetolatest")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Auto-reply
    proc.reply_status = opts
        .get("replystatus")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.reply_body = opts
        .get("replybody")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Time-based
    proc.start_time_string = opts
        .get("starttime")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    proc.end_time_string = opts
        .get("endtime")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Extended case query method
    if let Some(method) = opts.get("extcasequerymethod").and_then(|v| v.as_str()) {
        match method.to_uppercase().as_str() {
            "SUCCESS" => {
                proc.ext_case_query_method = crate::store::proc::ExtCaseQueryMethod::Success
            }
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
            Ok(v) => normalize_json_keys(v),
            Err(e) => {
                return QueryResponse::err(format!("Invalid JSON options: {}", e));
            }
        }
    };

    // Create worker data row from options
    let mut data = crate::types::row::OwnedRow::new();
    if let serde_json::Value::Object(map) = opts {
        for (k, v) in map {
            data.columns.insert(k, crate::types::value::Value::from(v));
        }
    }

    // Store worker class in data
    data.columns.insert(
        "workerclass".into(),
        crate::types::value::Value::String(class.as_str().to_string()),
    );

    // Extract common worker config from data
    let table = data
        .columns
        .get("table")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let frequency_ms = data
        .columns
        .get("frequencyms")
        .and_then(|v| match v {
            crate::types::value::Value::Number(n) => Some(*n as u64),
            _ => None,
        })
        .unwrap_or(1000);
    let burst = data
        .columns
        .get("burst")
        .and_then(|v| match v {
            crate::types::value::Value::Number(n) => Some(*n as i32),
            _ => None,
        })
        .unwrap_or(1);
    let max_runs = data
        .columns
        .get("maxruns")
        .and_then(|v| match v {
            crate::types::value::Value::Number(n) => Some(*n as i64),
            _ => None,
        })
        .unwrap_or(0);

    let (tx, rx) = tokio::sync::watch::channel(false);
    let worker = Worker::new(name.to_string(), class, data, tx);
    let worker_arc = Arc::new(worker);
    WORKERS.insert(name.to_string(), worker_arc.clone());

    // Spawn the worker loop based on class
    let handle = match class {
        WorkerClass::PushToStdout => {
            let w = Arc::new(crate::worker::push_to_stdout::PushToStdoutWorker::new(
                name.to_string(),
                table,
                frequency_ms,
                burst,
                max_runs,
                rx,
            ));
            Some(tokio::spawn(crate::worker::base::run_worker_loop(w)))
        }
        WorkerClass::PushToStderr => {
            let w = Arc::new(crate::worker::push_to_stderr::PushToStderrWorker::new(
                name.to_string(),
                table,
                frequency_ms,
                burst,
                max_runs,
                rx,
            ));
            Some(tokio::spawn(crate::worker::base::run_worker_loop(w)))
        }
        WorkerClass::PushToFile => {
            let file_path = worker_arc
                .data
                .columns
                .get("filepath")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let format = worker_arc
                .data
                .columns
                .get("format")
                .and_then(|v| v.as_str())
                .unwrap_or("json")
                .to_string();
            let gzip = worker_arc
                .data
                .columns
                .get("gzip")
                .and_then(|v| match v {
                    crate::types::value::Value::Bool(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false);
            let encrypt_key = worker_arc
                .data
                .columns
                .get("encryptkey")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let w = Arc::new(crate::worker::push_to_file::PushToFileWorker::new(
                name.to_string(),
                table,
                file_path,
                format,
                gzip,
                encrypt_key,
                frequency_ms,
                burst,
                max_runs,
                rx,
            ));
            Some(tokio::spawn(crate::worker::base::run_worker_loop(w)))
        }
        WorkerClass::ExecQuery => {
            let query = worker_arc
                .data
                .columns
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let w = Arc::new(crate::worker::exec_query::ExecQueryWorker::new(
                name.to_string(),
                table,
                query,
                frequency_ms,
                burst,
                max_runs,
                rx,
            ));
            Some(tokio::spawn(crate::worker::base::run_worker_loop(w)))
        }
        WorkerClass::PushToSlack => {
            let query = worker_arc
                .data
                .columns
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let dest_table = worker_arc
                .data
                .columns
                .get("desttable")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let token = worker_arc
                .data
                .columns
                .get("token")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let channel = worker_arc
                .data
                .columns
                .get("channel")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let text = worker_arc
                .data
                .columns
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let text_only = worker_arc
                .data
                .columns
                .get("textonly")
                .and_then(|v| match v {
                    crate::types::value::Value::Bool(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false);
            let w = Arc::new(crate::worker::push_to_slack::PushToSlackWorker::new(
                name.to_string(),
                table,
                query,
                dest_table,
                token,
                channel,
                text,
                text_only,
                frequency_ms,
                burst,
                max_runs,
                rx,
            ));
            Some(tokio::spawn(crate::worker::base::run_worker_loop(w)))
        }
        WorkerClass::PushToEmail => {
            let query = worker_arc
                .data
                .columns
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let dest_table = worker_arc
                .data
                .columns
                .get("desttable")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let to = worker_arc
                .data
                .columns
                .get("to")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let from = worker_arc
                .data
                .columns
                .get("from")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let from_name = worker_arc
                .data
                .columns
                .get("fromname")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let subject = worker_arc
                .data
                .columns
                .get("subject")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let text = worker_arc
                .data
                .columns
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let smtp_host = worker_arc
                .data
                .columns
                .get("smtphost")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let smtp_port = worker_arc
                .data
                .columns
                .get("smtpport")
                .and_then(|v| match v {
                    crate::types::value::Value::Number(n) => Some(*n as u16),
                    _ => None,
                })
                .unwrap_or(587);
            let smtp_user = worker_arc
                .data
                .columns
                .get("smtpuser")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let smtp_password = worker_arc
                .data
                .columns
                .get("smtppassword")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let gzip = worker_arc
                .data
                .columns
                .get("gzip")
                .and_then(|v| match v {
                    crate::types::value::Value::Bool(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false);
            let text_only = worker_arc
                .data
                .columns
                .get("textonly")
                .and_then(|v| match v {
                    crate::types::value::Value::Bool(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false);
            let encrypt_key = worker_arc
                .data
                .columns
                .get("encryptkey")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let w = Arc::new(crate::worker::push_to_email::PushToEmailWorker::new(
                name.to_string(),
                table,
                query,
                dest_table,
                to,
                from,
                from_name,
                subject,
                text,
                smtp_host,
                smtp_port,
                smtp_user,
                smtp_password,
                gzip,
                text_only,
                encrypt_key,
                frequency_ms,
                burst,
                max_runs,
                rx,
            ));
            Some(tokio::spawn(crate::worker::base::run_worker_loop(w)))
        }
        WorkerClass::PushToElasticsearch => {
            let query = worker_arc
                .data
                .columns
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let dest_table = worker_arc
                .data
                .columns
                .get("desttable")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let host = worker_arc
                .data
                .columns
                .get("host")
                .and_then(|v| v.as_str())
                .unwrap_or("http://localhost:9200")
                .to_string();
            let es_index = worker_arc
                .data
                .columns
                .get("esindex")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let es_type = worker_arc
                .data
                .columns
                .get("estype")
                .and_then(|v| v.as_str())
                .unwrap_or("_doc")
                .to_string();
            let w = Arc::new(
                crate::worker::push_to_elasticsearch::PushToElasticsearchWorker::new(
                    name.to_string(),
                    table,
                    query,
                    dest_table,
                    host,
                    es_index,
                    es_type,
                    frequency_ms,
                    burst,
                    max_runs,
                    rx,
                ),
            );
            Some(tokio::spawn(crate::worker::base::run_worker_loop(w)))
        }
        WorkerClass::PushToPagerDuty => {
            let query = worker_arc
                .data
                .columns
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let dest_table = worker_arc
                .data
                .columns
                .get("desttable")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let source = worker_arc
                .data
                .columns
                .get("source")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let severity = worker_arc
                .data
                .columns
                .get("severity")
                .and_then(|v| v.as_str())
                .unwrap_or("error")
                .to_string();
            let routing_key = worker_arc
                .data
                .columns
                .get("routingkey")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let link = worker_arc
                .data
                .columns
                .get("link")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let link_text = worker_arc
                .data
                .columns
                .get("linktext")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let w = Arc::new(
                crate::worker::push_to_pagerduty::PushToPagerDutyWorker::new(
                    name.to_string(),
                    table,
                    query,
                    dest_table,
                    source,
                    severity,
                    routing_key,
                    link,
                    link_text,
                    frequency_ms,
                    burst,
                    max_runs,
                    rx,
                ),
            );
            Some(tokio::spawn(crate::worker::base::run_worker_loop(w)))
        }
        WorkerClass::PushToFastCgi => {
            let query = worker_arc
                .data
                .columns
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let dest_table = worker_arc
                .data
                .columns
                .get("desttable")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let reply_table = worker_arc
                .data
                .columns
                .get("replytable")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let host = worker_arc
                .data
                .columns
                .get("host")
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1")
                .to_string();
            let port = worker_arc
                .data
                .columns
                .get("port")
                .and_then(|v| match v {
                    crate::types::value::Value::Number(n) => Some(*n as u16),
                    _ => None,
                })
                .unwrap_or(9000);
            let script_filename = worker_arc
                .data
                .columns
                .get("scriptfilename")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let two_way = worker_arc
                .data
                .columns
                .get("twoway")
                .and_then(|v| match v {
                    crate::types::value::Value::Bool(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false);
            let w = Arc::new(crate::worker::push_to_fastcgi::PushToFastcgiWorker::new(
                name.to_string(),
                table,
                query,
                dest_table,
                reply_table,
                host,
                port,
                script_filename,
                two_way,
                frequency_ms,
                burst,
                max_runs,
                rx,
            ));
            Some(tokio::spawn(crate::worker::base::run_worker_loop(w)))
        }
        WorkerClass::PushToCloudStorage => {
            let query = worker_arc
                .data
                .columns
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let dest_table = worker_arc
                .data
                .columns
                .get("desttable")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let key = worker_arc
                .data
                .columns
                .get("key")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let secret = worker_arc
                .data
                .columns
                .get("secret")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let bucket = worker_arc
                .data
                .columns
                .get("bucket")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let region = worker_arc
                .data
                .columns
                .get("region")
                .and_then(|v| v.as_str())
                .unwrap_or("us-east-1")
                .to_string();
            let provider_url = worker_arc
                .data
                .columns
                .get("providerurl")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let path = worker_arc
                .data
                .columns
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let output_mode = worker_arc
                .data
                .columns
                .get("outputmode")
                .and_then(|v| v.as_str())
                .unwrap_or("json")
                .to_string();
            let gzip = worker_arc
                .data
                .columns
                .get("gzip")
                .and_then(|v| match v {
                    crate::types::value::Value::Bool(b) => Some(*b),
                    _ => None,
                })
                .unwrap_or(false);
            let encrypt_key = worker_arc
                .data
                .columns
                .get("encryptkey")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let w = Arc::new(
                crate::worker::push_to_cloud_storage::PushToCloudStorageWorker::new(
                    name.to_string(),
                    table,
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
                    frequency_ms,
                    burst,
                    max_runs,
                    rx,
                ),
            );
            Some(tokio::spawn(crate::worker::base::run_worker_loop(w)))
        }
        WorkerClass::MySqlDataTransfer => {
            let query = worker_arc
                .data
                .columns
                .get("query")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let dest_table = worker_arc
                .data
                .columns
                .get("desttable")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mode_str = worker_arc
                .data
                .columns
                .get("mode")
                .and_then(|v| v.as_str())
                .unwrap_or("1_WAY_PUSH");
            let mode = crate::worker::mysql_data_transfer::TransferMode::parse(mode_str)
                .unwrap_or(crate::worker::mysql_data_transfer::TransferMode::OneWayPush);
            let remote_dsn = worker_arc
                .data
                .columns
                .get("remotedsn")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let remote_table = worker_arc
                .data
                .columns
                .get("remotetable")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let reply_table = worker_arc
                .data
                .columns
                .get("replytable")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let database_name = worker_arc
                .data
                .columns
                .get("databasename")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let user = worker_arc
                .data
                .columns
                .get("user")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let password = worker_arc
                .data
                .columns
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let host = worker_arc
                .data
                .columns
                .get("host")
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1:3306")
                .to_string();
            let max_request_time_ms = worker_arc
                .data
                .columns
                .get("maxrequesttimems")
                .and_then(|v| match v {
                    crate::types::value::Value::Number(n) => Some(*n as u64),
                    _ => None,
                })
                .unwrap_or(30000);
            let w = Arc::new(
                crate::worker::mysql_data_transfer::MysqlDataTransferWorker::new(
                    name.to_string(),
                    table,
                    query,
                    dest_table,
                    mode,
                    remote_dsn,
                    remote_table,
                    reply_table,
                    database_name,
                    user,
                    password,
                    host,
                    max_request_time_ms,
                    frequency_ms,
                    burst,
                    max_runs,
                    rx,
                ),
            );
            Some(tokio::spawn(crate::worker::base::run_worker_loop(w)))
        }
        WorkerClass::BufferAppl => {
            let time_dilation = worker_arc
                .data
                .columns
                .get("timedilation")
                .and_then(|v| match v {
                    crate::types::value::Value::Number(n) => Some(*n),
                    _ => None,
                })
                .unwrap_or(1.0);
            let w = Arc::new(crate::worker::buffer_appl::BufferApplWorker::new(
                name.to_string(),
                table,
                time_dilation,
                frequency_ms,
                max_runs,
                rx,
            ));
            Some(tokio::spawn(crate::worker::buffer_appl::run_buffer_appl(w)))
        }
    };

    // Store the task handle
    if let Some(h) = handle {
        *worker_arc.handle.lock() = Some(h);
    }

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!(
            "START WORKER: started {} worker {}",
            class.as_str(),
            name
        ));
    }

    QueryResponse::ok_bool()
}
