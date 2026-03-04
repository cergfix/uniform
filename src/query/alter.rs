use crate::store::registry::{PROCS, SERVERS, TABLES};
use crate::types::query_response::QueryResponse;
use crate::util::json::normalize_json_keys;
use crate::util::logging;

/// ALTER TABLE name (options)
pub fn alter_table_cmd(name: &str, options_str: &str) -> QueryResponse {
    let _table = match crate::store::registry::get_table(name) {
        Some(t) => t,
        None => return QueryResponse::err(format!("Table doesn't exist: {}", name)),
    };

    let opts: serde_json::Value = match serde_json::from_str(options_str) {
        Ok(v) => normalize_json_keys(v),
        Err(e) => return QueryResponse::err(format!("Invalid JSON options: {}", e)),
    };

    // ALTER TABLE options modify table properties
    // Since Table is behind Arc, we need to remove + modify + reinsert
    if let Some((_, table_arc)) = TABLES.remove(name) {
        match std::sync::Arc::try_unwrap(table_arc) {
            Ok(mut table) => {
                if let Some(v) = opts.get("forcelimit").and_then(|v| v.as_i64()) {
                    table.force_limit = v as i32;
                }
                if let Some(v) = opts.get("forceoffset").and_then(|v| v.as_i64()) {
                    table.force_offset = v as i32;
                }
                if let Some(v) = opts.get("maxscantimems").and_then(|v| v.as_i64()) {
                    table.max_scan_time_ms = v as i32;
                }
                if let Some(v) = opts.get("silent").and_then(|v| v.as_bool()) {
                    table.silent = v;
                }
                if let Some(v) = opts.get("longpollmaxclients").and_then(|v| v.as_u64()) {
                    table.long_poll_max_clients = v as usize;
                }

                TABLES.insert(name.to_string(), std::sync::Arc::new(table));

                if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                    logging::log(&format!("ALTER TABLE: modified table {}", name));
                }
                QueryResponse::ok_bool()
            }
            Err(arc) => {
                TABLES.insert(name.to_string(), arc);
                QueryResponse::err("Cannot modify table with active references")
            }
        }
    } else {
        QueryResponse::err(format!("Table doesn't exist: {}", name))
    }
}

/// ALTER SERVER name (options)
pub fn alter_server_cmd(name: &str, options_str: &str) -> QueryResponse {
    let _server = match crate::store::registry::get_server(name) {
        Some(s) => s,
        None => return QueryResponse::err(format!("Server doesn't exist: {}", name)),
    };

    let opts: serde_json::Value = match serde_json::from_str(options_str) {
        Ok(v) => normalize_json_keys(v),
        Err(e) => return QueryResponse::err(format!("Invalid JSON options: {}", e)),
    };

    // Since Server is behind Arc, we need to remove + modify + reinsert
    if let Some((_, server_arc)) = SERVERS.remove(name) {
        match std::sync::Arc::try_unwrap(server_arc) {
            Ok(mut server) => {
                if let Some(v) = opts.get("forcelimit").and_then(|v| v.as_i64()) {
                    server.force_limit = v as i32;
                }
                if let Some(v) = opts.get("forceoffset").and_then(|v| v.as_i64()) {
                    server.force_offset = v as i32;
                }
                if let Some(v) = opts.get("maxscantimems").and_then(|v| v.as_i64()) {
                    server.max_scan_time_ms = v as i32;
                }
                if let Some(v) = opts.get("timeoutms").and_then(|v| v.as_i64()) {
                    server.timeout_ms = v as i32;
                }
                if let Some(v) = opts.get("latencytargetms").and_then(|v| v.as_f64()) {
                    server.latency_target_ms = v;
                }

                SERVERS.insert(name.to_string(), std::sync::Arc::new(server));
                QueryResponse::ok_bool()
            }
            Err(arc) => {
                SERVERS.insert(name.to_string(), arc);
                QueryResponse::err("Cannot modify server with active references")
            }
        }
    } else {
        QueryResponse::err(format!("Server doesn't exist: {}", name))
    }
}

/// ALTER PROC name (options)
pub fn alter_proc_cmd(name: &str, options_str: &str) -> QueryResponse {
    let opts: serde_json::Value = match serde_json::from_str(options_str) {
        Ok(v) => normalize_json_keys(v),
        Err(e) => return QueryResponse::err(format!("Invalid JSON options: {}", e)),
    };

    let mut procs = PROCS.write();
    let proc = match procs.iter_mut().find(|p| p.name == name) {
        Some(p) => p,
        None => return QueryResponse::err(format!("Proc doesn't exist: {}", name)),
    };

    if let Some(v) = opts.get("enabled").and_then(|v| v.as_bool()) {
        proc.enabled = v;
    }
    if let Some(v) = opts.get("src").and_then(|v| v.as_str()) {
        proc.src = v.to_string();
    }
    if let Some(v) = opts.get("dest").and_then(|v| v.as_str()) {
        proc.dest = v.to_string();
    }
    if let Some(v) = opts.get("postwaitms").and_then(|v| v.as_i64()) {
        proc.post_wait_ms = v as i32;
    }
    if let Some(v) = opts.get("waitms").and_then(|v| v.as_i64()) {
        proc.wait_ms = v as i32;
    }
    if let Some(v) = opts.get("casequery").and_then(|v| v.as_str()) {
        proc.case_query_string = v.to_string();
    }
    if let Some(v) = opts.get("patch").and_then(|v| v.as_str()) {
        proc.patch_string = v.to_string();
        // Re-parse patch data
        if let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(v) {
            let patch: std::collections::HashMap<String, crate::types::value::Value> = map
                .into_iter()
                .map(|(k, v)| (k, crate::types::value::Value::from(v)))
                .collect();
            proc.patch_data = Some(patch);
        }
    }
    if let Some(v) = opts.get("reducekey").and_then(|v| v.as_str()) {
        proc.reduce_key = v.to_string();
    }
    if let Some(v) = opts.get("reducetolatest").and_then(|v| v.as_bool()) {
        proc.reduce_to_latest = v;
    }
    if let Some(v) = opts.get("encryptkey").and_then(|v| v.as_str()) {
        proc.encrypt_key = v.to_string();
    }
    if let Some(v) = opts.get("encryptfields").and_then(|v| v.as_str()) {
        proc.encrypt_fields = v.to_string();
    }
    if let Some(v) = opts.get("decryptkey").and_then(|v| v.as_str()) {
        proc.decrypt_key = v.to_string();
    }
    if let Some(v) = opts.get("decryptfields").and_then(|v| v.as_str()) {
        proc.decrypt_fields = v.to_string();
    }
    if let Some(v) = opts.get("gzipfields").and_then(|v| v.as_str()) {
        proc.gzip_fields = v.to_string();
    }
    if let Some(v) = opts.get("gunzipfields").and_then(|v| v.as_str()) {
        proc.gunzip_fields = v.to_string();
    }
    if let Some(v) = opts.get("replystatus").and_then(|v| v.as_str()) {
        proc.reply_status = v.to_string();
    }
    if let Some(v) = opts.get("replybody").and_then(|v| v.as_str()) {
        proc.reply_body = v.to_string();
    }
    if let Some(v) = opts.get("starttime").and_then(|v| v.as_str()) {
        proc.start_time_string = v.to_string();
    }
    if let Some(v) = opts.get("endtime").and_then(|v| v.as_str()) {
        proc.end_time_string = v.to_string();
    }

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!("ALTER PROC: modified proc {}", name));
    }

    QueryResponse::ok_bool()
}
