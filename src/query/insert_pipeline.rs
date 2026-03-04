use std::collections::HashMap;

use crate::compression::gzip as gzip_util;
use crate::crypto::aes_cbc;
use crate::server::connection::Connection;
use crate::store::proc::{Proc, ProcType};
use crate::store::registry::{self, PROCS};
use crate::types::row::OwnedRow;
use crate::types::value::Value;
use crate::util::logging;

/// Full insert pipeline — replaces Go's `insert()` function.
/// Runs all proc stages in order, then writes to table.
/// Returns (success, error_string, optional_select_result, row_id).
pub fn insert(
    table_name: &str,
    row: &mut OwnedRow,
    metadata: &OwnedRow,
    no_buffer: bool,
    _conn: Option<&Connection>,
) -> (bool, String, Option<Vec<OwnedRow>>, String) {
    if row.columns.is_empty() {
        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log("Tried to write empty record, returning failure.");
        }
        return (
            false,
            "Cannot write empty record".into(),
            None,
            String::new(),
        );
    }

    if table_name.is_empty() {
        return (
            false,
            "Empty tableName specified.".into(),
            None,
            String::new(),
        );
    }

    // Generate u_created_at if not set
    if !row.columns.contains_key("u_created_at")
        || row.columns.get("u_created_at") == Some(&Value::String(String::new()))
    {
        let now = chrono::Utc::now();
        row.columns.insert(
            "u_created_at".into(),
            Value::String(now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        );
    }

    // Add cluster version header
    let role = crate::util::cluster::get_cluster_role();
    row.columns.insert(
        format!("x_uniform_{}", role),
        Value::String(crate::config::vars::version().to_string()),
    );

    let table = match registry::get_table(table_name) {
        Some(t) => t,
        None => {
            if logging::get_log_level() >= logging::LOG_LEVEL_CRIT {
                logging::log(&format!("Table <{}> doesn't exist.", table_name));
            }
            return (false, "Table doesn't exist.".into(), None, String::new());
        }
    };

    // Generate u_id if not set
    if !row.columns.contains_key("u_id")
        || row.columns.get("u_id") == Some(&Value::String(String::new()))
    {
        row.columns.insert(
            "u_id".into(),
            Value::String(uuid::Uuid::new_v4().to_string()),
        );
    }

    let row_id = match row.columns.get("u_id") {
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    };

    let procs = PROCS.read();
    let mut moved = false;
    let mut _moved_to_buffer = false;
    let mut insert_select_result: Option<Vec<OwnedRow>> = None;

    // ========== STAGE 1: DEFLECT_WRITE ==========
    for proc in procs.iter() {
        if proc.enabled
            && proc.src == table_name
            && proc.proc_type == ProcType::DeflectWrite
            && is_proc_allowed(proc, metadata, row)
            && registry::get_table(&proc.dest).is_some()
        {
            filter_row_keys(row, &proc.case_query_string);
            apply_patch(row, proc);

            let (ok, err, result, _) = insert(&proc.dest, row, metadata, false, _conn);
            if !ok || !err.is_empty() {
                if logging::get_log_level() >= logging::LOG_LEVEL_CRIT && !table.silent {
                    logging::log(&format!(
                        "Could not execute DEFLECT_WRITE for src='{}', dest='{}', reason: {}",
                        proc.src, proc.dest, err
                    ));
                }
            } else {
                moved = true;
                insert_select_result = result;
            }

            post_wait(proc);
            break;
        }
    }

    // ========== STAGE 2: BUFFER_CATCH ==========
    if !no_buffer && !moved && table.buffer_max_size > 0 {
        for proc in procs.iter() {
            if proc.enabled
                && proc.table == table_name
                && proc.proc_type == ProcType::BufferCatch
                && is_proc_allowed(proc, metadata, row)
            {
                filter_row_keys(row, &proc.case_query_string);
                apply_patch(row, proc);

                if let Some(tx) = &table.buffer_tx {
                    match tx.try_send(row.clone()) {
                        Ok(_) => {
                            moved = true;
                            _moved_to_buffer = true;
                        }
                        Err(_) => {
                            if logging::get_log_level() >= logging::LOG_LEVEL_CRIT && !table.silent
                            {
                                logging::log(&format!("Table buffer full, table={}", table_name));
                            }
                        }
                    }
                }

                post_wait(proc);
                break;
            }
        }
    }

    if !moved {
        // ========== STAGE 3: WRITE_OFFLINE ==========
        for proc in procs.iter() {
            if proc.enabled
                && proc.table == table_name
                && proc.proc_type == ProcType::WriteOffline
                && is_proc_allowed(proc, metadata, row)
            {
                return (
                    false,
                    "Table is in WRITE_OFFLINE mode.".into(),
                    None,
                    String::new(),
                );
            }
        }

        // ========== STAGE 4: MIRROR_WRITE ==========
        for proc in procs.iter() {
            if proc.enabled
                && proc.src == table_name
                && proc.proc_type == ProcType::MirrorWrite
                && is_proc_allowed(proc, metadata, row)
            {
                if registry::get_table(&proc.dest).is_some() {
                    let mut nr = row.clone();
                    filter_row_keys(&mut nr, &proc.case_query_string);
                    apply_patch(&mut nr, proc);

                    let (ok, err, _, _) = insert(&proc.dest, &mut nr, metadata, false, _conn);
                    if !ok
                        && !err.is_empty()
                        && logging::get_log_level() >= logging::LOG_LEVEL_CRIT
                        && !table.silent
                    {
                        logging::log(&format!(
                            "Could not execute MIRROR_WRITE for src='{}', dest='{}', reason: {}",
                            proc.src, proc.dest, err
                        ));
                    }
                }
                post_wait(proc);
            }
        }

        // ========== STAGE 5: GUNZIP_WRITE ==========
        for proc in procs.iter() {
            if proc.enabled
                && proc.table == table_name
                && proc.proc_type == ProcType::GunzipWrite
                && is_proc_allowed(proc, metadata, row)
            {
                filter_row_keys(row, &proc.case_query_string);
                apply_patch(row, proc);
                gunzip_fields(row, &proc.gunzip_fields, &table.name, table.silent);
                post_wait(proc);
            }
        }

        // ========== STAGE 6: GZIP_WRITE ==========
        for proc in procs.iter() {
            if proc.enabled
                && proc.table == table_name
                && proc.proc_type == ProcType::GzipWrite
                && is_proc_allowed(proc, metadata, row)
            {
                filter_row_keys(row, &proc.case_query_string);
                apply_patch(row, proc);
                gzip_fields(row, &proc.gzip_fields, &table.name, table.silent);
                post_wait(proc);
            }
        }

        // ========== STAGE 7: DECRYPT_WRITE ==========
        for proc in procs.iter() {
            if proc.enabled
                && proc.table == table_name
                && proc.proc_type == ProcType::DecryptWrite
                && is_proc_allowed(proc, metadata, row)
            {
                filter_row_keys(row, &proc.case_query_string);
                apply_patch(row, proc);
                decrypt_fields(
                    row,
                    &proc.decrypt_fields,
                    &proc.decrypt_key,
                    &table.name,
                    table.silent,
                );
                post_wait(proc);
            }
        }

        // ========== STAGE 8: ENCRYPT_WRITE ==========
        for proc in procs.iter() {
            if proc.enabled
                && proc.table == table_name
                && proc.proc_type == ProcType::EncryptWrite
                && is_proc_allowed(proc, metadata, row)
            {
                filter_row_keys(row, &proc.case_query_string);
                apply_patch(row, proc);
                encrypt_fields(
                    row,
                    &proc.encrypt_fields,
                    &proc.encrypt_key,
                    &table.name,
                    table.silent,
                );
                post_wait(proc);
            }
        }

        // ========== STAGE 9: AUTO_REPLY ==========
        let mut auto_reply = false;
        for proc in procs.iter() {
            if proc.enabled
                && proc.src == table_name
                && proc.proc_type == ProcType::AutoReply
                && is_proc_allowed(proc, metadata, row)
            {
                if registry::get_table(&proc.dest).is_some() {
                    let mut reply = OwnedRow::new();
                    reply
                        .columns
                        .insert("Status".into(), Value::String(proc.reply_status.clone()));
                    reply
                        .columns
                        .insert("u_body".into(), Value::String(proc.reply_body.clone()));
                    if let Some(id) = row.columns.get("u_id") {
                        reply.columns.insert("u_reply_id".into(), id.clone());
                    }
                    filter_row_keys(&mut reply, &proc.case_query_string);
                    apply_patch(&mut reply, proc);

                    let (ok, err, _, _) = insert(&proc.dest, &mut reply, metadata, false, _conn);
                    if !ok && !err.is_empty() {
                        if logging::get_log_level() >= logging::LOG_LEVEL_CRIT && !table.silent {
                            logging::log(&format!(
                                "Could not execute AUTO_REPLY for src='{}', dest='{}', reason: {}",
                                proc.src, proc.dest, err
                            ));
                        }
                    } else {
                        auto_reply = true;
                    }
                }
                post_wait(proc);
                break;
            }
        }

        if !auto_reply {
            // ========== STAGE 10: READ_ONLY check ==========
            let mut read_only_mode = false;
            for proc in procs.iter() {
                if proc.enabled
                    && proc.table == table_name
                    && proc.proc_type == ProcType::ReadOnly
                    && is_proc_allowed(proc, metadata, row)
                {
                    read_only_mode = true;
                    break;
                }
            }

            // ========== STAGE 11: Long-poll delivery ==========
            let mut long_poll_success = false;
            {
                let mut clients = table.long_poll_clients.lock();
                if !clients.is_empty() {
                    for client in clients.iter_mut() {
                        if !client.is_expired()
                            && client.is_available()
                            && client.try_deliver(row.clone())
                        {
                            if !read_only_mode {
                                long_poll_success = true;
                            }
                            break;
                        }
                    }
                }
            }

            // ========== STAGE 12: Write to table ==========
            if !long_poll_success {
                match table.insert(row.columns.clone()) {
                    Ok(seq_id) => {
                        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG && !table.silent {
                            logging::log(&format!(
                                "INSERT: row seq_id={} into table {}",
                                seq_id, table_name
                            ));
                        }
                    }
                    Err(e) => {
                        // ========== STAGE 13: DEFLECT_WRITE_ON_FULL_TABLE ==========
                        if e == "Table full" {
                            for proc in procs.iter() {
                                if proc.enabled
                                    && proc.src == table_name
                                    && proc.proc_type == ProcType::DeflectWriteOnFullTable
                                    && is_proc_allowed(proc, metadata, row)
                                {
                                    if registry::get_table(&proc.dest).is_some() {
                                        filter_row_keys(row, &proc.case_query_string);
                                        apply_patch(row, proc);

                                        let (ok, err, _, _) =
                                            insert(&proc.dest, row, metadata, false, _conn);
                                        if !ok && !err.is_empty() {
                                            if logging::get_log_level() >= logging::LOG_LEVEL_CRIT
                                                && !table.silent
                                            {
                                                logging::log(&format!(
                                                        "Could not execute DEFLECT_WRITE_ON_FULL_TABLE for src='{}', dest='{}', reason: {}",
                                                        proc.src, proc.dest, err
                                                    ));
                                            }
                                        } else {
                                            return (true, String::new(), None, row_id);
                                        }
                                    }
                                    post_wait(proc);
                                    break;
                                }
                            }
                        }
                        return (false, e, None, String::new());
                    }
                }
            }
        }
    }

    (true, String::new(), insert_select_result, row_id)
}

// ---------- Helper functions ----------

/// Check if a proc is allowed to run (based on time restrictions and case_query_string conditions).
fn is_proc_allowed(proc: &Proc, _metadata: &OwnedRow, row: &OwnedRow) -> bool {
    // Time-based restrictions
    if !proc.start_time_string.is_empty() {
        if let Ok(start) = chrono::DateTime::parse_from_rfc3339(&proc.start_time_string) {
            if chrono::Utc::now() < start {
                return false;
            }
        }
    }
    if !proc.end_time_string.is_empty() {
        if let Ok(end) = chrono::DateTime::parse_from_rfc3339(&proc.end_time_string) {
            if chrono::Utc::now() > end {
                return false;
            }
        }
    }

    if proc.case_query_string.is_empty() {
        return true;
    }
    crate::query::condition::row_meets_conditions_simple(&proc.case_query_string, &row.columns)
}

/// Filter row keys based on a comma-separated key list from case_query_string.
fn filter_row_keys(_row: &mut OwnedRow, _case_query_string: &str) {
    // In Go, this extracts key names from the case query tree and keeps only those columns.
    // For now, no filtering (keep all columns).
}

/// Apply patch data to a row.
fn apply_patch(row: &mut OwnedRow, proc: &Proc) {
    if let Some(ref patch_data) = proc.patch_data {
        for (key, val) in patch_data {
            row.columns.insert(key.clone(), val.clone());
        }
    }
}

/// Post-wait after a proc stage.
fn post_wait(proc: &Proc) {
    if proc.post_wait_ms > 0 {
        std::thread::sleep(std::time::Duration::from_millis(proc.post_wait_ms as u64));
    }
}

/// Gunzip (decompress) specified fields in the row.
fn gunzip_fields(row: &mut OwnedRow, fields_str: &str, table_name: &str, silent: bool) {
    if fields_str.is_empty() {
        return;
    }
    let fields: Vec<&str> = fields_str.split(',').map(|s| s.trim()).collect();
    let wildcard = fields_str.contains('*');

    let keys: Vec<String> = row.columns.keys().cloned().collect();
    for key in keys {
        let should_process = wildcard || fields.iter().any(|f| *f == key);
        if should_process {
            if let Some(val) = row.columns.get(&key) {
                let val_str = val.to_string_repr();
                match gzip_util::gunzip_field(&val_str) {
                    Ok(decoded) => {
                        row.columns.insert(key, Value::String(decoded));
                    }
                    Err(e) => {
                        if logging::get_log_level() >= logging::LOG_LEVEL_CRIT && !silent {
                            logging::log(&format!(
                                "GUNZIP_WRITE error for table='{}', column='{}': {}",
                                table_name, key, e
                            ));
                        }
                    }
                }
            }
        }
    }
}

/// Gzip (compress) specified fields in the row.
fn gzip_fields(row: &mut OwnedRow, fields_str: &str, table_name: &str, silent: bool) {
    if fields_str.is_empty() {
        return;
    }
    let fields: Vec<&str> = fields_str.split(',').map(|s| s.trim()).collect();
    let wildcard = fields_str.contains('*');

    let keys: Vec<String> = row.columns.keys().cloned().collect();
    for key in keys {
        let should_process = wildcard || fields.iter().any(|f| *f == key);
        if should_process {
            if let Some(val) = row.columns.get(&key) {
                let val_str = val.to_string_repr();
                match gzip_util::gzip_field(&val_str) {
                    Ok(encoded) => {
                        row.columns.insert(key, Value::String(encoded));
                    }
                    Err(e) => {
                        if logging::get_log_level() >= logging::LOG_LEVEL_CRIT && !silent {
                            logging::log(&format!(
                                "GZIP_WRITE error for table='{}', column='{}': {}",
                                table_name, key, e
                            ));
                        }
                    }
                }
            }
        }
    }
}

/// Decrypt specified fields in the row.
fn decrypt_fields(row: &mut OwnedRow, fields_str: &str, key: &str, table_name: &str, silent: bool) {
    if fields_str.is_empty() || key.is_empty() {
        return;
    }
    let fields: Vec<&str> = fields_str.split(',').map(|s| s.trim()).collect();
    let wildcard = fields_str.contains('*');

    let keys: Vec<String> = row.columns.keys().cloned().collect();
    for col_key in keys {
        let should_process = wildcard || fields.iter().any(|f| *f == col_key);
        if should_process {
            if let Some(val) = row.columns.get(&col_key) {
                let val_str = val.to_string_repr();
                match aes_cbc::decrypt(&val_str, key) {
                    Ok(decrypted) => {
                        row.columns.insert(
                            col_key,
                            Value::String(String::from_utf8_lossy(&decrypted).into_owned()),
                        );
                    }
                    Err(e) => {
                        if logging::get_log_level() >= logging::LOG_LEVEL_CRIT && !silent {
                            logging::log(&format!(
                                "DECRYPT_WRITE error for table='{}', column='{}': {}",
                                table_name, col_key, e
                            ));
                        }
                    }
                }
            }
        }
    }
}

/// Encrypt specified fields in the row.
fn encrypt_fields(row: &mut OwnedRow, fields_str: &str, key: &str, table_name: &str, silent: bool) {
    if fields_str.is_empty() || key.is_empty() {
        return;
    }
    let fields: Vec<&str> = fields_str.split(',').map(|s| s.trim()).collect();
    let wildcard = fields_str.contains('*');

    let keys: Vec<String> = row.columns.keys().cloned().collect();
    for col_key in keys {
        let should_process = wildcard || fields.iter().any(|f| *f == col_key);
        if should_process {
            if let Some(val) = row.columns.get(&col_key) {
                let val_str = val.to_string_repr();
                match aes_cbc::encrypt(val_str.as_bytes(), key) {
                    Ok(encrypted) => {
                        row.columns.insert(col_key, Value::String(encrypted));
                    }
                    Err(e) => {
                        if logging::get_log_level() >= logging::LOG_LEVEL_CRIT && !silent {
                            logging::log(&format!(
                                "ENCRYPT_WRITE error for table='{}', column='{}': {}",
                                table_name, col_key, e
                            ));
                        }
                    }
                }
            }
        }
    }
}

/// Find the DEFLECT_READ proc for a table and return (new_table_name, patch_data, filter_keys).
/// Follows the DEFLECT_READ chain: src → dest, repeating until no more deflects.
fn find_deflect_read(
    table_name: &str,
    _metadata: &OwnedRow,
) -> (String, Option<HashMap<String, Value>>, Vec<String>) {
    let procs = PROCS.read();
    for proc in procs.iter() {
        if proc.enabled
            && proc.src == table_name
            && proc.proc_type == ProcType::DeflectRead
            && is_proc_allowed(proc, _metadata, &OwnedRow::new())
        {
            if proc.post_wait_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(proc.post_wait_ms as u64));
            }
            let patch = proc.patch_data.clone();
            return (proc.dest.clone(), patch, Vec::new());
        }
    }
    (String::new(), None, Vec::new())
}

/// Run the select (read) pipeline procs for a table.
/// Called with the original table_name (before deflect) and the current result rows.
/// Handles: DEFLECT_READ (pre-select), READ_OFFLINE, READ_ONLY (non-destructive),
/// LONG_POLL, DEFLECT_READ_ON_EMPTY, MIRROR_ON_READ, GUNZIP_ON_READ, GZIP_ON_READ,
/// DECRYPT_ON_READ, ENCRYPT_ON_READ, READ_REDUCE.
///
/// Returns: modified table name after DEFLECT_READ chain (for the SELECT caller to use),
/// read-only mode flag, and an optional error string.
pub fn resolve_deflect_read(
    table_name: &str,
    metadata: &OwnedRow,
) -> (String, Vec<Option<HashMap<String, Value>>>) {
    let mut current = table_name.to_string();
    let mut patches = Vec::new();

    loop {
        let (new_name, patch, _keys) = find_deflect_read(&current, metadata);
        if new_name.is_empty() || new_name == current {
            break;
        }
        patches.push(patch);
        current = new_name;
    }

    (current, patches)
}

/// Check READ_OFFLINE proc — returns true if the table is in read-offline mode.
pub fn check_read_offline(table_name: &str, metadata: &OwnedRow) -> bool {
    let procs = PROCS.read();
    for proc in procs.iter() {
        if proc.enabled
            && proc.table == table_name
            && proc.proc_type == ProcType::ReadOffline
            && is_proc_allowed(proc, metadata, &OwnedRow::new())
        {
            return true;
        }
    }
    false
}

/// Check READ_ONLY proc — returns true if the table is in read-only mode
/// (SELECT should not pop/remove rows).
pub fn check_read_only(table_name: &str, metadata: &OwnedRow) -> bool {
    let procs = PROCS.read();
    for proc in procs.iter() {
        if proc.enabled
            && proc.table == table_name
            && proc.proc_type == ProcType::ReadOnly
            && is_proc_allowed(proc, metadata, &OwnedRow::new())
        {
            return true;
        }
    }
    false
}

/// Run post-select pipeline procs on the result rows.
/// This handles: READ_REDUCE, DEFLECT_READ_ON_EMPTY, MIRROR_ON_READ,
/// GUNZIP_ON_READ, GZIP_ON_READ, DECRYPT_ON_READ, ENCRYPT_ON_READ.
pub fn run_select_pipeline(
    table_name: &str,
    rows: &mut Vec<OwnedRow>,
    metadata: &OwnedRow,
    _conn: Option<&Connection>,
    query: &str,
    deflect_patches: &[Option<HashMap<String, Value>>],
) {
    // Apply deflect patches to all rows
    for patch in deflect_patches.iter().flatten() {
        for row in rows.iter_mut() {
            for (key, val) in patch {
                row.columns.insert(key.clone(), val.clone());
            }
        }
    }

    let procs = PROCS.read();

    // ========== READ_REDUCE ==========
    if rows.len() > 1 {
        for proc in procs.iter() {
            if proc.enabled
                && proc.table == table_name
                && proc.proc_type == ProcType::ReadReduce
                && is_proc_allowed(proc, metadata, &OwnedRow::new())
            {
                let mut reduced_map: HashMap<String, OwnedRow> = HashMap::new();
                let mut non_key_rows: Vec<OwnedRow> = Vec::new();

                for row in rows.iter() {
                    if let Some(key_val) = row.columns.get(&proc.reduce_key) {
                        let key_str = key_val.to_string_repr();
                        if !reduced_map.contains_key(&key_str) {
                            reduced_map.insert(key_str.clone(), row.clone());
                        } else if let Some(existing) = reduced_map.get(&key_str) {
                            let replace = if proc.reduce_to_latest {
                                row.created > existing.created
                            } else {
                                row.created < existing.created
                            };
                            if replace {
                                reduced_map.insert(key_str, row.clone());
                            }
                        }
                    } else {
                        non_key_rows.push(row.clone());
                    }
                }

                let mut result = non_key_rows;
                for (_k, v) in reduced_map {
                    result.push(v);
                }
                *rows = result;

                post_wait(proc);
            }
        }
    }

    // If empty, try DEFLECT_READ_ON_EMPTY_RESPONSE
    // Extract proc info while holding the lock, then drop it before recursive call
    if rows.is_empty() {
        #[allow(clippy::type_complexity)]
        let deflect_info: Option<(String, Option<HashMap<String, Value>>, i32)> = {
            let mut found = None;
            for proc in procs.iter() {
                if proc.enabled
                    && proc.src == table_name
                    && proc.proc_type == ProcType::DeflectReadOnEmptyResponse
                    && is_proc_allowed(proc, metadata, &OwnedRow::new())
                {
                    let new_query = query.replace(table_name, &proc.dest);
                    found = Some((new_query, proc.patch_data.clone(), proc.post_wait_ms));
                    break;
                }
            }
            found
        };

        if let Some((new_query, patch_data, post_wait_ms)) = deflect_info {
            drop(procs); // release lock before recursive call

            let mut local_conn = Connection::new_local();
            let result =
                crate::query::engine::run_query(&new_query, None, &mut local_conn.metadata, None);
            if result.success && !result.result.is_empty() {
                let mut result_rows = result.result;
                if let Some(ref patch) = patch_data {
                    for r in result_rows.iter_mut() {
                        for (key, val) in patch {
                            r.columns.insert(key.clone(), val.clone());
                        }
                    }
                }
                *rows = result_rows;
            }

            if post_wait_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(post_wait_ms as u64));
            }
            return; // procs dropped, must return
        }
    }

    // ========== MIRROR_ON_READ ==========
    if !rows.is_empty() {
        for proc in procs.iter() {
            if proc.enabled
                && proc.src == table_name
                && proc.proc_type == ProcType::MirrorOnRead
                && is_proc_allowed(proc, metadata, &OwnedRow::new())
            {
                if registry::get_table(&proc.dest).is_some() {
                    for row in rows.iter() {
                        let mut mirror = row.clone();
                        apply_patch(&mut mirror, proc);
                        let _ = super::insert_pipeline::insert(
                            &proc.dest,
                            &mut mirror,
                            metadata,
                            false,
                            None,
                        );
                    }
                }
                post_wait(proc);
            }
        }

        // ========== GUNZIP_ON_READ ==========
        for proc in procs.iter() {
            if proc.enabled
                && proc.table == table_name
                && proc.proc_type == ProcType::GunzipOnRead
                && is_proc_allowed(proc, metadata, &OwnedRow::new())
            {
                for row in rows.iter_mut() {
                    apply_patch(row, proc);
                    gunzip_fields(row, &proc.gunzip_fields, table_name, false);
                }
                post_wait(proc);
            }
        }

        // ========== GZIP_ON_READ ==========
        for proc in procs.iter() {
            if proc.enabled
                && proc.table == table_name
                && proc.proc_type == ProcType::GzipOnRead
                && is_proc_allowed(proc, metadata, &OwnedRow::new())
            {
                for row in rows.iter_mut() {
                    apply_patch(row, proc);
                    gzip_fields(row, &proc.gzip_fields, table_name, false);
                }
                post_wait(proc);
            }
        }

        // ========== DECRYPT_ON_READ ==========
        for proc in procs.iter() {
            if proc.enabled
                && proc.table == table_name
                && proc.proc_type == ProcType::DecryptOnRead
                && is_proc_allowed(proc, metadata, &OwnedRow::new())
            {
                for row in rows.iter_mut() {
                    apply_patch(row, proc);
                    decrypt_fields(
                        row,
                        &proc.decrypt_fields,
                        &proc.decrypt_key,
                        table_name,
                        false,
                    );
                }
                post_wait(proc);
            }
        }

        // ========== ENCRYPT_ON_READ ==========
        for proc in procs.iter() {
            if proc.enabled
                && proc.table == table_name
                && proc.proc_type == ProcType::EncryptOnRead
                && is_proc_allowed(proc, metadata, &OwnedRow::new())
            {
                for row in rows.iter_mut() {
                    apply_patch(row, proc);
                    encrypt_fields(
                        row,
                        &proc.encrypt_fields,
                        &proc.encrypt_key,
                        table_name,
                        false,
                    );
                }
                post_wait(proc);
            }
        }
    }
}
