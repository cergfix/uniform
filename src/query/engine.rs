use std::sync::Arc;

use crate::config::vars;
use crate::query::command::{self, Command};
use crate::store::registry::{self, Server};
use crate::server::connection::Connection;
use crate::types::query_response::{QueryResponse, QueryType};
use crate::types::row::OwnedRow;
use crate::types::value::Value;
use crate::util::logging;

/// Run a query and return the response.
/// This is the main entry point — replaces Go's `RunQuery()`.
pub fn run_query(
    query: &str,
    server: Option<&Arc<Server>>,
    metadata: &mut OwnedRow,
    conn: Option<&Connection>,
) -> QueryResponse {
    let mut query_str = query.trim().to_string();

    if query_str.is_empty() || query_str.len() < 2 {
        return QueryResponse {
            success: true,
            err: "empty query".into(),
            ..QueryResponse::new()
        };
    }

    // Extract and apply metadata prefix
    if query_str.starts_with("/*") {
        let (meta_json, rest) = command::strip_metadata(&query_str);
        if let Some(json_str) = meta_json {
            match serde_json::from_str::<serde_json::Value>(&json_str) {
                Ok(serde_json::Value::Object(map)) => {
                    // Preserve Fb_id and Fb_created
                    let fb_id = metadata.columns.get("Fb_id").cloned();
                    let fb_created = metadata.columns.get("Fb_created").cloned();

                    for (key, val) in map {
                        match &val {
                            serde_json::Value::String(_)
                            | serde_json::Value::Number(_)
                            | serde_json::Value::Bool(_) => {
                                metadata.columns.insert(key, Value::from(val));
                            }
                            _ => {
                                return QueryResponse::err(format!(
                                    "Metadata error, invalid value type for key: {}",
                                    key
                                ));
                            }
                        }
                    }

                    if let Some(id) = fb_id {
                        metadata.columns.insert("Fb_id".into(), id);
                    }
                    if let Some(created) = fb_created {
                        metadata.columns.insert("Fb_created".into(), created);
                    }
                }
                Ok(_) => {
                    return QueryResponse::err("Metadata can only be key=value JSON.");
                }
                Err(_) => {
                    return QueryResponse::err("Error decoding metadata.");
                }
            }
        }
        query_str = rest;
    }

    // Strip comments
    query_str = command::strip_comments(&query_str);

    if query_str.is_empty() {
        return QueryResponse {
            success: true,
            err: "empty query".into(),
            ..QueryResponse::new()
        };
    }

    if query_str.starts_with('#') || query_str.starts_with('/') {
        return QueryResponse {
            success: true,
            err: "commented out".into(),
            ..QueryResponse::new()
        };
    }

    // Parse and dispatch
    let cmd = command::parse_command(&query_str);

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!("QUERY: {:?}", cmd));
    }

    match cmd {
        Command::Select(q) => handle_select(&q, server, metadata, conn),
        Command::InsertInto(q) => handle_insert(&q, server, metadata, conn),
        Command::RPush { table, data } => handle_rpush(&table, &data, metadata, conn),
        Command::RPop { table } => handle_rpop(&table, server, metadata, conn),

        Command::CreateTable { name, options } => crate::query::create::create_table_cmd(&name, &options),
        Command::CreateIndex { name, options } => crate::query::create::create_index_cmd(&name, &options),
        Command::CreateServer { protocol, name, options } => {
            crate::query::create::create_server_cmd(protocol, &name, &options)
        }
        Command::CreateProc { proc_type, name, options } => {
            crate::query::create::create_proc_cmd(proc_type, &name, &options)
        }

        Command::AlterTable { name, options } => crate::query::alter::alter_table_cmd(&name, &options),
        Command::AlterServer { name, options } => crate::query::alter::alter_server_cmd(&name, &options),
        Command::AlterProc { name, options } => crate::query::alter::alter_proc_cmd(&name, &options),

        Command::DropTable { name } => crate::query::drop::drop_table_cmd(&name),
        Command::DropIndex { name } => crate::query::drop::drop_index_cmd(&name),
        Command::DropServer { name } => crate::query::drop::drop_server_cmd(&name),
        Command::DropProc { name } => crate::query::drop::drop_proc_cmd(&name),
        Command::DropWorker { name } => crate::query::drop::drop_worker_cmd(&name),
        Command::DropConnection { addr } => crate::query::drop::drop_connection_cmd(&addr),

        Command::StartWorker { class, name, options } => {
            crate::query::create::start_worker_cmd(class, &name, &options)
        }

        Command::Show { what, filter } => crate::query::show::show_cmd(&what, filter.as_ref()),
        Command::Set { key, value } => crate::query::set::set_cmd(&key, &value),

        Command::Ping => QueryResponse::ok_string("PONG".to_string()),
        Command::Quit | Command::Exit => {
            // Signal connection to close
            QueryResponse {
                success: true,
                query_type: QueryType::String,
                value_str: "OK".into(),
                ..QueryResponse::new()
            }
        }
        Command::Shutdown => {
            // Signal shutdown
            QueryResponse::ok_string("OK".to_string())
        }
        Command::Version => {
            QueryResponse::ok_string(format!("{}-{}", vars::APP_NAME, vars::version()))
        }

        Command::Sleep { ms } => {
            std::thread::sleep(std::time::Duration::from_millis(ms));
            QueryResponse::ok_string(String::new())
        }
        Command::NSleep { ns } => {
            std::thread::sleep(std::time::Duration::from_nanos(ns));
            QueryResponse::ok_string(String::new())
        }

        Command::LLen { table } => handle_llen(&table),
        Command::LLenM { table } => handle_llenm(&table),
        Command::TableState { table } => handle_tablestate(&table),

        Command::SelectLastInsertId => handle_last_insert_id(server),
        Command::SelectDatabase | Command::SelectSchema | Command::SelectVersion => {
            QueryResponse::ok_bool()
        }

        Command::Use | Command::Lock | Command::Unlock | Command::SetGeneric => {
            QueryResponse::ok_bool()
        }

        Command::Empty | Command::Comment => QueryResponse {
            success: true,
            err: "empty query".into(),
            ..QueryResponse::new()
        },

        Command::Metadata(_) => QueryResponse::ok_bool(),
        Command::Unknown(q) => QueryResponse::err(format!("Unknown command: {}", q)),
    }
}

fn handle_select(
    query: &str,
    server: Option<&Arc<Server>>,
    metadata: &OwnedRow,
    conn: Option<&Connection>,
) -> QueryResponse {
    let (force_limit, force_offset, max_scan_time_ms) = if let Some(s) = server {
        (s.force_limit, s.force_offset, s.max_scan_time_ms)
    } else {
        (-1, -1, 0)
    };

    match crate::query::select::sql_select(query, force_limit, force_offset, max_scan_time_ms, metadata, conn) {
        Ok(rows) => QueryResponse::ok_result(rows),
        Err(e) => QueryResponse::err_typed(QueryType::Result, e),
    }
}

fn handle_insert(
    query: &str,
    server: Option<&Arc<Server>>,
    metadata: &OwnedRow,
    conn: Option<&Connection>,
) -> QueryResponse {
    match crate::query::insert::sql_insert(query, metadata, conn) {
        Ok((insert_id, select_result)) => {
            if let Some(s) = server {
                *s.last_insert_id.lock() = insert_id;
            }
            if let Some(result) = select_result {
                if !result.is_empty() {
                    return QueryResponse {
                        success: true,
                        query_type: QueryType::Result,
                        result,
                        affected_rows: 1,
                        ..QueryResponse::new()
                    };
                }
            }
            QueryResponse {
                success: true,
                query_type: QueryType::Bool,
                affected_rows: 1,
                ..QueryResponse::new()
            }
        }
        Err(e) => QueryResponse::err_typed(QueryType::Bool, e),
    }
}

fn handle_rpush(
    table: &str,
    data: &str,
    metadata: &OwnedRow,
    conn: Option<&Connection>,
) -> QueryResponse {
    match crate::query::insert::insert_json_string(table, data, metadata, conn) {
        Ok((_insert_id, select_result)) => {
            if let Some(result) = select_result {
                if !result.is_empty() {
                    return QueryResponse {
                        success: true,
                        query_type: QueryType::Result,
                        result,
                        value_int: 1,
                        affected_rows: 1,
                        ..QueryResponse::new()
                    };
                }
            }
            QueryResponse {
                success: true,
                query_type: QueryType::Number,
                value_int: 1,
                affected_rows: 1,
                ..QueryResponse::new()
            }
        }
        Err(e) => QueryResponse::err_typed(QueryType::Number, e),
    }
}

fn handle_rpop(
    table: &str,
    server: Option<&Arc<Server>>,
    metadata: &OwnedRow,
    conn: Option<&Connection>,
) -> QueryResponse {
    let q = format!("SELECT * FROM `{}` LIMIT 1", table);
    handle_select(&q, server, metadata, conn)
}

fn handle_llen(table_name: &str) -> QueryResponse {
    match registry::get_table(table_name) {
        Some(t) => {
            let len = t.len();
            QueryResponse::ok_string(len.to_string())
        }
        None => QueryResponse::err_typed(
            QueryType::String,
            format!("Table doesn't exist: {}", table_name),
        ),
    }
}

fn handle_llenm(table_name: &str) -> QueryResponse {
    match registry::get_table(table_name) {
        Some(t) => {
            let len = t.len();
            let mut row = OwnedRow::new();
            row.columns
                .insert("table_name".into(), Value::String(table_name.into()));
            row.columns
                .insert("table_length".into(), Value::Number(len as f64));
            QueryResponse::ok_result(vec![row])
        }
        None => QueryResponse::err_typed(
            QueryType::Result,
            format!("Table doesn't exist: {}", table_name),
        ),
    }
}

fn handle_tablestate(table_name: &str) -> QueryResponse {
    match registry::get_table(table_name) {
        Some(t) => {
            let mut row = OwnedRow::new();
            row.columns
                .insert("table_name".into(), Value::String(table_name.into()));

            let len = t.len();
            let capacity = t.capacity_str();
            row.columns
                .insert("row_count".into(), Value::Number(len as f64));
            row.columns
                .insert("capacity".into(), Value::String(capacity));
            row.columns
                .insert("indexes".into(), Value::Number(t.indexes.len() as f64));

            QueryResponse::ok_result(vec![row])
        }
        None => QueryResponse::err_typed(
            QueryType::Result,
            format!("Table doesn't exist: {}", table_name),
        ),
    }
}

fn handle_last_insert_id(server: Option<&Arc<Server>>) -> QueryResponse {
    let mut row = OwnedRow::new();
    let id = server
        .map(|s| s.last_insert_id.lock().clone())
        .unwrap_or_default();
    row.columns
        .insert("LAST_INSERT_ID()".into(), Value::String(id));
    QueryResponse::ok_result(vec![row])
}
