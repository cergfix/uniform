use crate::store::registry::{PROCS, SERVERS, TABLES};
use crate::store::worker;
use crate::types::query_response::QueryResponse;
use crate::util::logging;

pub fn drop_table_cmd(name: &str) -> QueryResponse {
    if TABLES.remove(name).is_some() {
        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log(&format!("DROP TABLE: dropped table {}", name));
        }
        QueryResponse::ok_bool()
    } else {
        QueryResponse::err("An unknown error occured while trying to drop table")
    }
}

pub fn drop_index_cmd(name: &str) -> QueryResponse {
    // Find which table has this index and remove it
    let mut found = false;
    for entry in TABLES.iter() {
        if entry.value().indexes.contains_key(name) {
            // We need to remove the index from the table
            // Since table is Arc, we need to do the same trick as CREATE INDEX
            let table_name = entry.key().clone();
            drop(entry);

            if let Some((_, table_arc)) = TABLES.remove(&table_name) {
                match std::sync::Arc::try_unwrap(table_arc) {
                    Ok(mut table) => {
                        table.indexes.remove(name);
                        TABLES.insert(table_name, std::sync::Arc::new(table));
                        found = true;
                    }
                    Err(arc) => {
                        // Put it back
                        TABLES.insert(table_name, arc);
                        return QueryResponse::err("Cannot modify table with active references");
                    }
                }
            }
            break;
        }
    }

    if found {
        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log(&format!("DROP INDEX: dropped index {}", name));
        }
        QueryResponse::ok_bool()
    } else {
        QueryResponse::err("No such index.")
    }
}

pub fn drop_server_cmd(name: &str) -> QueryResponse {
    if let Some((_, server)) = SERVERS.remove(name) {
        server.set_shutting_down();
        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log(&format!("DROP SERVER: dropped server {}", name));
        }
        QueryResponse::ok_bool()
    } else {
        QueryResponse::err("An unknown error occured while trying to drop server")
    }
}

pub fn drop_proc_cmd(name: &str) -> QueryResponse {
    let mut procs = PROCS.write();
    let initial_len = procs.len();
    procs.retain(|p| p.name != name);
    if procs.len() < initial_len {
        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log(&format!("DROP PROC: dropped proc {}", name));
        }
        QueryResponse::ok_bool()
    } else {
        QueryResponse::err("An unknown error occured while trying to drop a proc")
    }
}

pub fn drop_worker_cmd(name: &str) -> QueryResponse {
    if worker::drop_worker(name) {
        QueryResponse {
            success: true,
            affected_rows: 1,
            ..QueryResponse::ok_bool()
        }
    } else {
        QueryResponse::err("No such worker.")
    }
}

pub fn drop_connection_cmd(addr: &str) -> QueryResponse {
    for server_entry in SERVERS.iter() {
        if let Some((_, conn)) = server_entry.value().connections.remove(addr) {
            if let Ok(mut c) = conn.try_lock() {
                c.terminated = true;
            }
            if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
                logging::log(&format!("DROP CONNECTION: dropped connection {}", addr));
            }
            return QueryResponse::ok_bool();
        }
    }
    QueryResponse::err("No such connection.")
}
