use crate::config::vars;
use crate::types::query_response::QueryResponse;
use crate::util::{cluster, logging};

/// Execute a SET command.
pub fn set_cmd(key: &str, value: &str) -> QueryResponse {
    match key.to_uppercase().as_str() {
        "WORKERS_FORCE_SHUTDOWN" => match value.to_uppercase().as_str() {
            "ON" => {
                vars::set_workers_force_shutdown(true);
                QueryResponse::ok_bool()
            }
            "OFF" => {
                vars::set_workers_force_shutdown(false);
                QueryResponse::ok_bool()
            }
            _ => QueryResponse::err("Unknown value for variable. Should be either ON or OFF."),
        },
        "PERSIST_METADATA" => match value.to_uppercase().as_str() {
            "ON" => {
                vars::set_persist_metadata(true);
                QueryResponse::ok_bool()
            }
            "OFF" => {
                vars::set_persist_metadata(false);
                QueryResponse::ok_bool()
            }
            _ => QueryResponse::err("Unknown value for variable. Should be either ON or OFF."),
        },
        "ROLE" => {
            if cluster::set_cluster_role(value) {
                QueryResponse::ok_bool()
            } else {
                QueryResponse::err(format!("Couldn't set cluster role to: \"{}\"", value))
            }
        }
        "LOG_DEST" => {
            let result = logging::set_log_dest(value);
            if result == value {
                QueryResponse::ok_bool()
            } else {
                QueryResponse::err(format!("Couldn't set log destination to: \"{}\"", value))
            }
        }
        "LOG_LEVEL" => match value.to_uppercase().as_str() {
            "DEBUG" => {
                if logging::set_log_level(logging::LOG_LEVEL_DEBUG) == logging::LOG_LEVEL_DEBUG {
                    QueryResponse::ok_bool()
                } else {
                    QueryResponse::err("Couldn't set log level to DEBUG.")
                }
            }
            "ERROR" => {
                if logging::set_log_level(logging::LOG_LEVEL_ERROR) == logging::LOG_LEVEL_ERROR {
                    QueryResponse::ok_bool()
                } else {
                    QueryResponse::err("Couldn't set log level to ERROR.")
                }
            }
            "CRIT" => {
                if logging::set_log_level(logging::LOG_LEVEL_CRIT) == logging::LOG_LEVEL_CRIT {
                    QueryResponse::ok_bool()
                } else {
                    QueryResponse::err("Couldn't set log level to CRIT.")
                }
            }
            _ => QueryResponse::err(
                "Unknown value for variable. Should be one of: DEBUG, ERROR, CRIT.",
            ),
        },
        "SELECT_CACHE" => match value.to_uppercase().as_str() {
            "ON" => {
                vars::set_select_cache_enabled(true);
                QueryResponse::ok_bool()
            }
            "OFF" => {
                vars::set_select_cache_enabled(false);
                QueryResponse::ok_bool()
            }
            _ => QueryResponse::err("Unknown value for variable. Should be either ON or OFF."),
        },
        _ => QueryResponse::err("Unknown variable."),
    }
}
