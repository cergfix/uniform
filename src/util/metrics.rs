use std::sync::Arc;
use std::time::Duration;

use crate::store::registry::Server;
use crate::util::logging;

/// Publish a server request latency probe.
pub fn publish_server_latency_probe(server: &Arc<Server>, value: Duration, request_id: &str) {
    let value_ms = value.as_secs_f64() * 1000.0;

    if server.latency_target_ms > -1.0 && value_ms >= server.latency_target_ms {
        if logging::get_log_level() >= logging::LOG_LEVEL_CRIT {
            logging::log(&format!(
                "WARNING! Server request latency (server={}, fb_id={}) is higher than selected target threshold ({} ms): {:.3} ms",
                server.name, request_id, server.latency_target_ms, value_ms
            ));
        }
    }

    if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
        logging::log(&format!(
            "Server request latency (server={}, fb_id={}): {:.3} ms",
            server.name, request_id, value_ms
        ));
    }
}

/// Publish a worker latency probe.
pub fn publish_worker_latency_probe(
    worker_name: &str,
    value: Duration,
    stage: i32,
    latency_target_ms: Option<f64>,
) {
    let value_ms = value.as_secs_f64() * 1000.0;

    if stage == 1 {
        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log(&format!(
                "Worker wait (long poll) time (worker={}): {:.3} ms",
                worker_name, value_ms
            ));
        }
    }

    if stage == 2 {
        if logging::get_log_level() >= logging::LOG_LEVEL_DEBUG {
            logging::log(&format!(
                "Worker processing (work) time (worker={}): {:.3} ms",
                worker_name, value_ms
            ));
        }

        if let Some(target) = latency_target_ms {
            if target > -1.0 && value_ms >= target {
                if logging::get_log_level() >= logging::LOG_LEVEL_CRIT {
                    logging::log(&format!(
                        "WARNING! Worker processing (work) time (worker={}) is higher than selected target threshold ({} ms): {:.3} ms",
                        worker_name, target, value_ms
                    ));
                }
            }
        }
    }
}
