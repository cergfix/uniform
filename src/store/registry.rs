use std::sync::Arc;

use dashmap::DashMap;
use once_cell::sync::Lazy;
use parking_lot::RwLock;

use crate::server::connection::Connection;
use crate::store::proc::Proc;
use crate::store::table::Table;
use crate::store::worker::Worker;

/// Server protocol types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerProtocol {
    Redis = 0,
    MySQL = 1,
    Http = 2,
    FastCgi = 3,
}

impl ServerProtocol {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "REDIS" => Some(ServerProtocol::Redis),
            "MYSQL" => Some(ServerProtocol::MySQL),
            "HTTP" => Some(ServerProtocol::Http),
            "FASTCGI" => Some(ServerProtocol::FastCgi),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ServerProtocol::Redis => "redis",
            ServerProtocol::MySQL => "mysql",
            ServerProtocol::Http => "http",
            ServerProtocol::FastCgi => "fastcgi",
        }
    }
}

/// Server configuration.
#[derive(Debug)]
pub struct Server {
    pub name: String,
    pub bind: String,
    pub protocol: ServerProtocol,
    pub timeout_ms: i32,
    pub buffer_size: usize,
    pub shutting_down: std::sync::atomic::AtomicBool,

    pub force_limit: i32,
    pub force_offset: i32,
    pub max_scan_time_ms: i32,
    pub force_select_sleep_ms: i32,

    pub table: String,
    pub script_filename: String,
    pub document_root: String,
    pub https_redirect: bool,
    pub https_redirect_header: String,
    pub https_redirect_on: String,
    pub robots: String,
    pub gzip: bool,

    pub latency_target_ms: f64,
    pub last_insert_id: parking_lot::Mutex<String>,

    pub connection_count: std::sync::atomic::AtomicI64,

    /// Active connections keyed by remote address.
    pub connections: DashMap<String, Arc<tokio::sync::Mutex<Connection>>>,

    /// Handle to the TCP listener task (for shutdown).
    pub listener_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Server {
    pub fn new(
        name: String,
        bind: String,
        protocol: ServerProtocol,
        timeout_ms: i32,
        buffer_size: usize,
    ) -> Self {
        Server {
            name,
            bind,
            protocol,
            timeout_ms,
            buffer_size,
            shutting_down: std::sync::atomic::AtomicBool::new(false),
            force_limit: -1,
            force_offset: -1,
            max_scan_time_ms: 0,
            force_select_sleep_ms: 0,
            table: String::new(),
            script_filename: String::new(),
            document_root: String::new(),
            https_redirect: false,
            https_redirect_header: String::new(),
            https_redirect_on: String::new(),
            robots: String::new(),
            gzip: false,
            latency_target_ms: -1.0,
            last_insert_id: parking_lot::Mutex::new(String::new()),
            connection_count: std::sync::atomic::AtomicI64::new(0),
            connections: DashMap::new(),
            listener_handle: parking_lot::Mutex::new(None),
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn set_shutting_down(&self) {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

// ---------- Global registries ----------

/// Global table registry: table name → Arc<Table>
pub static TABLES: Lazy<DashMap<String, Arc<Table>>> = Lazy::new(DashMap::new);

/// Global server registry: server name → Arc<Server>
pub static SERVERS: Lazy<DashMap<String, Arc<Server>>> = Lazy::new(DashMap::new);

/// Global worker registry: worker name → Arc<Worker>
pub static WORKERS: Lazy<DashMap<String, Arc<Worker>>> = Lazy::new(DashMap::new);

/// Global proc list (protected by RwLock for concurrent reads, exclusive writes).
pub static PROCS: Lazy<RwLock<Vec<Proc>>> = Lazy::new(|| RwLock::new(Vec::new()));

/// Global shutdown sender — set once in main(), used by Redis SHUTDOWN command.
pub static SHUTDOWN_TX: Lazy<parking_lot::Mutex<Option<tokio::sync::watch::Sender<bool>>>> =
    Lazy::new(|| parking_lot::Mutex::new(None));

// ---------- Lookup helpers ----------

pub fn get_table(name: &str) -> Option<Arc<Table>> {
    TABLES.get(name).map(|r| r.value().clone())
}

pub fn get_server(name: &str) -> Option<Arc<Server>> {
    SERVERS.get(name).map(|r| r.value().clone())
}

pub fn get_worker(name: &str) -> Option<Arc<Worker>> {
    WORKERS.get(name).map(|r| r.value().clone())
}

pub fn get_proc_by_name(name: &str) -> Option<Proc> {
    let procs = PROCS.read();
    procs.iter().find(|p| p.name == name).cloned()
}
