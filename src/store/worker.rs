use crate::types::row::OwnedRow;
use crate::util::logging;

/// Worker class — the 12 worker types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerClass {
    ExecQuery,
    BufferAppl,
    PushToCloudStorage,
    PushToFile,
    PushToFastCgi,
    MySqlDataTransfer,
    PushToStdout,
    PushToStderr,
    PushToSlack,
    PushToEmail,
    PushToElasticsearch,
    PushToPagerDuty,
}

impl WorkerClass {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "EXEC_QUERY" => Some(WorkerClass::ExecQuery),
            "BUFFER_APPL" => Some(WorkerClass::BufferAppl),
            "PUSH_TO_CLOUD_STORAGE" => Some(WorkerClass::PushToCloudStorage),
            "PUSH_TO_FILE" => Some(WorkerClass::PushToFile),
            "PUSH_TO_FASTCGI" => Some(WorkerClass::PushToFastCgi),
            "MYSQL_DATA_TRANSFER" => Some(WorkerClass::MySqlDataTransfer),
            "PUSH_TO_STDOUT" => Some(WorkerClass::PushToStdout),
            "PUSH_TO_STDERR" => Some(WorkerClass::PushToStderr),
            "PUSH_TO_SLACK" => Some(WorkerClass::PushToSlack),
            "PUSH_TO_EMAIL" => Some(WorkerClass::PushToEmail),
            "PUSH_TO_ELASTICSEARCH" => Some(WorkerClass::PushToElasticsearch),
            "PUSH_TO_PAGERDUTY" => Some(WorkerClass::PushToPagerDuty),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            WorkerClass::ExecQuery => "EXEC_QUERY",
            WorkerClass::BufferAppl => "BUFFER_APPL",
            WorkerClass::PushToCloudStorage => "PUSH_TO_CLOUD_STORAGE",
            WorkerClass::PushToFile => "PUSH_TO_FILE",
            WorkerClass::PushToFastCgi => "PUSH_TO_FASTCGI",
            WorkerClass::MySqlDataTransfer => "MYSQL_DATA_TRANSFER",
            WorkerClass::PushToStdout => "PUSH_TO_STDOUT",
            WorkerClass::PushToStderr => "PUSH_TO_STDERR",
            WorkerClass::PushToSlack => "PUSH_TO_SLACK",
            WorkerClass::PushToEmail => "PUSH_TO_EMAIL",
            WorkerClass::PushToElasticsearch => "PUSH_TO_ELASTICSEARCH",
            WorkerClass::PushToPagerDuty => "PUSH_TO_PAGERDUTY",
        }
    }
}

/// Worker — a background task that pops rows and pushes them somewhere.
#[derive(Debug)]
pub struct Worker {
    pub name: String,
    pub class: WorkerClass,
    pub data: OwnedRow,
    pub terminator: tokio::sync::watch::Sender<bool>,
    pub handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Worker {
    pub fn new(
        name: String,
        class: WorkerClass,
        data: OwnedRow,
        terminator: tokio::sync::watch::Sender<bool>,
    ) -> Self {
        Worker {
            name,
            class,
            data,
            terminator,
            handle: parking_lot::Mutex::new(None),
        }
    }

    /// Signal the worker to stop.
    pub fn terminate(&self) {
        let _ = self.terminator.send(true);
    }
}

/// Drop a worker by name — signals termination and removes from registry.
pub fn drop_worker(name: &str) -> bool {
    use crate::store::registry::WORKERS;

    if let Some((_, worker)) = WORKERS.remove(name) {
        logging::log(&format!("DROP WORKER: dropping worker {}", name));
        worker.terminate();
        true
    } else {
        false
    }
}

/// Shutdown all workers (called during graceful shutdown).
pub fn shutdown_workers() {
    use crate::store::registry::WORKERS;

    // First pass: terminate non-stdout/stderr workers
    let mut names: Vec<String> = Vec::new();
    for entry in WORKERS.iter() {
        let w = entry.value();
        if w.class != WorkerClass::PushToStdout && w.class != WorkerClass::PushToStderr {
            names.push(entry.key().clone());
        }
    }
    for name in &names {
        drop_worker(name);
    }

    // Second pass: stdout/stderr workers
    let mut remaining: Vec<String> = Vec::new();
    for entry in WORKERS.iter() {
        remaining.push(entry.key().clone());
    }
    for name in &remaining {
        drop_worker(name);
    }
}
