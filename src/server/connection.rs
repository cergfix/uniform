use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::store::registry::Server;
use crate::types::row::OwnedRow;

/// Per-client connection state.
pub struct Connection {
    pub stream: Option<Arc<Mutex<TcpStream>>>,
    pub metadata: OwnedRow,
    pub query: String,
    pub last_activity: DateTime<Utc>,
    pub created: DateTime<Utc>,
    pub state_executing: bool,
    pub state_text: String,
    pub terminated: bool,
    pub remote_addr: String,
    pub local_addr: String,
    pub server_name: String,
}

impl Connection {
    pub fn new(stream: TcpStream, server: &Server) -> Self {
        let remote_addr = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        let local_addr = stream
            .local_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());

        Connection {
            stream: Some(Arc::new(Mutex::new(stream))),
            metadata: OwnedRow::new(),
            query: String::new(),
            last_activity: Utc::now(),
            created: Utc::now(),
            state_executing: false,
            state_text: "NEW".to_string(),
            terminated: false,
            remote_addr,
            local_addr,
            server_name: server.name.clone(),
        }
    }

    /// Create a local (non-network) connection for stdin/internal queries.
    pub fn new_local() -> Self {
        Connection {
            stream: None,
            metadata: OwnedRow::new(),
            query: String::new(),
            last_activity: Utc::now(),
            created: Utc::now(),
            state_executing: false,
            state_text: "LOCAL".to_string(),
            terminated: false,
            remote_addr: format!("local:{}", uuid::Uuid::new_v4()),
            local_addr: format!("local:{}", uuid::Uuid::new_v4()),
            server_name: String::new(),
        }
    }

    /// Get the stream (panics if local connection).
    pub fn get_stream(&self) -> &Arc<Mutex<TcpStream>> {
        self.stream.as_ref().expect("No stream on local connection")
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("remote_addr", &self.remote_addr)
            .field("local_addr", &self.local_addr)
            .field("state_text", &self.state_text)
            .field("terminated", &self.terminated)
            .finish()
    }
}
