use std::sync::atomic::{AtomicI32, Ordering};

use chrono::{DateTime, Utc};
use tokio::sync::oneshot;

use crate::types::row::OwnedRow;
use crate::types::value::Value;

/// A long-poll client — a connection waiting for rows to be inserted.
pub struct LongPollClient {
    /// Channel to send the reply row(s) back to the waiting connection.
    pub reply_tx: Option<oneshot::Sender<Vec<OwnedRow>>>,
    /// The WHERE conditions this client is waiting for (as a string for now).
    pub condition_str: String,
    /// When this long-poll request expires.
    pub expiration: DateTime<Utc>,
    /// CAS flag: 0 = waiting, 1 = fulfilled. Prevents double-delivery.
    pub used: AtomicI32,
    /// Optional reply index value.
    pub reply_index: Option<Value>,
}

impl LongPollClient {
    pub fn new(
        reply_tx: oneshot::Sender<Vec<OwnedRow>>,
        condition_str: String,
        expiration: DateTime<Utc>,
    ) -> Self {
        LongPollClient {
            reply_tx: Some(reply_tx),
            condition_str,
            expiration,
            used: AtomicI32::new(0),
            reply_index: None,
        }
    }

    /// Try to claim this client for delivery. Returns true if we won.
    pub fn try_claim(&self) -> bool {
        self.used
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// Check if this client is still waiting (not claimed, not expired).
    pub fn is_active(&self) -> bool {
        self.used.load(Ordering::Acquire) == 0 && Utc::now() < self.expiration
    }

    /// Check if expired.
    pub fn is_expired(&self) -> bool {
        Utc::now() >= self.expiration
    }

    /// Check if this client is available for delivery (not claimed, not expired).
    pub fn is_available(&self) -> bool {
        self.used.load(Ordering::Acquire) == 0
    }

    /// Try to deliver a row to this client. Returns true if successful.
    pub fn try_deliver(&mut self, row: OwnedRow) -> bool {
        if self
            .used
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            if let Some(tx) = self.reply_tx.take() {
                let _ = tx.send(vec![row]);
                return true;
            }
        }
        false
    }
}

impl std::fmt::Debug for LongPollClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LongPollClient")
            .field("condition_str", &self.condition_str)
            .field("expiration", &self.expiration)
            .field("used", &self.used.load(Ordering::Relaxed))
            .finish()
    }
}
