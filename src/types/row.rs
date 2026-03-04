use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::types::value::Value;

/// Row stored in the SkipMap — contains an atomic `claimed` flag for lock-free pop.
pub struct Row {
    pub seq_id: u64,
    pub columns: HashMap<String, Value>,
    pub created: DateTime<Utc>,
    /// CAS flag: `false` = available, `true` = claimed by a SELECT (pop).
    /// Only one query can flip `false→true` via CAS — that query owns the row.
    pub claimed: AtomicBool,
}

impl Row {
    /// Create a new row with auto-generated u_id and u_created_at.
    pub fn new(seq_id: u64, mut columns: HashMap<String, Value>) -> Self {
        let now = Utc::now();
        let row_id = Uuid::new_v4().to_string();
        let row_created = now.format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string();

        columns
            .entry("u_id".to_string())
            .or_insert_with(|| Value::String(row_id));
        columns
            .entry("u_created_at".to_string())
            .or_insert_with(|| Value::String(row_created));

        Row {
            seq_id,
            columns,
            created: now,
            claimed: AtomicBool::new(false),
        }
    }

    /// Create a row from pre-built columns (no auto-generated u_id / u_created_at).
    pub fn from_raw(seq_id: u64, columns: HashMap<String, Value>, created: DateTime<Utc>) -> Self {
        Row {
            seq_id,
            columns,
            created,
            claimed: AtomicBool::new(false),
        }
    }

    /// Check if this row has been claimed (popped).
    pub fn is_claimed(&self) -> bool {
        self.claimed.load(Ordering::Acquire)
    }

    /// Try to claim this row atomically. Returns true if we won the CAS.
    pub fn try_claim(&self) -> bool {
        self.claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    /// Convert to an owned row (for returning from SELECT).
    pub fn to_owned_row(&self) -> OwnedRow {
        OwnedRow {
            seq_id: self.seq_id,
            columns: self.columns.clone(),
            created: self.created,
        }
    }
}

impl std::fmt::Debug for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Row")
            .field("seq_id", &self.seq_id)
            .field("columns", &self.columns)
            .field("created", &self.created)
            .field("claimed", &self.claimed.load(Ordering::Relaxed))
            .finish()
    }
}

/// Owned row — no atomics, safe to send across tasks and serialize.
/// This is what SELECT returns to the caller.
#[derive(Debug, Clone)]
pub struct OwnedRow {
    pub seq_id: u64,
    pub columns: HashMap<String, Value>,
    pub created: DateTime<Utc>,
}

impl OwnedRow {
    /// Create a new owned row with auto-generated u_id and u_created_at.
    pub fn new() -> Self {
        let now = Utc::now();
        let mut columns = HashMap::new();
        columns.insert(
            "u_id".to_string(),
            Value::String(Uuid::new_v4().to_string()),
        );
        columns.insert(
            "u_created_at".to_string(),
            Value::String(now.format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string()),
        );
        OwnedRow {
            seq_id: 0,
            columns,
            created: now,
        }
    }

    /// Deep-copy this row.
    pub fn copy_row(&self) -> OwnedRow {
        OwnedRow {
            seq_id: self.seq_id,
            columns: self.columns.clone(),
            created: self.created,
        }
    }

    /// Filter columns to only keep the specified keys.
    pub fn filter_keys(&mut self, keys: &[String]) {
        if keys.is_empty() {
            return;
        }
        self.columns.retain(|k, _| keys.iter().any(|key| key == k));
    }

    /// Apply a patch (map of key→value) to this row's columns.
    pub fn patch(&mut self, patch: &HashMap<String, Value>) {
        for (k, v) in patch {
            self.columns.insert(k.clone(), v.clone());
        }
    }

    /// Convert columns to a JSON object (non-HTML-escaped, matching Go's behavior).
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        let json_map: serde_json::Map<String, serde_json::Value> = self
            .columns
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::Value::from(v.clone())))
            .collect();
        serde_json::to_string(&json_map)
    }

    /// Get a column value by name.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.columns.get(key)
    }

    /// Get a column value as string.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.columns.get(key).and_then(|v| v.as_str())
    }

    /// Get a column value as f64.
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.columns.get(key).and_then(|v| v.to_f64())
    }
}

impl Default for OwnedRow {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a new u_id (UUID v4).
pub fn generate_u_id() -> String {
    Uuid::new_v4().to_string()
}
