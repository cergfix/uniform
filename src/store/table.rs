use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

use chrono::{DateTime, Utc};

use crate::store::index::{SecondaryIndex, SharedIndex};
use crate::store::long_poll::LongPollClient;
use crate::types::row::{OwnedRow, Row};
use crate::types::value::Value;

/// Queue ordering mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueOrder {
    Fifo,
    Lifo,
    Priority { key: String },
}

/// Table — the core data structure. An in-memory queue with optional secondary indexes.
///
/// Primary storage: `crossbeam_skiplist::SkipMap<u64, Row>` (lock-free, ordered by seq_id).
/// Secondary indexes: each is a lock-free SkipMap with composite key `(Value, u64)`.
/// Per-row `AtomicBool` CAS for exactly-once claiming (SELECT pop).
pub struct Table {
    pub name: String,
    pub order: QueueOrder,
    pub max_size: Option<usize>,

    // Sequence counter (lock-free)
    pub next_seq: AtomicU64,

    // Primary storage: lock-free ordered skip list (sorted by seq_id = insertion order)
    pub rows: SkipMap<u64, Row>,

    // Secondary indexes: static after CREATE (name → index)
    pub indexes: HashMap<String, SharedIndex>,

    // Row count (atomic, for capacity checks and SHOW TABLES)
    pub row_count: AtomicI64,

    // Long-poll
    pub long_poll_clients: parking_lot::Mutex<Vec<LongPollClient>>,
    pub long_poll_max_clients: usize,

    // Buffer (optional, for delayed/time-dilated writes)
    pub buffer_tx: Option<tokio::sync::mpsc::Sender<OwnedRow>>,
    pub buffer_rx: Option<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<OwnedRow>>>,
    pub buffer_max_size: usize,
    pub buffer_long_poll_ms: i32,

    // Query constraints
    pub force_limit: i32,
    pub force_offset: i32,
    pub max_scan_time_ms: i32,
    pub silent: bool,
}

impl Table {
    pub fn new(name: String, max_size: Option<usize>) -> Self {
        Table {
            name,
            order: QueueOrder::Fifo,
            max_size,
            next_seq: AtomicU64::new(0),
            rows: SkipMap::new(),
            indexes: HashMap::new(),
            row_count: AtomicI64::new(0),
            long_poll_clients: parking_lot::Mutex::new(Vec::new()),
            long_poll_max_clients: 0,
            buffer_tx: None,
            buffer_rx: None,
            buffer_max_size: 0,
            buffer_long_poll_ms: 0,
            force_limit: -1,
            force_offset: -1,
            max_scan_time_ms: 0,
            silent: false,
        }
    }

    /// Insert a row into the table. Returns the seq_id on success.
    pub fn insert(&self, columns: HashMap<String, Value>) -> Result<u64, String> {
        let seq_id = self.prepare_insert(&columns)?;
        let row = Row::new(seq_id, columns);
        self.rows.insert(seq_id, row);
        self.row_count.fetch_add(1, Ordering::Relaxed);
        Ok(seq_id)
    }

    /// Insert with pre-built fields (skips Row::new UUID/timestamp generation).
    pub fn insert_raw(
        &self,
        columns: HashMap<String, Value>,
        created: DateTime<Utc>,
    ) -> Result<u64, String> {
        let seq_id = self.prepare_insert(&columns)?;
        let row = Row::from_raw(seq_id, columns, created);
        self.rows.insert(seq_id, row);
        self.row_count.fetch_add(1, Ordering::Relaxed);
        Ok(seq_id)
    }

    /// Shared logic: capacity check, seq_id allocation, index constraints + updates.
    fn prepare_insert(&self, columns: &HashMap<String, Value>) -> Result<u64, String> {
        if let Some(max) = self.max_size {
            if self.row_count.load(Ordering::Relaxed) as usize >= max {
                return Err("Table full".into());
            }
        }

        let seq_id = self.next_seq.fetch_add(1, Ordering::Relaxed);

        // Check unique index constraints (skip claimed rows — they're being removed)
        for idx in self.indexes.values() {
            if idx.unique {
                if let Some(val) = columns.get(&idx.column) {
                    let has_live = idx.has_live_entry(val, |sid| {
                        self.rows.get(&sid).is_some_and(|r| !r.value().is_claimed())
                    });
                    if has_live {
                        return Err(format!("Duplicate key on index {}", idx.name));
                    }
                }
            }
        }

        // Update all secondary indexes (lock-free SkipMap insert)
        for idx in self.indexes.values() {
            if let Some(val) = columns.get(&idx.column) {
                idx.insert(val.clone(), seq_id);
            }
        }

        Ok(seq_id)
    }

    /// Pop a single row from the front (FIFO) or back (LIFO).
    /// Uses per-row CAS — no table-level lock.
    pub fn pop_one(&self) -> Option<OwnedRow> {
        match self.order {
            QueueOrder::Fifo | QueueOrder::Priority { .. } => self.pop_front(),
            QueueOrder::Lifo => self.pop_back(),
        }
    }

    /// Pop from front — O(1) amortized for FIFO.
    fn pop_front(&self) -> Option<OwnedRow> {
        while let Some(entry) = self.rows.front() {
            let row = entry.value();
            if row.is_claimed() {
                // Stale claimed row at front — remove and continue
                let seq_id = *entry.key();
                drop(entry);
                self.rows.remove(&seq_id);
                continue;
            }
            if row.try_claim() {
                let owned = row.to_owned_row();
                self.row_count.fetch_sub(1, Ordering::Relaxed);
                self.remove_row(entry.key(), row);
                return Some(owned);
            }
            // CAS lost to another thread — retry from front
        }
        None
    }

    /// Pop from back — O(1) amortized for LIFO.
    fn pop_back(&self) -> Option<OwnedRow> {
        while let Some(entry) = self.rows.back() {
            let row = entry.value();
            if row.is_claimed() {
                let seq_id = *entry.key();
                drop(entry);
                self.rows.remove(&seq_id);
                continue;
            }
            if row.try_claim() {
                let owned = row.to_owned_row();
                self.row_count.fetch_sub(1, Ordering::Relaxed);
                self.remove_row(entry.key(), row);
                return Some(owned);
            }
        }
        None
    }

    /// Remove a claimed row from primary storage and all indexes.
    fn remove_row(&self, seq_id: &u64, row: &Row) {
        // Remove from each secondary index
        for idx in self.indexes.values() {
            if let Some(val) = row.columns.get(&idx.column) {
                idx.remove(val, *seq_id);
            }
        }
        // Remove from primary storage
        self.rows.remove(seq_id);
    }

    /// Background cleanup: remove claimed rows from SkipMap + all indexes.
    /// Called after SELECT pop claims rows via CAS.
    pub fn remove_claimed_rows(&self, seq_ids: &[u64]) {
        for seq_id in seq_ids {
            if let Some(entry) = self.rows.get(seq_id) {
                let row = entry.value();
                if row.is_claimed() {
                    // Remove from each secondary index
                    for idx in self.indexes.values() {
                        if let Some(val) = row.columns.get(&idx.column) {
                            idx.remove(val, *seq_id);
                        }
                    }
                    drop(entry);
                    self.rows.remove(seq_id);
                }
            }
        }
    }

    /// Get all seq_ids in order (FIFO = ascending, LIFO = descending).
    pub fn all_seq_ids_ordered(&self) -> Vec<u64> {
        match self.order {
            QueueOrder::Fifo => self.rows.iter().map(|e| *e.key()).collect(),
            QueueOrder::Lifo => {
                let mut v: Vec<u64> = self.rows.iter().map(|e| *e.key()).collect();
                v.reverse();
                v
            }
            QueueOrder::Priority { .. } => self.rows.iter().map(|e| *e.key()).collect(),
        }
    }

    /// Get the current row count.
    pub fn len(&self) -> i64 {
        self.row_count.load(Ordering::Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the table capacity (max_size or "unlimited").
    pub fn capacity_str(&self) -> String {
        match self.max_size {
            Some(n) => n.to_string(),
            None => "unlimited".to_string(),
        }
    }

    /// Add a secondary index to the table.
    pub fn add_index(&mut self, index: SecondaryIndex) {
        // Index existing rows
        let idx = Arc::new(index);
        for entry in self.rows.iter() {
            let row = entry.value();
            if !row.is_claimed() {
                if let Some(val) = row.columns.get(&idx.column) {
                    idx.insert(val.clone(), *entry.key());
                }
            }
        }
        self.indexes.insert(idx.name.clone(), idx);
    }
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.name)
            .field("order", &self.order)
            .field("max_size", &self.max_size)
            .field("row_count", &self.row_count.load(Ordering::Relaxed))
            .field("indexes", &self.indexes.keys().collect::<Vec<_>>())
            .finish()
    }
}
