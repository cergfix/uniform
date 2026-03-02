use std::ops::Bound;
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

use crate::types::value::Value;

/// Secondary index on a single column.
/// Uses a lock-free SkipMap with composite key `(column_value, seq_id)`.
///
/// This gives us:
/// - Value lookup + seq_id ordering in a single structure
/// - Range scan for equality: `(Value("pending"), 0)..=(Value("pending"), u64::MAX)`
/// - Range scan for range: `(Value("a"), 0)..(Value("z"), 0)`
/// - Within each value, seq_ids are sorted → FIFO/LIFO ordering preserved
pub struct SecondaryIndex {
    pub name: String,
    pub column: String,
    pub unique: bool,
    pub data: SkipMap<(Value, u64), ()>,
}

impl SecondaryIndex {
    pub fn new(name: String, column: String, unique: bool) -> Self {
        SecondaryIndex {
            name,
            column,
            unique,
            data: SkipMap::new(),
        }
    }

    /// Insert a (value, seq_id) entry into the index.
    pub fn insert(&self, value: Value, seq_id: u64) {
        self.data.insert((value, seq_id), ());
    }

    /// Remove a (value, seq_id) entry from the index.
    pub fn remove(&self, value: &Value, seq_id: u64) {
        self.data.remove(&(value.clone(), seq_id));
    }

    /// Find all seq_ids matching an exact value, in seq_id order (ascending = FIFO).
    pub fn find_eq(&self, value: &Value) -> Vec<u64> {
        let start = (value.clone(), 0u64);
        let end = (value.clone(), u64::MAX);
        self.data
            .range(start..=end)
            .map(|entry| entry.key().1)
            .collect()
    }

    /// Find all seq_ids in a value range [low, high], in seq_id order.
    pub fn find_range(&self, low: &Value, high: &Value) -> Vec<u64> {
        let start = (low.clone(), 0u64);
        let end = (high.clone(), u64::MAX);
        self.data
            .range(start..=end)
            .map(|entry| entry.key().1)
            .collect()
    }

    /// Find all seq_ids where value > low, in seq_id order.
    pub fn find_gt(&self, low: &Value) -> Vec<u64> {
        let start = Bound::Excluded((low.clone(), u64::MAX));
        let end = Bound::Unbounded;
        self.data
            .range((start, end))
            .map(|entry| entry.key().1)
            .collect()
    }

    /// Find all seq_ids where value >= low, in seq_id order.
    pub fn find_gte(&self, low: &Value) -> Vec<u64> {
        let start = Bound::Included((low.clone(), 0u64));
        let end = Bound::Unbounded;
        self.data
            .range((start, end))
            .map(|entry| entry.key().1)
            .collect()
    }

    /// Find all seq_ids where value < high, in seq_id order.
    pub fn find_lt(&self, high: &Value) -> Vec<u64> {
        let start = Bound::Unbounded;
        let end = Bound::Excluded((high.clone(), 0u64));
        self.data
            .range((start, end))
            .map(|entry| entry.key().1)
            .collect()
    }

    /// Find all seq_ids where value <= high, in seq_id order.
    pub fn find_lte(&self, high: &Value) -> Vec<u64> {
        let start = Bound::Unbounded;
        let end = Bound::Included((high.clone(), u64::MAX));
        self.data
            .range((start, end))
            .map(|entry| entry.key().1)
            .collect()
    }

    /// Check if any live (unclaimed) row has this value.
    /// Used for unique index constraint checks on INSERT.
    /// `is_live` closure checks if the row at seq_id is not claimed.
    pub fn has_live_entry<F>(&self, value: &Value, is_live: F) -> bool
    where
        F: Fn(u64) -> bool,
    {
        let start = (value.clone(), 0u64);
        let end = (value.clone(), u64::MAX);
        self.data
            .range(start..=end)
            .any(|entry| is_live(entry.key().1))
    }

    /// Count entries in the index.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl std::fmt::Debug for SecondaryIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecondaryIndex")
            .field("name", &self.name)
            .field("column", &self.column)
            .field("unique", &self.unique)
            .field("entries", &self.data.len())
            .finish()
    }
}

/// Wraps a SecondaryIndex in an Arc for shared ownership.
pub type SharedIndex = Arc<SecondaryIndex>;
