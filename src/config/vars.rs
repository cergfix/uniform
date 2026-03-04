use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

pub const APP_NAME: &str = "Uniform";
pub const COPYRIGHT: &str =
    "Copyright (c) 2018 - 2026 Uniform contributors. Licensed under Apache-2.0.";

// Injected at build time via env vars, or defaults
pub fn version() -> &'static str {
    option_env!("UNIFORM_VERSION").unwrap_or("0.9.0")
}

pub fn client() -> &'static str {
    option_env!("UNIFORM_CLIENT").unwrap_or("")
}

static WORKERS_FORCE_SHUTDOWN: AtomicBool = AtomicBool::new(false);
static PERSIST_METADATA: AtomicBool = AtomicBool::new(false);

pub fn workers_force_shutdown() -> bool {
    WORKERS_FORCE_SHUTDOWN.load(Ordering::Relaxed)
}

pub fn set_workers_force_shutdown(val: bool) {
    WORKERS_FORCE_SHUTDOWN.store(val, Ordering::Relaxed);
}

pub fn persist_metadata() -> bool {
    PERSIST_METADATA.load(Ordering::Relaxed)
}

pub fn set_persist_metadata(val: bool) {
    PERSIST_METADATA.store(val, Ordering::Relaxed);
}

static SELECT_CACHE_ENABLED: AtomicBool = AtomicBool::new(false);

pub fn select_cache_enabled() -> bool {
    SELECT_CACHE_ENABLED.load(Ordering::Relaxed)
}

pub fn set_select_cache_enabled(val: bool) {
    SELECT_CACHE_ENABLED.store(val, Ordering::Relaxed);
}

// Force select sleep (ms) - per-connection override
static FORCE_SELECT_SLEEP_MS: once_cell::sync::Lazy<RwLock<i32>> =
    once_cell::sync::Lazy::new(|| RwLock::new(0));

pub fn force_select_sleep_ms() -> i32 {
    *FORCE_SELECT_SLEEP_MS.read()
}

pub fn set_force_select_sleep_ms(val: i32) {
    *FORCE_SELECT_SLEEP_MS.write() = val;
}
