use parking_lot::RwLock;

static CLUSTER_ROLE: once_cell::sync::Lazy<RwLock<String>> =
    once_cell::sync::Lazy::new(|| RwLock::new("default".to_string()));

pub fn set_cluster_role(role: &str) -> bool {
    if !role.is_empty() {
        *CLUSTER_ROLE.write() = role.to_string();
        crate::util::logging::log(&format!("Setting cluster role to: {}", role));
        true
    } else {
        crate::util::logging::log(&format!("Failed setting cluster role to: {}", role));
        false
    }
}

pub fn get_cluster_role() -> String {
    CLUSTER_ROLE.read().clone()
}
