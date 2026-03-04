/// Lowercase all top-level keys of a JSON object.
pub fn normalize_json_keys(val: serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::Object(map) => {
            let normalized: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .map(|(k, v)| (k.to_lowercase(), v))
                .collect();
            serde_json::Value::Object(normalized)
        }
        other => other,
    }
}
