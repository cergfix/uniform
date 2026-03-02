use std::collections::HashMap;

use crate::types::value::Value;

/// Replace `${key}` placeholders with values from the row columns.
/// Matches Go's buildStringTemplate().
pub fn build_string_template(template: &str, columns: &HashMap<String, Value>) -> String {
    let mut result = template.to_string();

    for (key, value) in columns {
        let placeholder = format!("${{{}}}", key);
        if result.contains(&placeholder) {
            result = result.replace(&placeholder, &value.to_string_repr());
        }
    }

    result
}

/// Replace `${key}` with URL-encoded values.
pub fn build_url_encoded_template(template: &str, columns: &HashMap<String, Value>) -> String {
    let mut result = template.to_string();

    for (key, value) in columns {
        let placeholder = format!("${{{}}}", key);
        if result.contains(&placeholder) {
            let repr = value.to_string_repr();
            let encoded = urlencoding::encode(&repr);
            result = result.replace(&placeholder, &encoded);
        }
    }

    result
}
