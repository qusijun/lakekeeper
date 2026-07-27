use std::collections::HashMap;

use crate::service::PaimonEngineError;

fn normalize_key(raw: &str) -> Result<String, PaimonEngineError> {
    let key = raw.trim().to_ascii_lowercase();
    if key.is_empty() {
        return Err(PaimonEngineError::unsupported_options(
            "option keys must not be blank",
        ));
    }
    if !key
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
    {
        return Err(PaimonEngineError::unsupported_options(format!(
            "option key '{raw}' contains unsupported characters",
        )));
    }
    Ok(key)
}

fn normalize_value(raw: &str) -> String {
    raw.trim().to_string()
}

pub fn normalize_table_options(
    options: &HashMap<String, String>,
) -> Result<HashMap<String, String>, PaimonEngineError> {
    let mut normalized = HashMap::with_capacity(options.len());
    for (raw_key, raw_value) in options {
        let key = normalize_key(raw_key)?;
        let value = normalize_value(raw_value);
        if normalized.insert(key.clone(), value).is_some() {
            return Err(PaimonEngineError::unsupported_options(format!(
                "multiple options normalize to the same key '{key}'",
            )));
        }
    }
    Ok(normalized)
}

pub fn normalize_engine_options(
    options: &HashMap<String, String>,
) -> Result<HashMap<String, String>, PaimonEngineError> {
    normalize_table_options(options)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{normalize_engine_options, normalize_table_options};

    #[test]
    fn normalizes_keys_and_values_deterministically() {
        let options = HashMap::from([
            ("  bucket ".to_string(), " 4 ".to_string()),
            ("Write.Mode".to_string(), " append ".to_string()),
        ]);

        let normalized = normalize_table_options(&options).expect("options must normalize");
        assert_eq!(normalized.get("bucket"), Some(&"4".to_string()));
        assert_eq!(normalized.get("write.mode"), Some(&"append".to_string()));
        assert_eq!(
            normalize_engine_options(&normalized).expect("normalized options must stay stable"),
            normalized
        );
    }

    #[test]
    fn rejects_colliding_normalized_keys() {
        let options = HashMap::from([
            ("Bucket".to_string(), "4".to_string()),
            (" bucket ".to_string(), "5".to_string()),
        ]);

        let err = normalize_table_options(&options).expect_err("duplicate keys must fail");
        assert!(err.to_string().contains("same key"));
    }
}
