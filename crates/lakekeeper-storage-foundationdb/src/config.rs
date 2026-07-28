use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct FoundationDbConfig {
    pub cluster_file: Option<String>,
    pub tenant: Option<String>,
    pub root_prefix: String,
    pub api_version: i32,
    pub retry_limit: u32,
    pub retry_backoff_min_ms: u64,
    pub retry_backoff_max_ms: u64,
    pub transaction_timeout_ms: u64,
}

impl Default for FoundationDbConfig {
    fn default() -> Self {
        Self {
            cluster_file: None,
            tenant: None,
            root_prefix: "lakekeeper/catalog".to_string(),
            api_version: 740,
            retry_limit: 10,
            retry_backoff_min_ms: 5,
            retry_backoff_max_ms: 200,
            transaction_timeout_ms: 5_000,
        }
    }
}

impl FoundationDbConfig {
    #[must_use]
    pub fn root_prefix_segments(&self) -> Vec<&str> {
        self.root_prefix.split('/').filter(|s| !s.is_empty()).collect()
    }

    #[must_use]
    pub fn cluster_file_or_default(&self) -> &str {
        self.cluster_file.as_deref().unwrap_or("/etc/foundationdb/fdb.cluster")
    }
}

#[cfg(test)]
mod tests {
    use super::FoundationDbConfig;

    #[test]
    fn default_root_prefix_is_stable() {
        let config = FoundationDbConfig::default();
        assert_eq!(config.root_prefix, "lakekeeper/catalog");
        assert_eq!(config.root_prefix_segments(), vec!["lakekeeper", "catalog"]);
    }

    #[test]
    fn empty_segments_are_filtered() {
        let config = FoundationDbConfig {
            root_prefix: "/lakekeeper//catalog/".to_string(),
            ..FoundationDbConfig::default()
        };
        assert_eq!(config.root_prefix_segments(), vec!["lakekeeper", "catalog"]);
    }

    #[test]
    fn cluster_file_falls_back_to_default() {
        let config = FoundationDbConfig::default();
        assert_eq!(
            config.cluster_file_or_default(),
            "/etc/foundationdb/fdb.cluster"
        );
    }
}
