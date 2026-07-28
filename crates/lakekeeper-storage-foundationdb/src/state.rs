use std::sync::Arc;

use lakekeeper::service::health::{Health, HealthExt, HealthStatus};
use tokio::sync::RwLock;

use crate::config::FoundationDbConfig;

#[derive(Clone, Debug)]
pub struct CatalogState {
    config: FoundationDbConfig,
    health: Arc<RwLock<Vec<Health>>>,
}

impl CatalogState {
    #[must_use]
    pub fn from_config(config: FoundationDbConfig) -> Self {
        Self {
            config,
            health: Arc::new(RwLock::new(vec![Health::now(
                "foundationdb",
                HealthStatus::Unknown,
            )])),
        }
    }

    #[must_use]
    pub fn config(&self) -> &FoundationDbConfig {
        &self.config
    }

    #[must_use]
    pub fn root_prefix_segments(&self) -> Vec<&str> {
        self.config.root_prefix_segments()
    }

    #[must_use]
    pub fn cluster_file(&self) -> &str {
        self.config.cluster_file_or_default()
    }
}

#[async_trait::async_trait]
impl HealthExt for CatalogState {
    async fn health(&self) -> Vec<Health> {
        self.health.read().await.clone()
    }

    async fn update_health(&self) {
        let mut lock = self.health.write().await;
        lock.clear();
        lock.push(Health::now("foundationdb", HealthStatus::Unknown));
    }
}

#[cfg(test)]
mod tests {
    use lakekeeper::service::health::{HealthExt, HealthStatus};

    use crate::{CatalogState, FoundationDbConfig};

    #[tokio::test]
    async fn health_defaults_to_unknown() {
        let state = CatalogState::from_config(FoundationDbConfig::default());
        let health = state.health().await;
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].status(), HealthStatus::Unknown);
        assert_eq!(state.root_prefix_segments(), vec!["lakekeeper", "catalog"]);
        assert_eq!(state.cluster_file(), "/etc/foundationdb/fdb.cluster");
    }
}
