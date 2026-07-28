use crate::FoundationDbConfig;

#[derive(Debug, Clone)]
pub struct FoundationDbTransaction {
    config: FoundationDbConfig,
    read_only: bool,
}

impl FoundationDbTransaction {
    #[must_use]
    pub fn new(config: FoundationDbConfig, read_only: bool) -> Self {
        Self { config, read_only }
    }

    #[must_use]
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    #[must_use]
    pub fn config(&self) -> &FoundationDbConfig {
        &self.config
    }
}
