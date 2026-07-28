use crate::{CatalogState, FoundationDbBackendError, FoundationDbConfig, FoundationDbTransaction};

#[derive(Debug, Clone, Default)]
pub struct FoundationDbBackend;

impl FoundationDbBackend {
    #[must_use]
    pub fn state_from_config(config: FoundationDbConfig) -> CatalogState {
        CatalogState::from_config(config)
    }

    #[must_use]
    pub fn begin_read(config: FoundationDbConfig) -> FoundationDbTransaction {
        FoundationDbTransaction::new(config, true)
    }

    #[must_use]
    pub fn begin_write(config: FoundationDbConfig) -> FoundationDbTransaction {
        FoundationDbTransaction::new(config, false)
    }

    pub fn unsupported<T>(operation: &'static str) -> Result<T, FoundationDbBackendError> {
        Err(FoundationDbBackendError::not_implemented(operation))
    }
}
