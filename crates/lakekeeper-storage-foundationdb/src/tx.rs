use std::io;

use lakekeeper::{
    api::{ErrorModel, Result},
    service::{CatalogBackendError, Transaction},
};

use crate::{CatalogState, FoundationDbConfig, FoundationDbBackendError};

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

#[async_trait::async_trait]
impl Transaction<CatalogState> for FoundationDbTransaction {
    type Transaction<'a> = FoundationDbTransaction;

    async fn begin_write(db_state: CatalogState) -> Result<Self> {
        Ok(Self::new(db_state.config().clone(), false))
    }

    async fn begin_read(db_state: CatalogState) -> Result<Self> {
        Ok(Self::new(db_state.config().clone(), true))
    }

    async fn commit(self) -> Result<()> {
        Err(ErrorModel::from(CatalogBackendError::new_unexpected(
            io::Error::other(FoundationDbBackendError::not_implemented(
                "foundationdb transaction commit",
            )),
        ))
        .into())
    }

    async fn rollback(self) -> Result<()> {
        Err(ErrorModel::from(CatalogBackendError::new_unexpected(
            io::Error::other(FoundationDbBackendError::not_implemented(
                "foundationdb transaction rollback",
            )),
        ))
        .into())
    }

    fn transaction(&mut self) -> Self::Transaction<'_> {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use lakekeeper::service::Transaction;

    use crate::{CatalogState, FoundationDbConfig, FoundationDbTransaction};

    #[tokio::test]
    async fn begin_read_marks_transaction_read_only() {
        let state = CatalogState::from_config(FoundationDbConfig::default());
        let tx = FoundationDbTransaction::begin_read(state).await.unwrap();
        assert!(tx.read_only());
    }

    #[tokio::test]
    async fn begin_write_marks_transaction_writable() {
        let state = CatalogState::from_config(FoundationDbConfig::default());
        let tx = FoundationDbTransaction::begin_write(state).await.unwrap();
        assert!(!tx.read_only());
    }

    #[tokio::test]
    async fn transaction_accessor_preserves_flags() {
        let mut tx =
            FoundationDbTransaction::begin_write(CatalogState::from_config(FoundationDbConfig::default()))
                .await
                .unwrap();
        let cloned = tx.transaction();
        assert!(!cloned.read_only());
        assert_eq!(cloned.config().root_prefix, tx.config().root_prefix);
    }
}
