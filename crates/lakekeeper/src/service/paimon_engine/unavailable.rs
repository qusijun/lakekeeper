use async_trait::async_trait;

use super::{
    AlterPaimonEngineTableRequest, AlteredPaimonEngineTable, CleanupStagedPaimonCommitRequest,
    InitializePaimonTableRequest, InitializedPaimonTable, LoadPaimonEngineTableRequest,
    LoadedPaimonEngineTable, PaimonEngine, PaimonEngineError, PreparePaimonCommitRequest,
    PreparedPaimonCommit, PublishPaimonCommitRequest, PublishedPaimonCommit,
};

#[derive(Debug, Default, Clone, Copy)]
pub struct UnavailablePaimonEngine;

#[async_trait]
impl PaimonEngine for UnavailablePaimonEngine {
    async fn initialize_table(
        &self,
        _request: InitializePaimonTableRequest,
    ) -> Result<InitializedPaimonTable, PaimonEngineError> {
        Err(PaimonEngineError::engine_unavailable(
            "paimon-rust integration is not configured",
        ))
    }

    async fn load_table(
        &self,
        _request: LoadPaimonEngineTableRequest,
    ) -> Result<LoadedPaimonEngineTable, PaimonEngineError> {
        Err(PaimonEngineError::engine_unavailable(
            "paimon-rust integration is not configured",
        ))
    }

    async fn alter_table(
        &self,
        _request: AlterPaimonEngineTableRequest,
    ) -> Result<AlteredPaimonEngineTable, PaimonEngineError> {
        Err(PaimonEngineError::engine_unavailable(
            "paimon-rust integration is not configured",
        ))
    }

    async fn prepare_commit(
        &self,
        _request: PreparePaimonCommitRequest,
    ) -> Result<PreparedPaimonCommit, PaimonEngineError> {
        Err(PaimonEngineError::engine_unavailable(
            "paimon-rust integration is not configured",
        ))
    }

    async fn publish_commit(
        &self,
        _request: PublishPaimonCommitRequest,
    ) -> Result<PublishedPaimonCommit, PaimonEngineError> {
        Err(PaimonEngineError::engine_unavailable(
            "paimon-rust integration is not configured",
        ))
    }

    async fn cleanup_staged_commit(
        &self,
        _request: CleanupStagedPaimonCommitRequest,
    ) -> Result<(), PaimonEngineError> {
        Err(PaimonEngineError::engine_unavailable(
            "paimon-rust integration is not configured",
        ))
    }
}
