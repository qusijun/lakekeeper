use std::sync::Arc;

use async_trait::async_trait;

use super::{
    AlterPaimonEngineTableRequest, AlteredPaimonEngineTable, CleanupStagedPaimonCommitRequest,
    DefaultPaimonEngine, InitializePaimonTableRequest, InitializedPaimonTable,
    LoadPaimonEngineTableRequest, LoadedPaimonEngineTable, PaimonEngine, PaimonEngineError,
    PreparePaimonCommitRequest, PreparedPaimonCommit, PublishPaimonCommitRequest,
    PublishedPaimonCommit,
    default::{
        AlterPaimonEngineTableBackendRequest, CleanupStagedPaimonCommitBackendRequest,
        DynPaimonEngine, InitializePaimonTableBackendRequest, LoadPaimonEngineTableBackendRequest,
        PaimonEngineBackend, PreparePaimonCommitBackendRequest, PublishPaimonCommitBackendRequest,
        new_default_paimon_engine,
    },
};

const ENGINE_UNAVAILABLE_DETAIL: &str = "paimon-rust integration is not configured";

#[derive(Debug, Default, Clone, Copy)]
pub struct UnavailablePaimonEngine;

#[async_trait]
impl PaimonEngine for UnavailablePaimonEngine {
    async fn initialize_table(
        &self,
        _request: InitializePaimonTableRequest,
    ) -> Result<InitializedPaimonTable, PaimonEngineError> {
        Err(unavailable_error())
    }

    async fn load_table(
        &self,
        _request: LoadPaimonEngineTableRequest,
    ) -> Result<LoadedPaimonEngineTable, PaimonEngineError> {
        Err(unavailable_error())
    }

    async fn alter_table(
        &self,
        _request: AlterPaimonEngineTableRequest,
    ) -> Result<AlteredPaimonEngineTable, PaimonEngineError> {
        Err(unavailable_error())
    }

    async fn prepare_commit(
        &self,
        _request: PreparePaimonCommitRequest,
    ) -> Result<PreparedPaimonCommit, PaimonEngineError> {
        Err(unavailable_error())
    }

    async fn publish_commit(
        &self,
        _request: PublishPaimonCommitRequest,
    ) -> Result<PublishedPaimonCommit, PaimonEngineError> {
        Err(unavailable_error())
    }

    async fn cleanup_staged_commit(
        &self,
        _request: CleanupStagedPaimonCommitRequest,
    ) -> Result<(), PaimonEngineError> {
        Err(unavailable_error())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UnavailablePaimonEngineBackend;

#[async_trait]
impl PaimonEngineBackend for UnavailablePaimonEngineBackend {
    async fn initialize_table(
        &self,
        _request: InitializePaimonTableBackendRequest,
    ) -> Result<InitializedPaimonTable, PaimonEngineError> {
        Err(unavailable_error())
    }

    async fn load_table(
        &self,
        _request: LoadPaimonEngineTableBackendRequest,
    ) -> Result<LoadedPaimonEngineTable, PaimonEngineError> {
        Err(unavailable_error())
    }

    async fn alter_table(
        &self,
        _request: AlterPaimonEngineTableBackendRequest,
    ) -> Result<AlteredPaimonEngineTable, PaimonEngineError> {
        Err(unavailable_error())
    }

    async fn prepare_commit(
        &self,
        _request: PreparePaimonCommitBackendRequest,
    ) -> Result<PreparedPaimonCommit, PaimonEngineError> {
        Err(unavailable_error())
    }

    async fn publish_commit(
        &self,
        _request: PublishPaimonCommitBackendRequest,
    ) -> Result<PublishedPaimonCommit, PaimonEngineError> {
        Err(unavailable_error())
    }

    async fn cleanup_staged_commit(
        &self,
        _request: CleanupStagedPaimonCommitBackendRequest,
    ) -> Result<(), PaimonEngineError> {
        Err(unavailable_error())
    }
}

#[must_use]
pub fn unavailable_paimon_engine() -> DynPaimonEngine {
    new_default_paimon_engine(Arc::new(UnavailablePaimonEngineBackend))
}

#[must_use]
pub fn unavailable_default_paimon_engine() -> DefaultPaimonEngine<UnavailablePaimonEngineBackend> {
    DefaultPaimonEngine::new(Arc::new(UnavailablePaimonEngineBackend))
}

fn unavailable_error() -> PaimonEngineError {
    PaimonEngineError::engine_unavailable(ENGINE_UNAVAILABLE_DETAIL)
}
