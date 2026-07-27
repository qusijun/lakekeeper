use std::sync::Arc;

use async_trait::async_trait;

use super::{
    AlteredPaimonEngineTable, DefaultPaimonEngine, InitializedPaimonTable, LoadedPaimonEngineTable,
    PaimonEngineError, PreparedPaimonCommit, PublishedPaimonCommit,
    default::{
        AlterPaimonEngineTableBackendRequest, CleanupStagedPaimonCommitBackendRequest,
        DynPaimonEngine, InitializePaimonTableBackendRequest, LoadPaimonEngineTableBackendRequest,
        PaimonEngineBackend, PreparePaimonCommitBackendRequest, PublishPaimonCommitBackendRequest,
        new_default_paimon_engine,
    },
};

const NATIVE_ENGINE_DISABLED_DETAIL: &str =
    "compile with feature `paimon-engine` to enable the native Paimon backend";
const NATIVE_ENGINE_UNWIRED_DETAIL: &str =
    "native Paimon backend is enabled but the paimon-rust bridge is not implemented yet";

#[derive(Debug, Default, Clone, Copy)]
pub struct NativePaimonEngineBackend;

#[async_trait]
impl PaimonEngineBackend for NativePaimonEngineBackend {
    async fn initialize_table(
        &self,
        _request: InitializePaimonTableBackendRequest,
    ) -> Result<InitializedPaimonTable, PaimonEngineError> {
        Err(native_backend_error())
    }

    async fn load_table(
        &self,
        _request: LoadPaimonEngineTableBackendRequest,
    ) -> Result<LoadedPaimonEngineTable, PaimonEngineError> {
        Err(native_backend_error())
    }

    async fn alter_table(
        &self,
        _request: AlterPaimonEngineTableBackendRequest,
    ) -> Result<AlteredPaimonEngineTable, PaimonEngineError> {
        Err(native_backend_error())
    }

    async fn prepare_commit(
        &self,
        _request: PreparePaimonCommitBackendRequest,
    ) -> Result<PreparedPaimonCommit, PaimonEngineError> {
        Err(native_backend_error())
    }

    async fn publish_commit(
        &self,
        _request: PublishPaimonCommitBackendRequest,
    ) -> Result<PublishedPaimonCommit, PaimonEngineError> {
        Err(native_backend_error())
    }

    async fn cleanup_staged_commit(
        &self,
        _request: CleanupStagedPaimonCommitBackendRequest,
    ) -> Result<(), PaimonEngineError> {
        Err(native_backend_error())
    }
}

#[must_use]
pub fn native_paimon_engine() -> DynPaimonEngine {
    new_default_paimon_engine(Arc::new(NativePaimonEngineBackend))
}

#[must_use]
pub fn native_default_paimon_engine() -> DefaultPaimonEngine<NativePaimonEngineBackend> {
    DefaultPaimonEngine::new(Arc::new(NativePaimonEngineBackend))
}

#[must_use]
pub fn native_backend_error() -> PaimonEngineError {
    if cfg!(feature = "paimon-engine") {
        PaimonEngineError::engine_unavailable(NATIVE_ENGINE_UNWIRED_DETAIL)
    } else {
        PaimonEngineError::engine_unavailable(NATIVE_ENGINE_DISABLED_DETAIL)
    }
}
