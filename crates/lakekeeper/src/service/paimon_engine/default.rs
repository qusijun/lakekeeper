use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use iceberg::NamespaceIdent;
use serde_json::Value;
use uuid::Uuid;

use super::{
    AlterPaimonEngineTableRequest, AlteredPaimonEngineTable, CleanupStagedPaimonCommitRequest,
    InitializePaimonTableRequest, InitializedPaimonTable, LoadPaimonEngineTableRequest,
    LoadedPaimonEngineTable, PaimonEngine, PaimonEngineError, PaimonEngineSchema,
    PreparePaimonCommitRequest, PreparedPaimonCommit, PublishPaimonCommitRequest,
    PublishedPaimonCommit,
};
use crate::service::{
    Location, LogicalSchema, TableId, WarehouseId,
    paimon_engine::adapters::{
        options::{normalize_engine_options, normalize_table_options},
        schema::{engine_schema_from_logical, logical_schema_from_engine},
    },
};

#[derive(Debug, Clone)]
pub struct DefaultPaimonEngine<B> {
    backend: Arc<B>,
}

impl<B> DefaultPaimonEngine<B> {
    #[must_use]
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializePaimonTableBackendRequest {
    pub warehouse_id: WarehouseId,
    pub namespace: NamespaceIdent,
    pub table_name: String,
    pub location: Location,
    pub engine_schema: PaimonEngineSchema,
    pub normalized_options: HashMap<String, String>,
    pub primary_keys: Vec<String>,
    pub partition_keys: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadPaimonEngineTableBackendRequest {
    pub warehouse_id: WarehouseId,
    pub table_location: Location,
    pub metadata_location: Option<Location>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPaimonEngineTableBackendRequest {
    pub warehouse_id: WarehouseId,
    pub tabular_id: TableId,
    pub table_location: Location,
    pub metadata_location: Option<Location>,
    pub current_snapshot_id: Option<i64>,
    pub engine_schema: PaimonEngineSchema,
    pub normalized_options: HashMap<String, String>,
    pub primary_keys: Vec<String>,
    pub partition_keys: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparePaimonCommitBackendRequest {
    pub warehouse_id: WarehouseId,
    pub tabular_id: TableId,
    pub table_location: Location,
    pub metadata_location: Option<Location>,
    pub current_snapshot_id: Option<i64>,
    pub operations: Vec<Value>,
    pub expected_current_snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishPaimonCommitBackendRequest {
    pub warehouse_id: WarehouseId,
    pub tabular_id: TableId,
    pub commit_token: Uuid,
    pub staged_metadata_location: Option<Location>,
    pub current_metadata_location: Option<Location>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupStagedPaimonCommitBackendRequest {
    pub warehouse_id: WarehouseId,
    pub tabular_id: TableId,
    pub commit_token: Uuid,
    pub staged_metadata_location: Option<Location>,
}

#[async_trait]
pub trait PaimonEngineBackend: Send + Sync {
    async fn initialize_table(
        &self,
        request: InitializePaimonTableBackendRequest,
    ) -> Result<InitializedPaimonTable, PaimonEngineError>;

    async fn load_table(
        &self,
        request: LoadPaimonEngineTableBackendRequest,
    ) -> Result<LoadedPaimonEngineTable, PaimonEngineError>;

    async fn alter_table(
        &self,
        request: AlterPaimonEngineTableBackendRequest,
    ) -> Result<AlteredPaimonEngineTable, PaimonEngineError>;

    async fn prepare_commit(
        &self,
        request: PreparePaimonCommitBackendRequest,
    ) -> Result<PreparedPaimonCommit, PaimonEngineError>;

    async fn publish_commit(
        &self,
        request: PublishPaimonCommitBackendRequest,
    ) -> Result<PublishedPaimonCommit, PaimonEngineError>;

    async fn cleanup_staged_commit(
        &self,
        request: CleanupStagedPaimonCommitBackendRequest,
    ) -> Result<(), PaimonEngineError>;
}

#[async_trait]
impl<B> PaimonEngine for DefaultPaimonEngine<B>
where
    B: PaimonEngineBackend,
{
    async fn initialize_table(
        &self,
        request: InitializePaimonTableRequest,
    ) -> Result<InitializedPaimonTable, PaimonEngineError> {
        let backend_request = InitializePaimonTableBackendRequest {
            warehouse_id: request.warehouse_id,
            namespace: request.namespace,
            table_name: request.table_name,
            location: request.location,
            engine_schema: engine_schema_from_logical(&request.logical_schema)?,
            normalized_options: normalize_table_options(&request.table_options)?,
            primary_keys: request.primary_keys,
            partition_keys: request.partition_keys,
            comment: request.comment,
        };
        let response = self.backend.initialize_table(backend_request).await?;
        Ok(InitializedPaimonTable {
            metadata_location: response.metadata_location,
            current_snapshot_id: response.current_snapshot_id,
            logical_schema: normalize_logical_schema(response.logical_schema)?,
            normalized_options: normalize_engine_options(&response.normalized_options)?,
        })
    }

    async fn load_table(
        &self,
        request: LoadPaimonEngineTableRequest,
    ) -> Result<LoadedPaimonEngineTable, PaimonEngineError> {
        let response = self
            .backend
            .load_table(LoadPaimonEngineTableBackendRequest {
                warehouse_id: request.warehouse_id,
                table_location: request.table_location,
                metadata_location: request.metadata_location,
            })
            .await?;
        Ok(LoadedPaimonEngineTable {
            metadata_location: response.metadata_location,
            current_snapshot_id: response.current_snapshot_id,
            current_branch: response.current_branch,
            logical_schema: normalize_logical_schema(response.logical_schema)?,
            normalized_options: normalize_engine_options(&response.normalized_options)?,
            primary_keys: response.primary_keys,
            partition_keys: response.partition_keys,
            comment: response.comment,
        })
    }

    async fn alter_table(
        &self,
        request: AlterPaimonEngineTableRequest,
    ) -> Result<AlteredPaimonEngineTable, PaimonEngineError> {
        let response = self
            .backend
            .alter_table(AlterPaimonEngineTableBackendRequest {
                warehouse_id: request.warehouse_id,
                tabular_id: request.tabular_id,
                table_location: request.table_location,
                metadata_location: request.metadata_location,
                current_snapshot_id: request.current_snapshot_id,
                engine_schema: engine_schema_from_logical(&request.logical_schema)?,
                normalized_options: normalize_table_options(&request.table_options)?,
                primary_keys: request.primary_keys,
                partition_keys: request.partition_keys,
                comment: request.comment,
            })
            .await?;
        Ok(AlteredPaimonEngineTable {
            metadata_location: response.metadata_location,
            current_snapshot_id: response.current_snapshot_id,
            logical_schema: normalize_logical_schema(response.logical_schema)?,
            normalized_options: normalize_engine_options(&response.normalized_options)?,
            primary_keys: response.primary_keys,
            partition_keys: response.partition_keys,
            comment: response.comment,
        })
    }

    async fn prepare_commit(
        &self,
        request: PreparePaimonCommitRequest,
    ) -> Result<PreparedPaimonCommit, PaimonEngineError> {
        self.backend
            .prepare_commit(PreparePaimonCommitBackendRequest {
                warehouse_id: request.warehouse_id,
                tabular_id: request.tabular_id,
                table_location: request.table_location,
                metadata_location: request.metadata_location,
                current_snapshot_id: request.current_snapshot_id,
                operations: request.operations,
                expected_current_snapshot_id: request.expected_current_snapshot_id,
            })
            .await
    }

    async fn publish_commit(
        &self,
        request: PublishPaimonCommitRequest,
    ) -> Result<PublishedPaimonCommit, PaimonEngineError> {
        self.backend
            .publish_commit(PublishPaimonCommitBackendRequest {
                warehouse_id: request.warehouse_id,
                tabular_id: request.tabular_id,
                commit_token: request.commit_token,
                staged_metadata_location: request.staged_metadata_location,
                current_metadata_location: request.current_metadata_location,
            })
            .await
    }

    async fn cleanup_staged_commit(
        &self,
        request: CleanupStagedPaimonCommitRequest,
    ) -> Result<(), PaimonEngineError> {
        self.backend
            .cleanup_staged_commit(CleanupStagedPaimonCommitBackendRequest {
                warehouse_id: request.warehouse_id,
                tabular_id: request.tabular_id,
                commit_token: request.commit_token,
                staged_metadata_location: request.staged_metadata_location,
            })
            .await
    }
}

fn normalize_logical_schema(schema: LogicalSchema) -> Result<LogicalSchema, PaimonEngineError> {
    let engine_schema = engine_schema_from_logical(&schema)?;
    logical_schema_from_engine(&engine_schema)
}
