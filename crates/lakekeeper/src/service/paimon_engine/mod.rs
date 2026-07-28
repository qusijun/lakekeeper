use std::fmt;

use async_trait::async_trait;

mod default;
mod native;
mod unavailable;
pub use default::{DefaultPaimonEngine, DynPaimonEngine, new_default_paimon_engine};
pub use native::{
    MaterializedNativePaimonCatalogOptionSet, NativePaimonAdlsConfig, NativePaimonAdlsProfile,
    NativePaimonCatalogAuth, NativePaimonCatalogBootstrap, NativePaimonCatalogBridge,
    NativePaimonCatalogOptionSet, NativePaimonEngineBackend, NativePaimonGcsAuth,
    NativePaimonGcsConfig, NativePaimonRuntimeConfig, NativePaimonS3Auth, NativePaimonS3Config,
    NativePaimonStorageConfig, NativePaimonTempFileOption, native_backend_error,
    native_default_paimon_engine, native_paimon_engine,
};
pub use unavailable::{
    UnavailablePaimonEngine, UnavailablePaimonEngineBackend, unavailable_default_paimon_engine,
    unavailable_paimon_engine,
};

pub mod adapters;
mod types;
pub use types::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaimonPublishFailureClass {
    Retriable,
    Fatal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaimonEngineError {
    Validation {
        detail: String,
    },
    UnsupportedSchema {
        detail: String,
    },
    UnsupportedOptions {
        detail: String,
    },
    PrepareConflict {
        detail: String,
    },
    PublishFailed {
        class: PaimonPublishFailureClass,
        detail: String,
    },
    CleanupFailed {
        detail: String,
    },
    EngineUnavailable {
        detail: String,
    },
}

impl PaimonEngineError {
    #[must_use]
    pub fn validation(detail: impl Into<String>) -> Self {
        Self::Validation {
            detail: detail.into(),
        }
    }

    #[must_use]
    pub fn unsupported_schema(detail: impl Into<String>) -> Self {
        Self::UnsupportedSchema {
            detail: detail.into(),
        }
    }

    #[must_use]
    pub fn unsupported_options(detail: impl Into<String>) -> Self {
        Self::UnsupportedOptions {
            detail: detail.into(),
        }
    }

    #[must_use]
    pub fn prepare_conflict(detail: impl Into<String>) -> Self {
        Self::PrepareConflict {
            detail: detail.into(),
        }
    }

    #[must_use]
    pub fn publish_failed(class: PaimonPublishFailureClass, detail: impl Into<String>) -> Self {
        Self::PublishFailed {
            class,
            detail: detail.into(),
        }
    }

    #[must_use]
    pub fn cleanup_failed(detail: impl Into<String>) -> Self {
        Self::CleanupFailed {
            detail: detail.into(),
        }
    }

    #[must_use]
    pub fn engine_unavailable(detail: impl Into<String>) -> Self {
        Self::EngineUnavailable {
            detail: detail.into(),
        }
    }

    #[must_use]
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::PublishFailed {
                class: PaimonPublishFailureClass::Retriable,
                ..
            }
        )
    }
}

impl std::error::Error for PaimonEngineError {}

impl fmt::Display for PaimonEngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation { detail } => write!(f, "Paimon engine validation failed: {detail}"),
            Self::UnsupportedSchema { detail } => {
                write!(f, "Paimon engine schema is unsupported: {detail}")
            }
            Self::UnsupportedOptions { detail } => {
                write!(f, "Paimon engine options are unsupported: {detail}")
            }
            Self::PrepareConflict { detail } => {
                write!(f, "Paimon engine prepare conflict: {detail}")
            }
            Self::PublishFailed { class, detail } => {
                write!(f, "Paimon engine publish failed ({class:?}): {detail}")
            }
            Self::CleanupFailed { detail } => {
                write!(f, "Paimon engine cleanup failed: {detail}")
            }
            Self::EngineUnavailable { detail } => {
                write!(f, "Paimon engine is unavailable: {detail}")
            }
        }
    }
}

#[async_trait]
pub trait PaimonEngine: Send + Sync {
    async fn initialize_table(
        &self,
        request: InitializePaimonTableRequest,
    ) -> Result<InitializedPaimonTable, PaimonEngineError>;

    async fn load_table(
        &self,
        request: LoadPaimonEngineTableRequest,
    ) -> Result<LoadedPaimonEngineTable, PaimonEngineError>;

    async fn alter_table(
        &self,
        request: AlterPaimonEngineTableRequest,
    ) -> Result<AlteredPaimonEngineTable, PaimonEngineError>;

    async fn prepare_commit(
        &self,
        request: PreparePaimonCommitRequest,
    ) -> Result<PreparedPaimonCommit, PaimonEngineError>;

    async fn publish_commit(
        &self,
        request: PublishPaimonCommitRequest,
    ) -> Result<PublishedPaimonCommit, PaimonEngineError>;

    async fn cleanup_staged_commit(
        &self,
        request: CleanupStagedPaimonCommitRequest,
    ) -> Result<(), PaimonEngineError>;
}

#[must_use]
pub fn default_paimon_engine() -> DynPaimonEngine {
    if cfg!(feature = "paimon-engine") {
        native_paimon_engine()
    } else {
        unavailable_paimon_engine()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;
    use iceberg::NamespaceIdent;
    use serde_json::json;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::{
        AlterPaimonEngineTableRequest, AlteredPaimonEngineTable, CleanupStagedPaimonCommitRequest,
        DefaultPaimonEngine, InitializePaimonTableRequest, InitializedPaimonTable,
        LoadPaimonEngineTableRequest, LoadedPaimonEngineTable, PaimonEngine, PaimonEngineError,
        PaimonEngineField, PaimonEnginePrimitiveType, PaimonEngineSchema, PaimonEngineType,
        PaimonPublishFailureClass, PreparePaimonCommitRequest, PreparedPaimonCommit,
        PublishPaimonCommitRequest, PublishedPaimonCommit, default_paimon_engine,
        native_backend_error, unavailable_paimon_engine,
    };
    use crate::service::{
        Location, LogicalField, LogicalPrimitiveType, LogicalSchema, LogicalType, TableId,
        WarehouseId,
        paimon_engine::default::{
            AlterPaimonEngineTableBackendRequest, CleanupStagedPaimonCommitBackendRequest,
            InitializePaimonTableBackendRequest, LoadPaimonEngineTableBackendRequest,
            PaimonEngineBackend, PreparePaimonCommitBackendRequest,
            PublishPaimonCommitBackendRequest,
        },
    };

    #[test]
    fn retriable_publish_errors_are_detectable() {
        assert!(
            PaimonEngineError::publish_failed(PaimonPublishFailureClass::Retriable, "retry later",)
                .is_retriable()
        );
        assert!(
            !PaimonEngineError::publish_failed(PaimonPublishFailureClass::Fatal, "do not retry",)
                .is_retriable()
        );
    }

    #[derive(Default)]
    struct RecordingBackend {
        initialize_requests: Mutex<Vec<InitializePaimonTableBackendRequest>>,
        initialize_result: Mutex<Option<Result<InitializedPaimonTable, PaimonEngineError>>>,
        load_requests: Mutex<Vec<LoadPaimonEngineTableBackendRequest>>,
        load_result: Mutex<Option<Result<LoadedPaimonEngineTable, PaimonEngineError>>>,
        alter_requests: Mutex<Vec<AlterPaimonEngineTableBackendRequest>>,
        alter_result: Mutex<Option<Result<AlteredPaimonEngineTable, PaimonEngineError>>>,
        prepare_requests: Mutex<Vec<PreparePaimonCommitBackendRequest>>,
        prepare_result: Mutex<Option<Result<PreparedPaimonCommit, PaimonEngineError>>>,
        publish_requests: Mutex<Vec<PublishPaimonCommitBackendRequest>>,
        publish_result: Mutex<Option<Result<PublishedPaimonCommit, PaimonEngineError>>>,
        cleanup_requests: Mutex<Vec<CleanupStagedPaimonCommitBackendRequest>>,
        cleanup_result: Mutex<Option<Result<(), PaimonEngineError>>>,
    }

    #[async_trait]
    impl PaimonEngineBackend for RecordingBackend {
        async fn initialize_table(
            &self,
            request: InitializePaimonTableBackendRequest,
        ) -> Result<InitializedPaimonTable, PaimonEngineError> {
            self.initialize_requests.lock().await.push(request);
            self.initialize_result
                .lock()
                .await
                .take()
                .expect("initialize result must be configured")
        }

        async fn load_table(
            &self,
            request: LoadPaimonEngineTableBackendRequest,
        ) -> Result<super::LoadedPaimonEngineTable, PaimonEngineError> {
            self.load_requests.lock().await.push(request);
            self.load_result
                .lock()
                .await
                .take()
                .expect("load result must be configured")
        }

        async fn alter_table(
            &self,
            request: AlterPaimonEngineTableBackendRequest,
        ) -> Result<super::AlteredPaimonEngineTable, PaimonEngineError> {
            self.alter_requests.lock().await.push(request);
            self.alter_result
                .lock()
                .await
                .take()
                .expect("alter result must be configured")
        }

        async fn prepare_commit(
            &self,
            request: PreparePaimonCommitBackendRequest,
        ) -> Result<super::PreparedPaimonCommit, PaimonEngineError> {
            self.prepare_requests.lock().await.push(request);
            self.prepare_result
                .lock()
                .await
                .take()
                .expect("prepare result must be configured")
        }

        async fn publish_commit(
            &self,
            request: PublishPaimonCommitBackendRequest,
        ) -> Result<PublishedPaimonCommit, PaimonEngineError> {
            self.publish_requests.lock().await.push(request);
            self.publish_result
                .lock()
                .await
                .take()
                .expect("publish result must be configured")
        }

        async fn cleanup_staged_commit(
            &self,
            request: CleanupStagedPaimonCommitBackendRequest,
        ) -> Result<(), PaimonEngineError> {
            self.cleanup_requests.lock().await.push(request);
            self.cleanup_result
                .lock()
                .await
                .take()
                .expect("cleanup result must be configured")
        }
    }

    fn sample_logical_schema(primitive: LogicalPrimitiveType) -> LogicalSchema {
        LogicalSchema {
            schema_id: 7,
            root_fields: vec![LogicalField {
                field_id: 10,
                name: "id".to_string(),
                required: true,
                doc: Some("identifier".to_string()),
                field_type: LogicalType::Primitive(primitive),
                initial_default: None,
                write_default: None,
                is_identity_hint: true,
            }],
        }
    }

    fn sample_engine_schema(primitive: PaimonEnginePrimitiveType) -> PaimonEngineSchema {
        PaimonEngineSchema {
            schema_id: 7,
            root_fields: vec![PaimonEngineField {
                field_id: 10,
                name: "id".to_string(),
                required: true,
                doc: Some("identifier".to_string()),
                field_type: PaimonEngineType::Primitive(primitive),
                initial_default: None,
                write_default: None,
                is_primary_key: true,
            }],
        }
    }

    fn sample_table_location() -> Location {
        "s3://warehouse/ns/table".parse().unwrap()
    }

    #[tokio::test]
    async fn default_engine_normalizes_initialize_requests() {
        let backend = Arc::new(RecordingBackend::default());
        *backend.initialize_result.lock().await = Some(Ok(InitializedPaimonTable {
            metadata_location: Some("s3://warehouse/ns/table/metadata.json".parse().unwrap()),
            current_snapshot_id: Some(8),
            logical_schema: sample_logical_schema(LogicalPrimitiveType::Long),
            normalized_options: HashMap::from([
                ("bucket".to_string(), "7".to_string()),
                ("write-mode".to_string(), "change-log".to_string()),
            ]),
        }));

        let engine = DefaultPaimonEngine::new(backend.clone());
        let request = InitializePaimonTableRequest {
            warehouse_id: WarehouseId::new_random(),
            namespace: NamespaceIdent::from_vec(vec!["sales".to_string()]).unwrap(),
            table_name: "orders".to_string(),
            location: sample_table_location(),
            logical_schema: sample_logical_schema(LogicalPrimitiveType::Long),
            table_options: HashMap::from([
                ("  Bucket ".to_string(), "7".to_string()),
                ("WRITE-Mode".to_string(), "change-log".to_string()),
            ]),
            primary_keys: vec!["id".to_string()],
            partition_keys: vec!["dt".to_string()],
            comment: Some("orders table".to_string()),
        };

        let response = engine.initialize_table(request).await.unwrap();

        let recorded = backend.initialize_requests.lock().await;
        let initialize = recorded
            .first()
            .expect("initialize request must be recorded");
        assert_eq!(
            initialize.normalized_options,
            HashMap::from([
                ("bucket".to_string(), "7".to_string()),
                ("write-mode".to_string(), "change-log".to_string()),
            ])
        );
        assert_eq!(
            initialize.engine_schema,
            sample_engine_schema(PaimonEnginePrimitiveType::Long)
        );
        drop(recorded);

        assert_eq!(response.current_snapshot_id, Some(8));
        assert_eq!(
            response.normalized_options,
            HashMap::from([
                ("bucket".to_string(), "7".to_string()),
                ("write-mode".to_string(), "change-log".to_string()),
            ])
        );
    }

    #[tokio::test]
    async fn default_engine_rejects_unsupported_initialize_schema() {
        let engine = DefaultPaimonEngine::new(Arc::new(RecordingBackend::default()));
        let request = InitializePaimonTableRequest {
            warehouse_id: WarehouseId::new_random(),
            namespace: NamespaceIdent::from_vec(vec!["sales".to_string()]).unwrap(),
            table_name: "orders".to_string(),
            location: sample_table_location(),
            logical_schema: LogicalSchema {
                schema_id: 1,
                root_fields: vec![LogicalField {
                    field_id: 1,
                    name: "id".to_string(),
                    required: true,
                    doc: None,
                    field_type: LogicalType::Primitive(LogicalPrimitiveType::Uuid),
                    initial_default: None,
                    write_default: None,
                    is_identity_hint: true,
                }],
            },
            table_options: HashMap::new(),
            primary_keys: vec!["id".to_string()],
            partition_keys: Vec::new(),
            comment: None,
        };

        let error = engine.initialize_table(request).await.unwrap_err();
        assert!(matches!(error, PaimonEngineError::UnsupportedSchema { .. }));
    }

    #[tokio::test]
    async fn default_engine_normalizes_loaded_table_response() {
        let backend = Arc::new(RecordingBackend::default());
        *backend.load_result.lock().await = Some(Ok(LoadedPaimonEngineTable {
            metadata_location: Some("s3://warehouse/ns/table/metadata-v2.json".parse().unwrap()),
            current_snapshot_id: Some(9),
            current_branch: "main".to_string(),
            logical_schema: sample_logical_schema(LogicalPrimitiveType::Long),
            normalized_options: HashMap::from([
                (" write.mode ".to_string(), " append ".to_string()),
                ("bucket".to_string(), " 8 ".to_string()),
            ]),
            primary_keys: vec!["id".to_string()],
            partition_keys: vec!["dt".to_string()],
            comment: Some("loaded table".to_string()),
        }));

        let engine = DefaultPaimonEngine::new(backend.clone());
        let request = LoadPaimonEngineTableRequest {
            warehouse_id: WarehouseId::new_random(),
            table_location: sample_table_location(),
            metadata_location: Some("s3://warehouse/ns/table/metadata-v1.json".parse().unwrap()),
        };

        let response = engine.load_table(request.clone()).await.unwrap();

        let recorded = backend.load_requests.lock().await;
        assert_eq!(
            recorded.as_slice(),
            &[LoadPaimonEngineTableBackendRequest {
                warehouse_id: request.warehouse_id,
                table_location: request.table_location,
                metadata_location: request.metadata_location,
            }]
        );
        drop(recorded);

        assert_eq!(
            response.logical_schema,
            sample_logical_schema(LogicalPrimitiveType::Long)
        );
        assert_eq!(
            response.normalized_options,
            HashMap::from([
                ("write.mode".to_string(), "append".to_string()),
                ("bucket".to_string(), "8".to_string()),
            ])
        );
    }

    #[tokio::test]
    async fn default_engine_translates_alter_requests_and_responses() {
        let backend = Arc::new(RecordingBackend::default());
        *backend.alter_result.lock().await = Some(Ok(AlteredPaimonEngineTable {
            metadata_location: Some("s3://warehouse/ns/table/metadata-v3.json".parse().unwrap()),
            current_snapshot_id: Some(12),
            logical_schema: sample_logical_schema(LogicalPrimitiveType::Long),
            normalized_options: HashMap::from([("bucket".to_string(), "9".to_string())]),
            primary_keys: vec!["id".to_string()],
            partition_keys: vec!["dt".to_string()],
            comment: Some("altered".to_string()),
        }));

        let engine = DefaultPaimonEngine::new(backend.clone());
        let request = AlterPaimonEngineTableRequest {
            warehouse_id: WarehouseId::new_random(),
            tabular_id: TableId::new_random(),
            table_location: sample_table_location(),
            metadata_location: Some("s3://warehouse/ns/table/metadata-v2.json".parse().unwrap()),
            current_snapshot_id: Some(11),
            logical_schema: sample_logical_schema(LogicalPrimitiveType::Long),
            table_options: HashMap::from([(" Bucket ".to_string(), " 9 ".to_string())]),
            primary_keys: vec!["id".to_string()],
            partition_keys: vec!["dt".to_string()],
            comment: Some("alter".to_string()),
        };

        let response = engine.alter_table(request.clone()).await.unwrap();

        let recorded = backend.alter_requests.lock().await;
        assert_eq!(
            recorded.as_slice(),
            &[AlterPaimonEngineTableBackendRequest {
                warehouse_id: request.warehouse_id,
                tabular_id: request.tabular_id,
                table_location: request.table_location,
                metadata_location: request.metadata_location,
                current_snapshot_id: request.current_snapshot_id,
                engine_schema: sample_engine_schema(PaimonEnginePrimitiveType::Long),
                normalized_options: HashMap::from([("bucket".to_string(), "9".to_string())]),
                primary_keys: vec!["id".to_string()],
                partition_keys: vec!["dt".to_string()],
                comment: Some("alter".to_string()),
            }]
        );
        drop(recorded);

        assert_eq!(response.current_snapshot_id, Some(12));
        assert_eq!(response.normalized_options["bucket"], "9");
    }

    #[tokio::test]
    async fn default_engine_passes_prepare_publish_and_cleanup_through() {
        let backend = Arc::new(RecordingBackend::default());
        let tabular_id = TableId::new_random();
        let warehouse_id = WarehouseId::new_random();
        let commit_token = Uuid::new_v4();
        let staged_metadata_location = Some(
            "s3://warehouse/ns/table/staged/metadata-v4.json"
                .parse()
                .unwrap(),
        );

        *backend.prepare_result.lock().await = Some(Ok(PreparedPaimonCommit {
            commit_token,
            staged_metadata_location: staged_metadata_location.clone(),
            next_snapshot_id: Some(13),
        }));
        *backend.publish_result.lock().await = Some(Err(PaimonEngineError::publish_failed(
            PaimonPublishFailureClass::Retriable,
            "retry publish",
        )));
        *backend.cleanup_result.lock().await = Some(Ok(()));

        let engine = DefaultPaimonEngine::new(backend.clone());
        let operations = vec![json!({"kind": "append"}), json!({"kind": "compact"})];
        let prepare = engine
            .prepare_commit(PreparePaimonCommitRequest {
                warehouse_id,
                tabular_id,
                table_location: sample_table_location(),
                metadata_location: Some(
                    "s3://warehouse/ns/table/metadata-v3.json".parse().unwrap(),
                ),
                current_snapshot_id: Some(12),
                operations: operations.clone(),
                expected_current_snapshot_id: Some(12),
            })
            .await
            .unwrap();
        assert_eq!(prepare.commit_token, commit_token);
        assert_eq!(prepare.next_snapshot_id, Some(13));

        let publish_error = engine
            .publish_commit(PublishPaimonCommitRequest {
                warehouse_id,
                tabular_id,
                commit_token,
                staged_metadata_location: staged_metadata_location.clone(),
                current_metadata_location: Some(
                    "s3://warehouse/ns/table/metadata-v3.json".parse().unwrap(),
                ),
            })
            .await
            .unwrap_err();
        assert!(publish_error.is_retriable());

        engine
            .cleanup_staged_commit(CleanupStagedPaimonCommitRequest {
                warehouse_id,
                tabular_id,
                commit_token,
                staged_metadata_location: staged_metadata_location.clone(),
            })
            .await
            .unwrap();

        let prepare_requests = backend.prepare_requests.lock().await;
        assert_eq!(prepare_requests.len(), 1);
        assert_eq!(prepare_requests[0].operations, operations);
        drop(prepare_requests);

        let publish_requests = backend.publish_requests.lock().await;
        assert_eq!(publish_requests.len(), 1);
        assert_eq!(publish_requests[0].commit_token, commit_token);
        drop(publish_requests);

        let cleanup_requests = backend.cleanup_requests.lock().await;
        assert_eq!(cleanup_requests.len(), 1);
        assert_eq!(
            cleanup_requests[0].staged_metadata_location,
            staged_metadata_location
        );
    }

    #[tokio::test]
    async fn unavailable_engine_factory_returns_dyn_engine() {
        let engine = unavailable_paimon_engine();
        let err = engine
            .load_table(LoadPaimonEngineTableRequest {
                warehouse_id: WarehouseId::new_random(),
                table_location: sample_table_location(),
                metadata_location: None,
            })
            .await
            .unwrap_err();

        assert!(matches!(err, PaimonEngineError::EngineUnavailable { .. }));
    }

    #[tokio::test]
    async fn default_engine_factory_uses_current_feature_set() {
        let engine = default_paimon_engine();
        let err = engine
            .load_table(LoadPaimonEngineTableRequest {
                warehouse_id: WarehouseId::new_random(),
                table_location: sample_table_location(),
                metadata_location: None,
            })
            .await
            .unwrap_err();

        if cfg!(feature = "paimon-engine") {
            assert_eq!(err, native_backend_error());
        } else {
            assert!(matches!(err, PaimonEngineError::EngineUnavailable { .. }));
            assert_eq!(
                err.to_string(),
                PaimonEngineError::engine_unavailable("paimon-rust integration is not configured")
                    .to_string()
            );
        }
    }
}
