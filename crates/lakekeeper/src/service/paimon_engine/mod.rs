use std::fmt;

use async_trait::async_trait;

mod default;
mod unavailable;
pub use default::DefaultPaimonEngine;
pub use unavailable::UnavailablePaimonEngine;

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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;
    use iceberg::NamespaceIdent;
    use tokio::sync::Mutex;

    use super::{
        DefaultPaimonEngine, InitializePaimonTableRequest, InitializedPaimonTable, PaimonEngine,
        PaimonEngineError, PaimonEngineField, PaimonEnginePrimitiveType, PaimonEngineSchema,
        PaimonEngineType, PaimonPublishFailureClass, PublishedPaimonCommit,
    };
    use crate::service::{
        LogicalField, LogicalPrimitiveType, LogicalSchema, LogicalType, WarehouseId,
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
            _request: LoadPaimonEngineTableBackendRequest,
        ) -> Result<super::LoadedPaimonEngineTable, PaimonEngineError> {
            unimplemented!()
        }

        async fn alter_table(
            &self,
            _request: AlterPaimonEngineTableBackendRequest,
        ) -> Result<super::AlteredPaimonEngineTable, PaimonEngineError> {
            unimplemented!()
        }

        async fn prepare_commit(
            &self,
            _request: PreparePaimonCommitBackendRequest,
        ) -> Result<super::PreparedPaimonCommit, PaimonEngineError> {
            unimplemented!()
        }

        async fn publish_commit(
            &self,
            _request: PublishPaimonCommitBackendRequest,
        ) -> Result<PublishedPaimonCommit, PaimonEngineError> {
            unimplemented!()
        }

        async fn cleanup_staged_commit(
            &self,
            _request: CleanupStagedPaimonCommitBackendRequest,
        ) -> Result<(), PaimonEngineError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn default_engine_normalizes_initialize_requests() {
        let backend = Arc::new(RecordingBackend::default());
        *backend.initialize_result.lock().await = Some(Ok(InitializedPaimonTable {
            metadata_location: Some("s3://warehouse/ns/table/metadata.json".parse().unwrap()),
            current_snapshot_id: Some(8),
            logical_schema: LogicalSchema {
                schema_id: 7,
                root_fields: vec![LogicalField {
                    field_id: 10,
                    name: "id".to_string(),
                    required: true,
                    doc: Some("identifier".to_string()),
                    field_type: LogicalType::Primitive(LogicalPrimitiveType::Long),
                    initial_default: None,
                    write_default: None,
                    is_identity_hint: true,
                }],
            },
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
            location: "s3://warehouse/ns/table".parse().unwrap(),
            logical_schema: LogicalSchema {
                schema_id: 7,
                root_fields: vec![LogicalField {
                    field_id: 10,
                    name: "id".to_string(),
                    required: true,
                    doc: Some("identifier".to_string()),
                    field_type: LogicalType::Primitive(LogicalPrimitiveType::Long),
                    initial_default: None,
                    write_default: None,
                    is_identity_hint: true,
                }],
            },
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
            PaimonEngineSchema {
                schema_id: 7,
                root_fields: vec![PaimonEngineField {
                    field_id: 10,
                    name: "id".to_string(),
                    required: true,
                    doc: Some("identifier".to_string()),
                    field_type: PaimonEngineType::Primitive(PaimonEnginePrimitiveType::Long),
                    initial_default: None,
                    write_default: None,
                    is_primary_key: true,
                }],
            }
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
            location: "s3://warehouse/ns/table".parse().unwrap(),
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
}
