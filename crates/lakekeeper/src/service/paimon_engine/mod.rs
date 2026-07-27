use std::fmt;

use async_trait::async_trait;

mod unavailable;
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
    use super::{PaimonEngineError, PaimonPublishFailureClass};

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
}
