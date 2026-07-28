#[derive(Debug, thiserror::Error)]
pub enum FoundationDbBackendError {
    #[error("FoundationDB catalog backend scaffold is not implemented yet: {operation}")]
    NotImplemented { operation: &'static str },
}

impl FoundationDbBackendError {
    #[must_use]
    pub fn not_implemented(operation: &'static str) -> Self {
        Self::NotImplemented { operation }
    }
}
