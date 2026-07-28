//! Test helpers for the FoundationDB backend scaffold.
//!
//! Stage 1 intentionally keeps this empty until the backend binds to a real
//! FoundationDB client and can expose reusable fixtures.

use crate::{CatalogState, FoundationDbConfig};

#[must_use]
pub fn test_state() -> CatalogState {
    CatalogState::from_config(FoundationDbConfig::default())
}
