# FoundationDB Backend Stage 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a selectable FoundationDB catalog backend scaffold while keeping PostgreSQL as the default backend.

**Architecture:** Introduce a new `lakekeeper-storage-foundationdb` crate with config, state, transaction, and backend types; route `lakekeeper-bin` through a backend selector instead of hard-coding PostgreSQL. Stage 1 is correctness-first and intentionally leaves unsupported catalog capabilities explicit.

**Tech Stack:** Rust, Cargo workspace crates, Lakekeeper `CatalogStore`, Figment config, FoundationDB client crate, existing PostgreSQL backend for default behavior.

---

## File Structure

- Create: `crates/lakekeeper-storage-foundationdb/Cargo.toml`
- Create: `crates/lakekeeper-storage-foundationdb/src/lib.rs`
- Create: `crates/lakekeeper-storage-foundationdb/src/config.rs`
- Create: `crates/lakekeeper-storage-foundationdb/src/state.rs`
- Create: `crates/lakekeeper-storage-foundationdb/src/tx.rs`
- Create: `crates/lakekeeper-storage-foundationdb/src/catalog.rs`
- Create: `crates/lakekeeper-storage-foundationdb/src/error.rs`
- Create: `crates/lakekeeper-storage-foundationdb/src/test_utils.rs`
- Modify: `Cargo.toml`
- Modify: `crates/lakekeeper-bin/Cargo.toml`
- Modify: `crates/lakekeeper-bin/src/config.rs`
- Modify: `crates/lakekeeper-bin/src/main.rs`
- Modify: `crates/lakekeeper-bin/src/serve.rs`
- Modify: `crates/lakekeeper-bin/src/wait_for_db.rs`
- Test: crate-local unit tests in the new FDB crate and binary config tests in `crates/lakekeeper-bin/src/config.rs`

### Task 1: Add Workspace and Binary Dependencies

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/lakekeeper-bin/Cargo.toml`
- Create: `crates/lakekeeper-storage-foundationdb/Cargo.toml`

- [ ] Add the new workspace member and the minimal dependency entries for the FDB backend crate.
- [ ] Add `lakekeeper-storage-foundationdb` as a dependency of `lakekeeper-bin`.
- [ ] Add the FDB client crate and shared workspace deps needed for config/state/health wiring.
- [ ] Run: `cargo metadata --no-deps`
- [ ] Expected: command succeeds and the new crate appears in the workspace graph.

### Task 2: Create the FDB Crate Skeleton

**Files:**
- Create: `crates/lakekeeper-storage-foundationdb/src/lib.rs`
- Create: `crates/lakekeeper-storage-foundationdb/src/error.rs`
- Create: `crates/lakekeeper-storage-foundationdb/src/config.rs`
- Create: `crates/lakekeeper-storage-foundationdb/src/state.rs`
- Create: `crates/lakekeeper-storage-foundationdb/src/tx.rs`
- Create: `crates/lakekeeper-storage-foundationdb/src/catalog.rs`

- [ ] Define `FoundationDbBackend`, `CatalogState`, `FoundationDbTransaction`, and exported config types.
- [ ] Wire a minimal health implementation and transaction trait implementation.
- [ ] Make unsupported catalog methods return explicit backend-not-supported errors instead of `todo!()`.
- [ ] Run: `cargo check -p lakekeeper-storage-foundationdb`
- [ ] Expected: the crate compiles on its own.

### Task 3: Add Runtime Backend Selection Config

**Files:**
- Modify: `crates/lakekeeper-bin/src/config.rs`

- [ ] Add a catalog backend enum and binary config fields for selecting `postgres` or `foundationdb`.
- [ ] Add FDB-specific config fields for cluster file, tenant, prefix, API version, and retry settings.
- [ ] Add or extend config parsing tests for both backend variants and default behavior.
- [ ] Run: `cargo test -p lakekeeper-bin config::tests -- --nocapture`
- [ ] Expected: tests pass and default backend remains PostgreSQL.

### Task 4: Route Serve Through a Backend Selector

**Files:**
- Modify: `crates/lakekeeper-bin/src/serve.rs`

- [ ] Extract PostgreSQL-specific startup into a dedicated path.
- [ ] Add a backend selector that dispatches to PostgreSQL or FoundationDB.
- [ ] For the FoundationDB path, return a clear unsupported/placeholder startup error until catalog methods exist.
- [ ] Run: `cargo check -p lakekeeper-bin`
- [ ] Expected: binary compiles with both backend code paths present.

### Task 5: Route Command-Line Helpers Through the Selector

**Files:**
- Modify: `crates/lakekeeper-bin/src/main.rs`
- Modify: `crates/lakekeeper-bin/src/wait_for_db.rs`

- [ ] Keep PostgreSQL-only commands explicit where they are DB-specific, especially migrations.
- [ ] Make `wait-for-db` either dispatch by backend or fail fast with a backend-specific message for unsupported FDB paths.
- [ ] Ensure `serve` uses the selector introduced in Task 4.
- [ ] Run: `cargo check -p lakekeeper-bin`
- [ ] Expected: command handling compiles and unsupported FDB-only CLI paths are explicit.

### Task 6: Add FDB State and Config Unit Tests

**Files:**
- Create or modify: `crates/lakekeeper-storage-foundationdb/src/config.rs`
- Create or modify: `crates/lakekeeper-storage-foundationdb/src/state.rs`

- [ ] Add tests for config defaults and environment extraction.
- [ ] Add tests for root prefix or subspace derivation helpers that do not require a live FDB cluster.
- [ ] Run: `cargo test -p lakekeeper-storage-foundationdb --lib`
- [ ] Expected: unit tests pass without requiring external services.

### Task 7: Verify Workspace Build

**Files:**
- No file changes required beyond earlier tasks.

- [ ] Run: `cargo check`
- [ ] Expected: workspace compiles with the new crate and backend-selection wiring.
- [ ] Run: `cargo test -p lakekeeper-bin config::tests -p lakekeeper-storage-foundationdb --lib`
- [ ] Expected: targeted tests pass.

### Task 8: Follow-Up Scope Notes

**Files:**
- Modify: `docs/superpowers/specs/2026-07-28-foundationdb-backend-design.md` only if implementation findings force clarification.

- [ ] Capture any discovered constraints that affect stage-2 work, especially around unsupported commands, FDB crate selection, or `CatalogStore` method surfacing.
- [ ] Re-run: `cargo check`
- [ ] Expected: no regressions from documentation-only adjustments.

