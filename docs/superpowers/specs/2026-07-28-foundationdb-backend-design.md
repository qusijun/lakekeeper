# FoundationDB Backend Design

## Goal

Add a new `CatalogStore` backend based on FoundationDB that can support a first-stage Lakekeeper catalog flow without replacing the existing PostgreSQL backend.

## Scope

Stage 1 covers:

- server/bootstrap metadata
- project CRUD needed by the catalog
- warehouse CRUD and state/protection/managed-by updates
- namespace create/get/list/update/drop for non-recursive empty drops
- tabular common metadata
- table and view minimal create/load/list/rename/drop flows
- user exact lookup/upsert primitives
- role create/list/update and direct membership edges
- idempotency records
- runtime backend selection in `lakekeeper-bin`

Stage 1 explicitly does not try to match PostgreSQL for:

- full task queue support
- advanced search and fuzzy filtering
- statistics sink parity
- recursive namespace rename/drop flows
- every `CatalogStore` endpoint in one pass

## Non-Goals

- Replacing PostgreSQL as the default backend
- Preserving SQL-oriented implementations such as joins or server-side filtering
- Adding cross-replica cache invalidation beyond existing local invalidation rules

## Architectural Comparison

PostgreSQL currently provides:

- transactions
- unique constraints
- joins
- ordered scans with filtering
- batch mutation primitives

FoundationDB will provide:

- strict transactional reads and writes
- application-managed unique indexes
- explicit read models instead of joins
- keyset/range scans instead of offset-based pagination
- application-managed secondary indexes and summaries

The FDB backend therefore shifts correctness work from the database engine into the backend implementation. The main design rule is: every write updates the authoritative record and all affected secondary indexes in the same FoundationDB transaction.

## Backend Shape

Add a new crate:

- `crates/lakekeeper-storage-foundationdb`

Primary modules:

- `config.rs`: FDB-specific configuration
- `state.rs`: database handle, root subspace, health state
- `tx.rs`: transaction wrapper and retry helpers
- `codec.rs`: record encoding and decoding
- `keys.rs`: typed key builders and cursor helpers
- `catalog.rs`: `impl CatalogStore for FoundationDbBackend`
- per-domain modules:
  - `bootstrap.rs`
  - `project.rs`
  - `warehouse.rs`
  - `namespace.rs`
  - `tabular/`
  - `user.rs`
  - `role.rs`
  - `idempotency.rs`

`lakekeeper-bin` must stop assuming PostgreSQL is always the catalog backend. It should resolve backend selection from config and route startup, serve, and health commands through a backend-specific bootstrap layer.

## Data Model

FDB records use:

- tuple-encoded keys under a single root subspace
- `bincode` for primary records
- empty or lightweight values for secondary indexes

Every primary record stores:

- logical id
- `created_at`
- `updated_at`
- `version`

Important index patterns:

- primary record: `("entity", "by-id", id)`
- unique lookup: `("entity", "by-...", fields...) -> id`
- list index: `("entity", "list-...", ordered-fields..., id) -> summary`

## Keyspace

Stage 1 requires at least these key families:

- `server/by-id`
- `server/singleton`
- `project/by-id`
- `project/by-name`
- `project/list`
- `warehouse/by-id`
- `warehouse/by-project-name`
- `warehouse/list-by-project`
- `namespace/by-id`
- `namespace/by-warehouse-path`
- `namespace/list-by-parent`
- `tabular/by-id`
- `tabular/by-namespace-name`
- `tabular/list-by-namespace`
- `tabular/by-location`
- `table/by-id`
- `view/by-id`
- `user/by-id`
- `user/by-email`
- `role/by-id`
- `role/by-project-source`
- `role/list-by-project`
- `role-membership/by-parent`
- `role-membership/by-member-role`
- `user-role/by-user`
- `user-role/by-role`
- `idempotency/by-scope-key`

## Transaction Rules

The backend must preserve the project invariants from `AGENTS.md`:

- never switch to another connection/transaction mid-write
- read back updated state in the same transaction after writes
- never rely on local caches for cross-replica correctness

For FDB this becomes:

- each API write path uses a single FDB transaction
- all index maintenance is in that same transaction
- any response that needs updated state is built from the same transaction view

## Pagination

PostgreSQL-style offset behavior is not the target. Stage 1 uses keyset pagination:

- cursor stores the last logical index tuple
- cursor is versioned and base64 encoded
- each list endpoint scans from `first_greater_than(last_key)`

This keeps scans stable and avoids offset cost.

## Namespace Semantics

Namespaces are modeled as hierarchical paths:

- canonical path segments are stored in the record
- path uniqueness is enforced by `namespace/by-warehouse-path`
- child listing is driven by `namespace/list-by-parent`

Stage 1 supports empty namespace drops only. Recursive namespace mutation is deferred.

## Tabular Semantics

Use a shared `tabular` record for:

- name uniqueness within a namespace
- protection and deletion state
- listing and lookup

Use subtype records for:

- `table/by-id`
- `view/by-id`

Stage 1 keeps soft-delete behavior simple and correctness-first. Name release semantics must match the existing PostgreSQL behavior before destructive paths are completed.

## Role and Membership Semantics

Role membership is stored with explicit reverse indexes to support deletion and listing:

- role -> member edge
- reverse edge for role members
- user -> role edge
- role -> user edge

Stage 1 supports direct edges. Transitive closure precomputation is out of scope.

## Tasks, Search, and Stats

These are intentionally deferred:

- task queue parity is a separate phase
- advanced search should degrade to exact lookup or simple listing
- endpoint statistics can remain PostgreSQL-only or no-op during stage 1

The backend should return explicit unsupported errors where behavior is intentionally absent.

## Runtime Selection

`lakekeeper-bin` must support runtime catalog backend selection, for example:

- `LAKEKEEPER__CATALOG__BACKEND=postgres`
- `LAKEKEEPER__CATALOG__BACKEND=foundationdb`

PostgreSQL remains the default.

FDB-specific configuration should include:

- cluster file
- optional tenant
- root prefix
- API version
- retry limits and backoff
- transaction timeout

## Testing Strategy

Stage 1 testing should be layered:

- unit tests for key encoding, cursor encoding, and uniqueness helpers
- backend tests for state/config bootstrap and basic transaction wiring
- binary-level tests for backend selection config parsing

Do not attempt full backend-agnostic service parity in the first change. The first milestone is a compiling, selectable backend skeleton with correctness-oriented primitives.

## Stage 1 Deliverables

Stage 1 is complete when:

- the workspace builds with a new FDB storage crate
- `lakekeeper-bin` can parse and route catalog backend selection
- PostgreSQL remains the default path
- the FDB backend crate exposes `FoundationDbBackend`, `CatalogState`, and transaction/config primitives
- unsupported surfaces fail explicitly rather than silently behaving incorrectly

