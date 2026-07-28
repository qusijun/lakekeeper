use std::collections::HashMap;

use iceberg::NamespaceIdent;
use serde_json::Value;
use uuid::Uuid;

use crate::service::{Location, LogicalSchema, TableId, WarehouseId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializePaimonTableRequest {
    pub warehouse_id: WarehouseId,
    pub namespace: NamespaceIdent,
    pub table_name: String,
    pub location: Location,
    pub logical_schema: LogicalSchema,
    pub table_options: HashMap<String, String>,
    pub primary_keys: Vec<String>,
    pub partition_keys: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializedPaimonTable {
    pub metadata_location: Option<Location>,
    pub current_snapshot_id: Option<i64>,
    pub logical_schema: LogicalSchema,
    pub normalized_options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadPaimonEngineTableRequest {
    pub warehouse_id: WarehouseId,
    pub table_location: Location,
    pub metadata_location: Option<Location>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedPaimonEngineTable {
    pub metadata_location: Option<Location>,
    pub current_snapshot_id: Option<i64>,
    pub current_branch: String,
    pub logical_schema: LogicalSchema,
    pub normalized_options: HashMap<String, String>,
    pub primary_keys: Vec<String>,
    pub partition_keys: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPaimonEngineTableRequest {
    pub warehouse_id: WarehouseId,
    pub tabular_id: TableId,
    pub table_location: Location,
    pub metadata_location: Option<Location>,
    pub current_snapshot_id: Option<i64>,
    pub logical_schema: LogicalSchema,
    pub table_options: HashMap<String, String>,
    pub primary_keys: Vec<String>,
    pub partition_keys: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlteredPaimonEngineTable {
    pub metadata_location: Option<Location>,
    pub current_snapshot_id: Option<i64>,
    pub logical_schema: LogicalSchema,
    pub normalized_options: HashMap<String, String>,
    pub primary_keys: Vec<String>,
    pub partition_keys: Vec<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparePaimonCommitRequest {
    pub warehouse_id: WarehouseId,
    pub tabular_id: TableId,
    pub table_location: Location,
    pub metadata_location: Option<Location>,
    pub current_snapshot_id: Option<i64>,
    pub operations: Vec<Value>,
    pub expected_current_snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedPaimonCommit {
    pub commit_token: Uuid,
    pub staged_metadata_location: Option<Location>,
    pub next_snapshot_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishPaimonCommitRequest {
    pub warehouse_id: WarehouseId,
    pub tabular_id: TableId,
    pub commit_token: Uuid,
    pub staged_metadata_location: Option<Location>,
    pub current_metadata_location: Option<Location>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedPaimonCommit {
    pub current_snapshot_id: Option<i64>,
    pub metadata_location: Option<Location>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanupStagedPaimonCommitRequest {
    pub warehouse_id: WarehouseId,
    pub tabular_id: TableId,
    pub commit_token: Uuid,
    pub staged_metadata_location: Option<Location>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaimonEngineSchema {
    pub schema_id: i32,
    pub root_fields: Vec<PaimonEngineField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaimonEngineField {
    pub field_id: i32,
    pub name: String,
    pub required: bool,
    pub doc: Option<String>,
    pub field_type: PaimonEngineType,
    pub initial_default: Option<Value>,
    pub write_default: Option<Value>,
    pub is_primary_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaimonEngineType {
    Primitive(PaimonEnginePrimitiveType),
    Struct {
        fields: Vec<PaimonEngineField>,
    },
    List {
        element_field: Box<PaimonEngineField>,
    },
    Map {
        key_field: Box<PaimonEngineField>,
        value_field: Box<PaimonEngineField>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaimonEnginePrimitiveType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal { precision: u32, scale: u32 },
    Date,
    Time,
    Timestamp,
    String,
    Binary,
}
