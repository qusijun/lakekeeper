use std::collections::HashMap;

use async_trait::async_trait;
use axum::{
    Extension, Json, Router,
    extract::{Path, Query, RawQuery, State},
    routing::{get, post},
};
use http::{HeaderMap, StatusCode};
#[cfg(feature = "open-api")]
use iceberg_ext::catalog::rest::IcebergErrorResponse;
use iceberg_ext::catalog::rest::StorageCredential;
use serde::{Deserialize, Serialize};

#[cfg(feature = "open-api")]
use crate::api::endpoints::PaimonTableV1Endpoint;
use crate::{
    api::{
        ApiContext, Result,
        iceberg::{
            types::{DropParams, Prefix, ReferencedByQuery, ReferencingView},
            v1::{
                DataAccess, DataAccessMode,
                namespace::{NamespaceIdentUrl, NamespaceParameters},
                tables::{parse_data_access, parse_referenced_by_param},
            },
        },
    },
    request_metadata::RequestMetadata,
    service::{LogicalSchema, PaimonCommitState, TableId},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct CreatePaimonTableRequest {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,
    #[serde(default = "default_branch")]
    pub current_branch: String,
    #[cfg_attr(feature = "open-api", schema(value_type = Option<serde_json::Value>))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<LogicalSchema>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub table_options: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

fn default_branch() -> String {
    "main".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct PaimonTableData {
    pub name: String,
    pub location: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_location: Option<String>,
    pub protected: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,
    pub current_branch: String,
    #[cfg_attr(feature = "open-api", schema(value_type = Option<serde_json::Value>))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<LogicalSchema>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub table_options: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub commit_state: PaimonCommitState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_commit_token: Option<uuid::Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_commit_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct LoadPaimonTableResponse {
    pub table: PaimonTableData,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_credentials: Option<Vec<StorageCredential>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct PaimonTableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "open-api", schema(value_type = Option<uuid::Uuid>))]
    pub id: Option<TableId>,
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub commit_state: PaimonCommitState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct ListPaimonTablesResponse {
    pub identifiers: Vec<PaimonTableIdentifier>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "open-api", derive(utoipa::IntoParams))]
#[serde(rename_all = "camelCase")]
pub struct ListPaimonTablesQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct LoadPaimonTableCredentialsQuery {
    pub referenced_by: Option<ReferencedByQuery>,
}

impl<'de> serde::Deserialize<'de> for LoadPaimonTableCredentialsQuery {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};

        struct V;

        impl Visitor<'_> for V {
            type Value = LoadPaimonTableCredentialsQuery;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a string containing query parameters")
            }

            fn visit_str<E>(self, s: &str) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(LoadPaimonTableCredentialsQuery {
                    referenced_by: parse_referenced_by_param(s),
                })
            }
        }

        deserializer.deserialize_str(V)
    }
}

#[derive(Debug, Clone, PartialEq, Default, typed_builder::TypedBuilder)]
pub struct LoadPaimonTableCredentialsRequest {
    #[builder(default)]
    pub referenced_by: Option<Vec<ReferencingView>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct LoadPaimonTableCredentialsResponse {
    pub storage_credentials: Vec<StorageCredential>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "open-api", derive(utoipa::ToSchema))]
#[serde(rename_all = "kebab-case")]
pub struct UpdatePaimonCommitStateRequest {
    #[cfg_attr(feature = "open-api", schema(value_type = String))]
    pub commit_state: PaimonCommitState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_commit_token: Option<uuid::Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_snapshot_id: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_commit_error: Option<String>,
}

impl axum::response::IntoResponse for LoadPaimonTableCredentialsResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for LoadPaimonTableResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

impl axum::response::IntoResponse for ListPaimonTablesResponse {
    fn into_response(self) -> axum::response::Response {
        axum::Json(self).into_response()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PaimonTableParameters {
    pub prefix: Option<Prefix>,
    pub namespace: iceberg::NamespaceIdent,
    pub table_name: String,
}

#[async_trait]
pub trait PaimonTableService<S: crate::api::ThreadSafe>
where
    Self: Send + Sync + 'static,
{
    async fn create_paimon_table(
        parameters: NamespaceParameters,
        request: CreatePaimonTableRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadPaimonTableResponse>;

    async fn load_paimon_table(
        parameters: PaimonTableParameters,
        state: ApiContext<S>,
        data_access: impl Into<DataAccessMode> + Send,
        request_metadata: RequestMetadata,
    ) -> Result<LoadPaimonTableResponse>;

    async fn load_paimon_table_credentials(
        parameters: PaimonTableParameters,
        request: LoadPaimonTableCredentialsRequest,
        data_access: DataAccess,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadPaimonTableCredentialsResponse>;

    async fn list_paimon_tables(
        parameters: NamespaceParameters,
        query: ListPaimonTablesQuery,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<ListPaimonTablesResponse>;

    async fn drop_paimon_table(
        parameters: PaimonTableParameters,
        drop_params: DropParams,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<()>;

    async fn update_paimon_commit_state(
        parameters: PaimonTableParameters,
        request: UpdatePaimonCommitStateRequest,
        state: ApiContext<S>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadPaimonTableResponse>;
}

#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "paimon-table",
    path = PaimonTableV1Endpoint::CreatePaimonTable.path(),
    params(("prefix" = String,), ("namespace" = String,)),
    request_body = CreatePaimonTableRequest,
    responses(
        (status = 200, body = LoadPaimonTableResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn create_paimon_table<I: PaimonTableService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<CreatePaimonTableRequest>,
) -> Result<LoadPaimonTableResponse> {
    I::create_paimon_table(
        NamespaceParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
        },
        request,
        api_context,
        metadata,
    )
    .await
}

#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "paimon-table",
    path = PaimonTableV1Endpoint::ListPaimonTables.path(),
    params(("prefix" = String,), ("namespace" = String,), ListPaimonTablesQuery),
    responses(
        (status = 200, body = ListPaimonTablesResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn list_paimon_tables<I: PaimonTableService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace)): Path<(Prefix, NamespaceIdentUrl)>,
    Query(query): Query<ListPaimonTablesQuery>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<ListPaimonTablesResponse> {
    I::list_paimon_tables(
        NamespaceParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
        },
        query,
        api_context,
        metadata,
    )
    .await
}

#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "paimon-table",
    path = PaimonTableV1Endpoint::LoadPaimonTable.path(),
    params(("prefix" = String,), ("namespace" = String,), ("table" = String,)),
    responses(
        (status = 200, body = LoadPaimonTableResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn load_paimon_table<I: PaimonTableService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
    State(api_context): State<ApiContext<S>>,
    headers: HeaderMap,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<LoadPaimonTableResponse> {
    I::load_paimon_table(
        PaimonTableParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            table_name: table,
        },
        api_context,
        parse_data_access(&headers),
        metadata,
    )
    .await
}

#[cfg_attr(feature = "open-api", utoipa::path(
    delete,
    tag = "paimon-table",
    path = PaimonTableV1Endpoint::DropPaimonTable.path(),
    params(("prefix" = String,), ("namespace" = String,), ("table" = String,)),
    responses(
        (status = 204, description = "Paimon table dropped successfully"),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn drop_paimon_table<I: PaimonTableService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
    Query(drop_params): Query<DropParams>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<StatusCode> {
    I::drop_paimon_table(
        PaimonTableParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            table_name: table,
        },
        drop_params,
        api_context,
        metadata,
    )
    .await
    .map(|()| StatusCode::NO_CONTENT)
}

#[cfg_attr(feature = "open-api", utoipa::path(
    get,
    tag = "paimon-table",
    path = PaimonTableV1Endpoint::LoadPaimonTableCredentials.path(),
    params(
        ("prefix" = String,),
        ("namespace" = String,),
        ("table" = String,),
        ("referenced-by" = Option<String>, Query),
    ),
    responses(
        (status = 200, body = LoadPaimonTableCredentialsResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn load_paimon_table_credentials<I: PaimonTableService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
    State(api_context): State<ApiContext<S>>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
    Extension(metadata): Extension<RequestMetadata>,
) -> Result<LoadPaimonTableCredentialsResponse> {
    let load_credentials_query = raw_query
        .as_deref()
        .and_then(|q| {
            use serde::de::{IntoDeserializer, value::StrDeserializer};
            let deserializer: StrDeserializer<'_, serde::de::value::Error> = q.into_deserializer();
            LoadPaimonTableCredentialsQuery::deserialize(deserializer)
                .map_err(|e| {
                    tracing::warn!("Failed to parse load paimon table credentials query: {e}");
                    e
                })
                .ok()
        })
        .unwrap_or_default();

    let data_access = match parse_data_access(&headers) {
        DataAccessMode::ClientManaged => DataAccess::not_specified(),
        DataAccessMode::ServerDelegated(da) => da,
    };

    I::load_paimon_table_credentials(
        PaimonTableParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            table_name: table,
        },
        LoadPaimonTableCredentialsRequest {
            referenced_by: load_credentials_query
                .referenced_by
                .map(ReferencedByQuery::into_inner),
        },
        data_access,
        api_context,
        metadata,
    )
    .await
}

#[cfg_attr(feature = "open-api", utoipa::path(
    post,
    tag = "paimon-table",
    path = PaimonTableV1Endpoint::UpdatePaimonCommitState.path(),
    params(("prefix" = String,), ("namespace" = String,), ("table" = String,)),
    request_body = UpdatePaimonCommitStateRequest,
    responses(
        (status = 200, body = LoadPaimonTableResponse),
        (status = "4XX", body = IcebergErrorResponse),
    ),
))]
async fn update_paimon_commit_state<I: PaimonTableService<S>, S: crate::api::ThreadSafe>(
    Path((prefix, namespace, table)): Path<(Prefix, NamespaceIdentUrl, String)>,
    State(api_context): State<ApiContext<S>>,
    Extension(metadata): Extension<RequestMetadata>,
    Json(request): Json<UpdatePaimonCommitStateRequest>,
) -> Result<LoadPaimonTableResponse> {
    I::update_paimon_commit_state(
        PaimonTableParameters {
            prefix: Some(prefix),
            namespace: namespace.into(),
            table_name: table,
        },
        request,
        api_context,
        metadata,
    )
    .await
}

pub fn router<I: PaimonTableService<S>, S: crate::api::ThreadSafe>() -> Router<ApiContext<S>> {
    Router::new()
        .route(
            "/{prefix}/namespaces/{namespace}/paimon-tables",
            post(create_paimon_table::<I, S>).get(list_paimon_tables::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/paimon-tables/{table}",
            get(load_paimon_table::<I, S>).delete(drop_paimon_table::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/paimon-tables/{table}/credentials",
            get(load_paimon_table_credentials::<I, S>),
        )
        .route(
            "/{prefix}/namespaces/{namespace}/paimon-tables/{table}/commit-state",
            post(update_paimon_commit_state::<I, S>),
        )
}

#[cfg(feature = "open-api")]
mod openapi;
#[cfg(feature = "open-api")]
pub use openapi::api_doc;

#[cfg(test)]
mod tests {
    use serde::de::{IntoDeserializer, value::StrDeserializer};
    use serde::Deserialize;

    use super::{
        CreatePaimonTableRequest, LoadPaimonTableCredentialsQuery, UpdatePaimonCommitStateRequest,
    };
    use crate::{
        api::iceberg::types::ReferencedByQuery,
        service::PaimonCommitState,
    };

    #[test]
    fn create_paimon_table_request_defaults_current_branch_to_main() {
        let request: CreatePaimonTableRequest = serde_json::from_value(serde_json::json!({
            "name": "orders",
        }))
        .unwrap();

        assert_eq!(request.current_branch, "main");
        assert!(request.table_options.is_empty());
        assert!(request.partition_keys.is_empty());
        assert!(request.primary_keys.is_empty());
    }

    #[test]
    fn load_paimon_table_credentials_query_deserializes_referenced_by_chain() {
        let query =
            "referenced-by=prod%1Fanalytics%1Fquarterly_view,prod%1Fanalytics%1Fmonthly_view";
        let query_deserializer: StrDeserializer<'_, serde::de::value::Error> =
            query.into_deserializer();
        let deserialized_query: LoadPaimonTableCredentialsQuery =
            LoadPaimonTableCredentialsQuery::deserialize(query_deserializer).unwrap();

        assert_eq!(
            deserialized_query,
            LoadPaimonTableCredentialsQuery {
                referenced_by: Some(ReferencedByQuery::from(vec![
                    iceberg::TableIdent::from_strs(vec!["prod", "analytics", "quarterly_view"])
                        .unwrap(),
                    iceberg::TableIdent::from_strs(vec!["prod", "analytics", "monthly_view"])
                        .unwrap(),
                ])),
            }
        );
    }

    #[test]
    fn update_paimon_commit_state_request_uses_kebab_case_fields() {
        let pending_commit_token = uuid::Uuid::now_v7();
        let request: UpdatePaimonCommitStateRequest =
            serde_json::from_value(serde_json::json!({
                "commit-state": "pending-publish",
                "pending-commit-token": pending_commit_token,
                "current-snapshot-id": 42,
                "metadata-location": "s3://warehouse/ns/orders/metadata/0001.json",
                "last-commit-error": "temporary conflict",
            }))
            .unwrap();

        assert_eq!(request.commit_state, PaimonCommitState::PendingPublish);
        assert_eq!(request.pending_commit_token, Some(pending_commit_token));
        assert_eq!(request.current_snapshot_id, Some(42));
        assert_eq!(
            request.metadata_location.as_deref(),
            Some("s3://warehouse/ns/orders/metadata/0001.json")
        );
        assert_eq!(
            request.last_commit_error.as_deref(),
            Some("temporary conflict")
        );
    }
}
