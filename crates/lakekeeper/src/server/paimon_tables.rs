mod create;
mod credentials;
mod drop;
mod list;
mod load;
mod update_commit_state;

use async_trait::async_trait;
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    api::{
        ApiContext,
        data::v1::paimon_tables::{
            CreatePaimonTableRequest, ListPaimonTablesQuery, ListPaimonTablesResponse,
            LoadPaimonTableCredentialsRequest, LoadPaimonTableCredentialsResponse,
            LoadPaimonTableResponse, PaimonTableParameters, PaimonTableService,
            UpdatePaimonCommitStateRequest,
        },
        iceberg::v1::{DataAccess, DataAccessMode, namespace::NamespaceParameters},
    },
    request_metadata::RequestMetadata,
    server::CatalogServer,
    service::{
        CatalogStore, IcebergErrorResponse, PaimonTableInfo, Result, SecretStore, State,
        WarehouseId,
        authz::{AuthZError, Authorizer},
        events::AuthorizationFailureSource,
    },
};

fn authz_to_error_model(e: AuthZError) -> IcebergErrorResponse {
    AuthorizationFailureSource::into_error_model(e).into()
}

fn authz_failure_to_error_model(
    e: impl AuthorizationFailureSource,
) -> IcebergErrorResponse {
    AuthorizationFailureSource::into_error_model(e).into()
}

fn require_paimon_info(
    warehouse_id: WarehouseId,
    table_ident: &iceberg::TableIdent,
    info: crate::service::TableInfo,
) -> std::result::Result<crate::service::TableInfo, IcebergErrorResponse> {
    if info.table_format == Some(crate::service::TableFormat::Paimon) {
        Ok(info)
    } else {
        Err(IcebergErrorResponse::from(
            ErrorModel::from(
                crate::service::PaimonTableNotFound::new()
                    .append_detail(format!("warehouse_id={warehouse_id}, table={table_ident}")),
            ),
        ))
    }
}

fn paimon_data(info: &PaimonTableInfo) -> crate::api::data::v1::paimon_tables::PaimonTableData {
    crate::api::data::v1::paimon_tables::PaimonTableData {
        name: info.name.clone(),
        location: info.location.to_string(),
        metadata_location: info.metadata_location.as_ref().map(ToString::to_string),
        protected: info.protected,
        current_snapshot_id: info.current_snapshot_id,
        current_branch: info.current_branch.clone(),
        schema: info.schema.clone(),
        table_options: info.table_options.clone(),
        partition_keys: info.partition_keys.clone(),
        primary_keys: info.primary_keys.clone(),
        comment: info.comment.clone(),
        commit_state: info.commit_state,
        pending_commit_token: info.pending_commit_token,
        last_commit_error: info.last_commit_error.clone(),
    }
}

#[async_trait]
impl<C: CatalogStore, A: Authorizer + Clone, S: SecretStore> PaimonTableService<State<A, C, S>>
    for CatalogServer<C, A, S>
{
    async fn create_paimon_table(
        parameters: NamespaceParameters,
        request: CreatePaimonTableRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadPaimonTableResponse> {
        create::create_paimon_table::<C, A, S>(parameters, request, state, request_metadata).await
    }

    async fn load_paimon_table(
        parameters: PaimonTableParameters,
        state: ApiContext<State<A, C, S>>,
        data_access: impl Into<DataAccessMode> + Send,
        request_metadata: RequestMetadata,
    ) -> Result<LoadPaimonTableResponse> {
        load::load_paimon_table::<C, A, S>(parameters, state, data_access, request_metadata).await
    }

    async fn load_paimon_table_credentials(
        parameters: PaimonTableParameters,
        request: LoadPaimonTableCredentialsRequest,
        data_access: DataAccess,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadPaimonTableCredentialsResponse> {
        credentials::load_paimon_table_credentials::<C, A, S>(
            parameters,
            request,
            data_access,
            state,
            request_metadata,
        )
        .await
    }

    async fn list_paimon_tables(
        parameters: NamespaceParameters,
        query: ListPaimonTablesQuery,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<ListPaimonTablesResponse> {
        list::list_paimon_tables::<C, A, S>(parameters, query, state, request_metadata).await
    }

    async fn drop_paimon_table(
        parameters: PaimonTableParameters,
        drop_params: crate::api::iceberg::types::DropParams,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<()> {
        drop::drop_paimon_table::<C, A, S>(parameters, drop_params, state, request_metadata).await
    }

    async fn update_paimon_commit_state(
        parameters: PaimonTableParameters,
        request: UpdatePaimonCommitStateRequest,
        state: ApiContext<State<A, C, S>>,
        request_metadata: RequestMetadata,
    ) -> Result<LoadPaimonTableResponse> {
        update_commit_state::update_paimon_commit_state::<C, A, S>(
            parameters,
            request,
            state,
            request_metadata,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr};

    use uuid::Uuid;

    use super::{paimon_data, require_paimon_info};
    use crate::{
        WarehouseId,
        service::{
            NamespaceId, PaimonCommitState, PaimonTableInfo, TableFormat, TableId, TableInfo,
            WarehouseVersion,
        },
    };

    fn paimon_table_info(warehouse_id: WarehouseId) -> PaimonTableInfo {
        let table_id = TableId::from(Uuid::now_v7());
        let namespace = iceberg::NamespaceIdent::new("analytics".to_string());
        let table_ident = iceberg::TableIdent::new(namespace.clone(), "orders".to_string());

        PaimonTableInfo {
            tabular_id: table_id,
            warehouse_id,
            warehouse_version: WarehouseVersion::from(0),
            namespace_id: NamespaceId::new_random(),
            namespace_version: 0.into(),
            namespace_ident: namespace,
            name: "orders".to_string(),
            tabular_ident: table_ident,
            location: lakekeeper_io::Location::from_str("s3://warehouse/analytics/orders").unwrap(),
            metadata_location: Some(
                lakekeeper_io::Location::from_str(
                    "s3://warehouse/analytics/orders/metadata/00001.json",
                )
                .unwrap(),
            ),
            protected: true,
            current_snapshot_id: Some(42),
            current_branch: "dev".to_string(),
            schema: None,
            table_options: HashMap::from([(String::from("bucket"), String::from("8"))]),
            partition_keys: vec!["dt".to_string()],
            primary_keys: vec!["id".to_string()],
            comment: Some("orders table".to_string()),
            commit_state: PaimonCommitState::PendingPublish,
            pending_commit_token: Some(Uuid::now_v7()),
            last_commit_error: Some("conflict".to_string()),
            updated_at: None,
        }
    }

    #[test]
    fn require_paimon_info_accepts_paimon_table_info() {
        let warehouse_id = WarehouseId::new_random();
        let mut info = TableInfo::new_random(warehouse_id);
        info.table_format = Some(TableFormat::Paimon);
        let table_ident = info.tabular_ident.clone();

        let resolved = require_paimon_info(warehouse_id, &table_ident, info.clone()).unwrap();

        assert_eq!(resolved, info);
    }

    #[test]
    fn require_paimon_info_rejects_non_paimon_table_info() {
        let warehouse_id = WarehouseId::new_random();
        let info = TableInfo::new_random(warehouse_id);
        let table_ident = info.tabular_ident.clone();

        let error = require_paimon_info(warehouse_id, &table_ident, info).unwrap_err();

        assert_eq!(error.error.r#type, "PaimonTableNotFound");
        assert!(
            error.error.message.contains("Paimon table not found"),
            "unexpected message: {}",
            error.error.message
        );
    }

    #[test]
    fn paimon_data_preserves_commit_state_and_storage_metadata() {
        let info = paimon_table_info(WarehouseId::new_random());

        let response = paimon_data(&info);

        assert_eq!(response.name, info.name);
        assert_eq!(response.location, info.location.to_string());
        assert_eq!(
            response.metadata_location,
            info.metadata_location.as_ref().map(ToString::to_string)
        );
        assert_eq!(response.current_snapshot_id, info.current_snapshot_id);
        assert_eq!(response.current_branch, info.current_branch);
        assert_eq!(response.table_options, info.table_options);
        assert_eq!(response.partition_keys, info.partition_keys);
        assert_eq!(response.primary_keys, info.primary_keys);
        assert_eq!(response.comment, info.comment);
        assert_eq!(response.commit_state, info.commit_state);
        assert_eq!(response.pending_commit_token, info.pending_commit_token);
        assert_eq!(response.last_commit_error, info.last_commit_error);
        assert!(response.protected);
    }
}
