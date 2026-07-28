use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use crate::{
    api::{
        ApiContext, ErrorModel,
        data::v1::paimon_tables::{
            LoadPaimonTableResponse, PaimonTableParameters, UpdatePaimonCommitStateRequest,
        },
    },
    request_metadata::RequestMetadata,
    server::{require_paimon_warehouse, require_warehouse_id},
    service::{
        AuthZTableInfo, CatalogPaimonOps, CatalogStore, PaimonCommitStateUpdate, Result,
        SecretStore, State, TableIdentOrId, Transaction,
        authz::{AuthZTableOps, Authorizer, CatalogTableAction},
        events::context::UserProvidedTable,
    },
};

pub(super) async fn update_paimon_commit_state<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: PaimonTableParameters,
    request: UpdatePaimonCommitStateRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadPaimonTableResponse> {
    let PaimonTableParameters {
        prefix,
        namespace,
        table_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let table_ident = iceberg::TableIdent::new(namespace.clone(), table_name.clone());
    let authorizer = state.v1_state.authz;

    let authz_result = authorizer
        .load_and_authorize_table_operation::<C>(
            &request_metadata,
            &UserProvidedTable {
                warehouse_id,
                table: TableIdentOrId::Ident(table_ident.clone()),
            },
            crate::service::TabularListFlags::active(),
            CatalogTableAction::Commit {
                updated_properties: Arc::new(BTreeMap::new()),
                removed_properties: Arc::new(Vec::new()),
            },
            state.v1_state.catalog.clone(),
        )
        .await;
    let (warehouse, _ns, table_info) = authz_result.map_err(super::authz_to_error_model)?;
    let _warehouse = require_paimon_warehouse(warehouse)?;
    let table_info = super::require_paimon_info(warehouse_id, &table_ident, table_info)?;

    let metadata_location = request
        .metadata_location
        .as_deref()
        .map(lakekeeper_io::Location::from_str)
        .transpose()
        .map_err(|e| ErrorModel::bad_request(e.to_string(), "InvalidMetadataLocation", None))?;

    let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
    let info = C::update_paimon_commit_state(
        warehouse_id,
        table_info.table_id(),
        PaimonCommitStateUpdate {
            commit_state: request.commit_state,
            pending_commit_token: request.pending_commit_token,
            current_snapshot_id: request.current_snapshot_id,
            metadata_location,
            last_commit_error: request.last_commit_error,
        },
        t.transaction(),
    )
    .await?;
    t.commit().await?;

    Ok(LoadPaimonTableResponse {
        table: super::paimon_data(&info),
        config: None,
        storage_credentials: None,
    })
}
