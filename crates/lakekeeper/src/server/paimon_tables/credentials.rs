use iceberg_ext::catalog::rest::StorageCredential;

use crate::{
    api::{
        ApiContext,
        data::v1::paimon_tables::{
            LoadPaimonTableCredentialsRequest, LoadPaimonTableCredentialsResponse,
            PaimonTableParameters,
        },
        iceberg::v1::{DataAccess, Result},
    },
    request_metadata::RequestMetadata,
    server::{maybe_get_secret, require_paimon_warehouse, require_warehouse_id},
    service::{
        AuthZTableInfo, CatalogPaimonOps, CatalogStore, SecretStore, State, Transaction,
        authz::Authorizer,
    },
};

pub(super) async fn load_paimon_table_credentials<
    C: CatalogStore,
    A: Authorizer + Clone,
    S: SecretStore,
>(
    parameters: PaimonTableParameters,
    request: LoadPaimonTableCredentialsRequest,
    data_access: DataAccess,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadPaimonTableCredentialsResponse> {
    let LoadPaimonTableCredentialsRequest { referenced_by } = request;
    let PaimonTableParameters {
        prefix,
        namespace,
        table_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let table_ident = iceberg::TableIdent::new(namespace.clone(), table_name.clone());

    let (warehouse, table_info, storage_permissions) =
        crate::server::tables::authorize_load_table::<C, A>(
            &request_metadata,
            table_ident.clone(),
            warehouse_id,
            crate::service::TabularListFlags::active(),
            state.v1_state.authz.clone(),
            state.v1_state.catalog.clone(),
            referenced_by.as_deref(),
        )
        .await
        .map_err(super::authz_to_error_model)?;
    let warehouse = require_paimon_warehouse(warehouse)?;
    let table_info = super::require_paimon_info(warehouse_id, &table_ident, table_info)?;

    let Some(storage_permissions) = storage_permissions else {
        return Ok(LoadPaimonTableCredentialsResponse {
            storage_credentials: vec![],
        });
    };

    let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
    let info =
        C::load_paimon_table_by_id(warehouse_id, table_info.table_id(), t.transaction()).await?;
    t.commit().await?;

    let storage_secret =
        maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
    let storage_config = warehouse
        .storage_profile
        .generate_table_config(
            data_access.into(),
            storage_secret.as_deref(),
            &info.location,
            storage_permissions,
            &request_metadata,
            &info,
        )
        .await?;

    let storage_credentials = if storage_config.creds.inner().is_empty() {
        vec![]
    } else {
        vec![StorageCredential {
            prefix: info.location.to_string(),
            config: storage_config.creds.into(),
        }]
    };

    Ok(LoadPaimonTableCredentialsResponse {
        storage_credentials,
    })
}
