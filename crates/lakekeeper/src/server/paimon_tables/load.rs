use iceberg_ext::catalog::rest::StorageCredential;

use crate::{
    api::{
        ApiContext,
        data::v1::paimon_tables::{LoadPaimonTableResponse, PaimonTableParameters},
        iceberg::v1::DataAccessMode,
    },
    request_metadata::RequestMetadata,
    server::{maybe_get_secret, require_paimon_warehouse, require_warehouse_id},
    service::{
        AuthZTableInfo, CatalogPaimonOps, CatalogStore, Result, SecretStore, State, Transaction,
        authz::Authorizer,
    },
};

pub(super) async fn load_paimon_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: PaimonTableParameters,
    state: ApiContext<State<A, C, S>>,
    data_access: impl Into<DataAccessMode>,
    request_metadata: RequestMetadata,
) -> Result<LoadPaimonTableResponse> {
    let data_access = data_access.into();
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
            None,
        )
        .await
        .map_err(super::authz_to_error_model)?;
    let warehouse = require_paimon_warehouse(warehouse)?;
    let table_info = super::require_paimon_info(warehouse_id, &table_ident, table_info)?;

    let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
    let info =
        C::load_paimon_table_by_id(warehouse_id, table_info.table_id(), t.transaction()).await?;
    t.commit().await?;

    let (config, storage_credentials) = if let Some(storage_permissions) = storage_permissions {
        let storage_secret =
            maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
        let table_config = warehouse
            .storage_profile
            .generate_table_config(
                data_access,
                storage_secret.as_deref(),
                &info.location,
                storage_permissions,
                &request_metadata,
                &info,
            )
            .await?;

        let creds = (!table_config.creds.inner().is_empty()).then(|| {
            vec![StorageCredential {
                prefix: info.location.to_string(),
                config: table_config.creds.clone().into(),
            }]
        });

        (Some(table_config.config.into()), creds)
    } else {
        (None, None)
    };

    Ok(LoadPaimonTableResponse {
        table: super::paimon_data(&info),
        config,
        storage_credentials,
    })
}
