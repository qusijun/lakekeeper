use std::{str::FromStr, sync::Arc};

use http::StatusCode;
use uuid::Uuid;

use crate::{
    api::{
        ApiContext, ErrorModel,
        data::v1::paimon_tables::{CreatePaimonTableRequest, LoadPaimonTableResponse},
        endpoints::EndpointFlat,
        iceberg::v1::namespace::NamespaceParameters,
    },
    request_metadata::RequestMetadata,
    server::{
        maybe_get_secret, require_paimon_warehouse, require_warehouse_id,
        tabular::determine_tabular_location,
    },
    service::{
        CachePolicy, CatalogIdempotencyOps, CatalogPaimonOps, CatalogStore,
        IcebergErrorResponse, PaimonTableCreation, Result, SecretStore, State, TableId,
        TabularId, Transaction,
        authz::{Authorizer, AuthzNamespaceOps, CatalogNamespaceAction},
        idempotency::{IdempotencyInfo, IdempotencyKey},
    },
};

fn validate_request(request: &CreatePaimonTableRequest) -> Result<()> {
    if request.name.is_empty() {
        return Err(ErrorModel::bad_request(
            "Paimon table name cannot be empty",
            "InvalidName",
            None,
        )
        .into());
    }
    if request.name.contains('+') {
        return Err(ErrorModel::bad_request(
            "Paimon table name cannot contain '+' character.",
            "InvalidName",
            None,
        )
        .into());
    }
    if request.current_branch.trim().is_empty() {
        return Err(ErrorModel::bad_request(
            "Paimon current branch cannot be empty.",
            "InvalidBranch",
            None,
        )
        .into());
    }
    Ok(())
}

pub(super) async fn create_paimon_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    request: CreatePaimonTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<LoadPaimonTableResponse> {
    let NamespaceParameters { namespace, prefix } = &parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    validate_request(&request)?;

    let idempotency_key = request_metadata.idempotency_key().copied();
    if let Some(ref key) = idempotency_key {
        let check =
            C::check_idempotency_key(warehouse_id, key, state.v1_state.catalog.clone()).await?;
        if check.is_replay() {
            return super::load::load_paimon_table::<C, A, S>(
                crate::api::data::v1::paimon_tables::PaimonTableParameters {
                    prefix: prefix.clone(),
                    namespace: namespace.clone(),
                    table_name: request.name.clone(),
                },
                state,
                crate::api::iceberg::v1::DataAccessMode::ClientManaged,
                request_metadata,
            )
            .await;
        }
    }

    create_paimon_table_inner::<C, A, S>(
        warehouse_id,
        namespace,
        request,
        state,
        request_metadata,
        idempotency_key.as_ref(),
    )
    .await
}

async fn create_paimon_table_inner<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    warehouse_id: crate::WarehouseId,
    namespace: &iceberg::NamespaceIdent,
    request: CreatePaimonTableRequest,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
    idempotency_key: Option<&IdempotencyKey>,
) -> Result<LoadPaimonTableResponse> {
    let authorizer = state.v1_state.authz.clone();
    let table_id = TableId::from(Uuid::now_v7());

    let action = CatalogNamespaceAction::CreateTable {
        name: Some(request.name.clone()),
        table_id: Some(table_id),
        properties: Arc::new(Default::default()),
    };

    let (warehouse, ns_hierarchy) = authorizer
        .load_and_authorize_namespace_action::<C>(
            &request_metadata,
            crate::service::events::context::UserProvidedNamespace::new(
                warehouse_id,
                namespace.clone(),
            ),
            action,
            CachePolicy::Use,
            state.v1_state.catalog.clone(),
        )
        .await
        .map_err(super::authz_failure_to_error_model)?;
    let warehouse = require_paimon_warehouse(warehouse)?;

    let location = determine_tabular_location(
        &ns_hierarchy,
        request.location.clone(),
        TabularId::Table(table_id),
        &iceberg::TableIdent::new(namespace.clone(), request.name.clone()),
        &warehouse.storage_profile,
    )?;
    let metadata_location = request
        .metadata_location
        .as_deref()
        .map(lakekeeper_io::Location::from_str)
        .transpose()
        .map_err(|e| ErrorModel::bad_request(e.to_string(), "InvalidMetadataLocation", None))?;

    let mut t = C::Transaction::begin_write(state.v1_state.catalog.clone()).await?;
    let info = C::create_paimon_table(
        PaimonTableCreation {
            tabular_id: table_id,
            warehouse_id,
            namespace_id: ns_hierarchy.namespace_id(),
            name: request.name.clone(),
            location: location.clone(),
            metadata_location,
            current_snapshot_id: request.current_snapshot_id,
            current_branch: request.current_branch.clone(),
            schema: request.schema.clone(),
            table_options: request.table_options.clone(),
            partition_keys: request.partition_keys.clone(),
            primary_keys: request.primary_keys.clone(),
            comment: request.comment.clone(),
        },
        t.transaction(),
    )
    .await?;

    authorizer
        .create_table(
            &request_metadata,
            warehouse_id,
            table_id,
            ns_hierarchy.namespace_id(),
        )
        .await
        .map_err(IcebergErrorResponse::from)?;

    if let Some(key) = idempotency_key
        && !C::try_insert_idempotency_key(
            warehouse_id,
            &IdempotencyInfo::builder()
                .key(*key)
                .endpoint(EndpointFlat::PaimonTableV1CreatePaimonTable)
                .http_status(StatusCode::OK)
                .build(),
            t.transaction(),
        )
        .await?
    {
        t.rollback()
            .await
            .inspect_err(|e| tracing::warn!("Rollback failed after idempotency conflict: {e}"))
            .ok();
        return Err(ErrorModel::request_in_progress().into());
    }

    t.commit().await?;

    let storage_secret =
        maybe_get_secret(warehouse.storage_secret_id, &state.v1_state.secrets).await?;
    let config = warehouse
        .storage_profile
        .generate_table_config(
            crate::api::iceberg::v1::DataAccessMode::ClientManaged,
            storage_secret.as_deref(),
            &location,
            crate::service::storage::StoragePermissions::ReadWriteDelete,
            &request_metadata,
            &info,
        )
        .await?;

    let storage_credentials = (!config.creds.inner().is_empty()).then(|| {
        vec![iceberg_ext::catalog::rest::StorageCredential {
            prefix: location.to_string(),
            config: config.creds.into(),
        }]
    });

    Ok(LoadPaimonTableResponse {
        table: super::paimon_data(&info),
        config: Some(config.config.into()),
        storage_credentials,
    })
}

#[cfg(test)]
mod tests {
    use super::validate_request;
    use crate::api::data::v1::paimon_tables::CreatePaimonTableRequest;

    fn request_with_name(name: &str) -> CreatePaimonTableRequest {
        CreatePaimonTableRequest {
            name: name.to_string(),
            location: None,
            metadata_location: None,
            current_snapshot_id: None,
            current_branch: "main".to_string(),
            schema: None,
            table_options: Default::default(),
            partition_keys: Vec::new(),
            primary_keys: Vec::new(),
            comment: None,
        }
    }

    #[test]
    fn validate_request_rejects_empty_name() {
        let error = validate_request(&request_with_name("")).unwrap_err();

        assert_eq!(error.error.r#type, "InvalidName");
    }

    #[test]
    fn validate_request_rejects_plus_in_name() {
        let error = validate_request(&request_with_name("orders+archive")).unwrap_err();

        assert_eq!(error.error.r#type, "InvalidName");
    }

    #[test]
    fn validate_request_rejects_blank_branch() {
        let mut request = request_with_name("orders");
        request.current_branch = "   ".to_string();

        let error = validate_request(&request).unwrap_err();

        assert_eq!(error.error.r#type, "InvalidBranch");
    }

    #[test]
    fn validate_request_accepts_normal_request() {
        validate_request(&request_with_name("orders")).unwrap();
    }
}
