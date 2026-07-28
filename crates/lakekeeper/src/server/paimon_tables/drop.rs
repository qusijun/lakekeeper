use std::sync::Arc;

use http::StatusCode;

use crate::{
    api::{
        ApiContext, ErrorModel,
        data::v1::paimon_tables::PaimonTableParameters,
        endpoints::EndpointFlat,
        iceberg::types::DropParams,
        management::v1::{DeleteKind, warehouse::TabularDeleteProfile},
    },
    request_metadata::RequestMetadata,
    server::{require_paimon_warehouse, require_warehouse_id},
    service::{
        AuthZTableInfo, CatalogIdempotencyOps, CatalogStore, CatalogTabularOps, NamedEntity,
        Result, SecretStore, State, TabularId, Transaction,
        authz::{AuthZTableOps, Authorizer, CatalogTableAction},
        events::{APIEventContext, context::ResolvedTable},
        idempotency::IdempotencyInfo,
        tasks::{
            ScheduleTaskMetadata, TaskEntity, WarehouseTaskEntityId,
            tabular_expiration_queue::{TabularExpirationPayload, TabularExpirationTask},
            tabular_purge_queue::{TabularPurgePayload, TabularPurgeTask},
        },
    },
};

pub(super) async fn drop_paimon_table<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: PaimonTableParameters,
    DropParams {
        purge_requested,
        force,
    }: DropParams,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<()> {
    let PaimonTableParameters {
        prefix,
        namespace,
        table_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let authorizer = state.v1_state.authz;
    let table_ident = iceberg::TableIdent::new(namespace.clone(), table_name.clone());

    let idempotency_key = request_metadata.idempotency_key().copied();
    if let Some(ref key) = idempotency_key {
        let check =
            C::check_idempotency_key(warehouse_id, key, state.v1_state.catalog.clone()).await?;
        if check.is_replay() {
            return Ok(());
        }
    }

    let event_ctx = APIEventContext::for_table(
        Arc::new(request_metadata),
        state.v1_state.events,
        warehouse_id,
        table_ident.clone(),
        CatalogTableAction::Drop {
            force,
            purge: purge_requested,
        },
    );

    let authz_result = authorizer
        .load_and_authorize_table_operation::<C>(
            event_ctx.request_metadata(),
            event_ctx.user_provided_entity(),
            crate::service::TabularListFlags::active(),
            event_ctx.action().clone(),
            state.v1_state.catalog.clone(),
        )
        .await;
    let (event_ctx, (warehouse, _ns, table_info)) = event_ctx.emit_authz(authz_result)?;
    let warehouse = require_paimon_warehouse(warehouse)?;
    let table_info = super::require_paimon_info(warehouse_id, &table_ident, table_info)?;
    let table_id = table_info.table_id();

    let event_ctx = event_ctx.resolve(ResolvedTable {
        warehouse: warehouse.clone(),
        table: Arc::new(table_info),
        storage_permissions: None,
    });

    let mut t = C::Transaction::begin_write(state.v1_state.catalog).await?;
    let delete_profile = if force {
        TabularDeleteProfile::Hard {}
    } else {
        warehouse.tabular_delete_profile
    };
    let project_id = &warehouse.project_id;

    match delete_profile {
        TabularDeleteProfile::Hard {} => {
            let location = C::drop_tabular(warehouse_id, table_id, force, t.transaction()).await?;

            if purge_requested {
                TabularPurgeTask::schedule_task::<C>(
                    ScheduleTaskMetadata {
                        project_id: project_id.clone(),
                        parent_task_id: None,
                        scheduled_for: None,
                        entity: TaskEntity::EntityInWarehouse {
                            entity_name: table_ident.clone().into_name_parts(),
                            warehouse_id,
                            entity_id: WarehouseTaskEntityId::Table { table_id },
                        },
                    },
                    TabularPurgePayload {
                        tabular_location: location.to_string(),
                    },
                    t.transaction(),
                )
                .await?;
            }
        }
        TabularDeleteProfile::Soft { expiration_seconds } => {
            let _ = TabularExpirationTask::schedule_task::<C>(
                ScheduleTaskMetadata {
                    project_id: project_id.clone(),
                    parent_task_id: None,
                    scheduled_for: Some(chrono::Utc::now() + expiration_seconds),
                    entity: TaskEntity::EntityInWarehouse {
                        entity_name: table_ident.clone().into_name_parts(),
                        entity_id: WarehouseTaskEntityId::Table { table_id },
                        warehouse_id,
                    },
                },
                TabularExpirationPayload {
                    deletion_kind: if purge_requested {
                        DeleteKind::Purge
                    } else {
                        DeleteKind::Default
                    },
                },
                t.transaction(),
            )
            .await?;

            C::mark_tabular_as_deleted(
                warehouse_id,
                TabularId::Table(table_id),
                force,
                t.transaction(),
            )
            .await?;
        }
    }

    if let Some(ref key) = idempotency_key
        && !C::try_insert_idempotency_key(
            warehouse_id,
            &IdempotencyInfo::builder()
                .key(*key)
                .endpoint(EndpointFlat::PaimonTableV1DropPaimonTable)
                .http_status(StatusCode::NO_CONTENT)
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

    if matches!(delete_profile, TabularDeleteProfile::Hard {}) {
        authorizer
            .delete_table(warehouse_id, table_id)
            .await
            .inspect_err(|e| {
                tracing::error!(
                    ?e,
                    "Failed to delete paimon table from authorizer: {}",
                    e.error
                );
            })
            .ok();
    }

    event_ctx.emit_table_dropped_async(DropParams {
        purge_requested,
        force,
    });

    Ok(())
}
