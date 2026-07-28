use crate::{
    api::{
        ApiContext,
        data::v1::paimon_tables::{
            ListPaimonTablesQuery, ListPaimonTablesResponse, PaimonTableIdentifier,
        },
        iceberg::v1::namespace::NamespaceParameters,
    },
    request_metadata::RequestMetadata,
    server::{require_paimon_warehouse, require_warehouse_id},
    service::{
        CachePolicy, CatalogPaimonOps, CatalogStore, Result, SecretStore, State, Transaction,
        authz::{
            ActionOnTable, AuthZTableOps, Authorizer, AuthzNamespaceOps, CatalogNamespaceAction,
            CatalogTableAction,
        },
    },
};

pub(super) async fn list_paimon_tables<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: NamespaceParameters,
    query: ListPaimonTablesQuery,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<ListPaimonTablesResponse> {
    let NamespaceParameters { namespace, prefix } = &parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let authorizer = &state.v1_state.authz;

    let (warehouse, ns) = authorizer
        .load_and_authorize_namespace_action::<C>(
            &request_metadata,
            crate::service::events::context::UserProvidedNamespace::new(
                warehouse_id,
                namespace.clone(),
            ),
            CatalogNamespaceAction::ListTables,
            CachePolicy::Use,
            state.v1_state.catalog.clone(),
        )
        .await
        .map_err(super::authz_failure_to_error_model)?;
    let warehouse = require_paimon_warehouse(warehouse)?;

    let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
    let (entries, next_page_token) = C::list_paimon_tables(
        warehouse_id,
        ns.namespace_id(),
        namespace,
        query.page_size,
        query.page_token.as_deref(),
        t.transaction(),
    )
    .await?;
    t.commit().await?;

    let can_list_everything = authorizer
        .is_allowed_namespace_action(
            &request_metadata,
            None,
            &warehouse,
            &ns.parents,
            &ns.namespace,
            CatalogNamespaceAction::ListEverything,
        )
        .await
        .map_err(crate::service::events::AuthorizationFailureSource::into_error_model)?
        .into_inner();

    let masks = if can_list_everything {
        vec![true; entries.len()]
    } else {
        let parents_map = ns
            .parents
            .iter()
            .cloned()
            .map(|parent| (parent.namespace_id(), parent))
            .collect();

        let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
        let mut infos = Vec::with_capacity(entries.len());
        for entry in &entries {
            infos.push(
                C::load_paimon_table_by_id(warehouse_id, entry.tabular_id, t.transaction()).await?,
            );
        }
        t.commit().await?;

        let actions: Vec<_> = infos
            .iter()
            .map(|info| {
                (
                    &ns.namespace,
                    ActionOnTable {
                        info,
                        action: CatalogTableAction::IncludeInList,
                        user: None,
                        is_delegated_execution: false,
                    },
                )
            })
            .collect();

        authorizer
            .are_allowed_table_actions_vec(&request_metadata, &warehouse, &parents_map, &actions)
            .await
            .map_err(crate::service::events::AuthorizationFailureSource::into_error_model)?
            .into_allowed()
    };

    let identifiers = entries
        .into_iter()
        .zip(masks)
        .filter(|(_, allowed)| *allowed)
        .map(|(entry, _)| PaimonTableIdentifier {
            namespace: namespace.clone().inner(),
            name: entry.name,
            id: Some(entry.tabular_id),
            commit_state: entry.commit_state,
        })
        .collect();

    Ok(ListPaimonTablesResponse {
        identifiers,
        next_page_token,
    })
}
