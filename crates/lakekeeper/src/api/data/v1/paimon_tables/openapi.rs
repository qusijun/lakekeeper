#![allow(clippy::needless_for_each)]

use utoipa::{OpenApi, openapi::security::SecurityScheme};

#[derive(Debug, OpenApi)]
#[openapi(
    info(
        title = "Lakekeeper Paimon Table API",
        description = "Lakekeeper data-plane API for Paimon catalogs.",
    ),
    servers(
        (
            url = "{scheme}://{host}{basePath}",
            description = "Lakekeeper Paimon Table API",
            variables(
                ("scheme" = (default = "https", description = "The scheme of the URI, either http or https")),
                ("host" = (default = "localhost", description = "The host (and optional port) for the specified server")),
                ("basePath" = (default = "", description = "Optional path prefix (starting with '/') to be prepended to all routes"))
            )
        )
    ),
    tags(
        (name = "paimon-table", description = "Manage Paimon tables")
    ),
    security(("bearerAuth" = [])),
    paths(
        super::create_paimon_table,
        super::list_paimon_tables,
        super::load_paimon_table,
        super::drop_paimon_table,
        super::load_paimon_table_credentials,
        super::update_paimon_commit_state,
    ),
    modifiers(&SecurityAddon)
)]
struct PaimonTableApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi
            .components
            .get_or_insert_with(|| utoipa::openapi::ComponentsBuilder::new().build());
        components.add_security_scheme(
            "bearerAuth",
            SecurityScheme::Http(
                utoipa::openapi::security::HttpBuilder::new()
                    .scheme(utoipa::openapi::security::HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}

#[must_use]
pub fn api_doc() -> utoipa::openapi::OpenApi {
    PaimonTableApiDoc::openapi()
}
