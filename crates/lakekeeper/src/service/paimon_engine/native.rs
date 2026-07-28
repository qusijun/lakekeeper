use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use url::Url;
use uuid::Uuid;

use super::{
    AlteredPaimonEngineTable, DefaultPaimonEngine, InitializedPaimonTable, LoadedPaimonEngineTable,
    PaimonEngineError, PreparedPaimonCommit, PublishedPaimonCommit,
    default::{
        AlterPaimonEngineTableBackendRequest, CleanupStagedPaimonCommitBackendRequest,
        DynPaimonEngine, InitializePaimonTableBackendRequest, LoadPaimonEngineTableBackendRequest,
        PaimonEngineBackend, PreparePaimonCommitBackendRequest, PublishPaimonCommitBackendRequest,
        new_default_paimon_engine,
    },
};
use crate::service::{
    Location,
    storage::{
        AzCredential, GcsCredential, GcsProfile, GcsServiceKey, GenericAdlsProfile, OneLakeProfile,
        S3Credential, S3Profile, StorageCredential, StorageProfile,
        s3::{S3AccessKeyCredential, S3AwsSystemIdentityCredential, S3CloudflareR2Credential},
    },
};

const NATIVE_ENGINE_DISABLED_DETAIL: &str =
    "compile with feature `paimon-engine` to enable the native Paimon backend";
const NATIVE_ENGINE_UNWIRED_DETAIL: &str =
    "native Paimon backend is enabled but the paimon-rust bridge is not implemented yet";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePaimonRuntimeConfig {
    pub warehouse_location: Location,
    pub storage: NativePaimonStorageConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePaimonCatalogBootstrap {
    pub options: HashMap<String, String>,
    pub auth: NativePaimonCatalogAuth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePaimonCatalogOptionSet {
    pub options: HashMap<String, String>,
    pub temp_file_options: Vec<NativePaimonTempFileOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePaimonTempFileOption {
    pub option_key: String,
    pub file_name_hint: String,
    pub file_contents: String,
}

#[derive(Debug)]
pub struct MaterializedNativePaimonCatalogOptionSet {
    pub options: HashMap<String, String>,
    temp_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativePaimonCatalogAuth {
    None,
    S3AccessKey {
        access_key_id: String,
        secret_access_key: String,
    },
    S3SystemIdentity {
        external_id: Option<String>,
    },
    S3CloudflareR2 {
        access_key_id: String,
        secret_access_key: String,
        token: String,
        account_id: String,
    },
    AliyunOssAccessKey {
        access_key_id: String,
        secret_access_key: String,
        external_id: Option<String>,
    },
    AzClientCredentials {
        client_id: String,
        tenant_id: String,
        client_secret: String,
    },
    AzSharedAccessKey {
        key: String,
    },
    AzSystemIdentity,
    GcsServiceAccountKey {
        service_account_json: String,
    },
    GcsSystemIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativePaimonStorageConfig {
    S3(NativePaimonS3Config),
    Adls(NativePaimonAdlsConfig),
    Gcs(NativePaimonGcsConfig),
    #[cfg(feature = "test-utils")]
    Memory(NativePaimonMemoryConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePaimonS3Config {
    pub bucket: String,
    pub key_prefix: Option<String>,
    pub region: String,
    pub endpoint: Option<Url>,
    pub path_style_access: bool,
    pub auth: NativePaimonS3Auth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativePaimonS3Auth {
    AccessKey(S3AccessKeyCredential),
    AwsSystemIdentity(S3AwsSystemIdentityCredential),
    CloudflareR2(S3CloudflareR2Credential),
    AliyunOss(S3AccessKeyCredential),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativePaimonAdlsProfile {
    Generic(GenericAdlsProfile),
    OneLake(OneLakeProfile),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePaimonAdlsConfig {
    pub profile: NativePaimonAdlsProfile,
    pub auth: AzCredential,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePaimonGcsConfig {
    pub bucket: String,
    pub key_prefix: Option<String>,
    pub auth: NativePaimonGcsAuth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativePaimonGcsAuth {
    ServiceAccountKey(GcsServiceKey),
    GcpSystemIdentity,
}

#[cfg(feature = "test-utils")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativePaimonMemoryConfig {
    pub base_location: Location,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NativePaimonEngineBackend;

impl NativePaimonRuntimeConfig {
    pub fn from_warehouse(
        warehouse_location: &Location,
        storage_profile: &StorageProfile,
        storage_credential: Option<&StorageCredential>,
    ) -> Result<Self, PaimonEngineError> {
        let profile_base_location = storage_profile
            .base_location()
            .map_err(|err| PaimonEngineError::validation(err.to_string()))?;
        if !warehouse_location.is_sublocation_of(&profile_base_location) {
            return Err(PaimonEngineError::validation(format!(
                "warehouse location '{}' is outside storage profile base location '{}'",
                warehouse_location, profile_base_location
            )));
        }

        Ok(Self {
            warehouse_location: warehouse_location.clone(),
            storage: storage_config_from_profile(
                storage_profile,
                &profile_base_location,
                storage_credential,
            )?,
        })
    }

    pub fn catalog_bootstrap(&self) -> Result<NativePaimonCatalogBootstrap, PaimonEngineError> {
        let mut options =
            HashMap::from([("warehouse".to_string(), self.warehouse_location.to_string())]);
        let auth = match &self.storage {
            NativePaimonStorageConfig::S3(config) => {
                options.insert("s3.region".to_string(), config.region.clone());
                if let Some(endpoint) = &config.endpoint {
                    options.insert("s3.endpoint".to_string(), endpoint.to_string());
                }
                if config.path_style_access {
                    options.insert("s3.path.style.access".to_string(), "true".to_string());
                }
                s3_catalog_auth(&config.auth)
            }
            NativePaimonStorageConfig::Adls(config) => {
                maybe_insert_adls_endpoint_option(&mut options, &config.profile);
                adls_catalog_auth(&config.auth)
            }
            NativePaimonStorageConfig::Gcs(config) => gcs_catalog_auth(&config.auth)?,
            #[cfg(feature = "test-utils")]
            NativePaimonStorageConfig::Memory(_) => NativePaimonCatalogAuth::None,
        };

        Ok(NativePaimonCatalogBootstrap { options, auth })
    }

    pub fn catalog_option_set(&self) -> Result<NativePaimonCatalogOptionSet, PaimonEngineError> {
        let bootstrap = self.catalog_bootstrap()?;
        let mut options = bootstrap.options;
        let mut temp_file_options = Vec::new();
        options.insert("metastore".to_string(), "filesystem".to_string());

        match (&self.storage, bootstrap.auth) {
            (
                NativePaimonStorageConfig::S3(_),
                NativePaimonCatalogAuth::S3AccessKey {
                    access_key_id,
                    secret_access_key,
                },
            ) => {
                options.insert("s3.access-key-id".to_string(), access_key_id);
                options.insert("s3.secret-access-key".to_string(), secret_access_key);
            }
            (
                NativePaimonStorageConfig::S3(_),
                NativePaimonCatalogAuth::S3SystemIdentity { external_id },
            ) => {
                if external_id.is_some() {
                    return Err(PaimonEngineError::unsupported_options(
                        "native Paimon bootstrap does not yet support S3 system identity with external_id",
                    ));
                }
            }
            (NativePaimonStorageConfig::S3(_), NativePaimonCatalogAuth::S3CloudflareR2 { .. }) => {
                return Err(PaimonEngineError::unsupported_options(
                    "native Paimon bootstrap does not yet support Cloudflare R2 credentials",
                ));
            }
            (
                NativePaimonStorageConfig::S3(_),
                NativePaimonCatalogAuth::AliyunOssAccessKey { .. },
            ) => {
                return Err(PaimonEngineError::unsupported_options(
                    "native Paimon bootstrap does not yet support Aliyun OSS credentials",
                ));
            }
            (
                NativePaimonStorageConfig::Adls(_),
                NativePaimonCatalogAuth::AzSharedAccessKey { key },
            ) => {
                options.insert("azure.account-key".to_string(), key);
            }
            (NativePaimonStorageConfig::Adls(_), NativePaimonCatalogAuth::AzSystemIdentity) => {}
            (
                NativePaimonStorageConfig::Adls(_),
                NativePaimonCatalogAuth::AzClientCredentials { .. },
            ) => {
                return Err(PaimonEngineError::unsupported_options(
                    "native Paimon bootstrap does not yet support Azure client credentials",
                ));
            }
            (
                NativePaimonStorageConfig::Gcs(_),
                NativePaimonCatalogAuth::GcsServiceAccountKey {
                    service_account_json,
                },
            ) => {
                temp_file_options.push(NativePaimonTempFileOption {
                    option_key: "gcs.credential-path".to_string(),
                    file_name_hint: "gcp-service-account.json".to_string(),
                    file_contents: service_account_json,
                });
            }
            (NativePaimonStorageConfig::Gcs(_), NativePaimonCatalogAuth::GcsSystemIdentity) => {}
            #[cfg(feature = "test-utils")]
            (NativePaimonStorageConfig::Memory(_), NativePaimonCatalogAuth::None) => {}
            (_, NativePaimonCatalogAuth::None) => {}
            (storage, auth) => {
                return Err(PaimonEngineError::validation(format!(
                    "native Paimon bootstrap auth {auth:?} is incompatible with storage config {storage:?}",
                )));
            }
        }

        Ok(NativePaimonCatalogOptionSet {
            options,
            temp_file_options,
        })
    }
}

impl NativePaimonCatalogOptionSet {
    pub fn materialize(
        self,
    ) -> Result<MaterializedNativePaimonCatalogOptionSet, PaimonEngineError> {
        if self.temp_file_options.is_empty() {
            return Ok(MaterializedNativePaimonCatalogOptionSet {
                options: self.options,
                temp_dir: None,
            });
        }

        let temp_dir = std::env::temp_dir().join(format!("lakekeeper-paimon-{}", Uuid::new_v4()));
        fs::create_dir_all(&temp_dir).map_err(|err| {
            PaimonEngineError::cleanup_failed(format!(
                "failed to create native Paimon temp dir '{}': {err}",
                temp_dir.display()
            ))
        })?;

        let mut options = self.options;
        for temp_file in self.temp_file_options {
            let file_path = temp_dir.join(sanitize_file_name(&temp_file.file_name_hint));
            fs::write(&file_path, temp_file.file_contents).map_err(|err| {
                PaimonEngineError::cleanup_failed(format!(
                    "failed to materialize native Paimon temp file '{}': {err}",
                    file_path.display()
                ))
            })?;
            options.insert(
                temp_file.option_key,
                file_path_to_option_string(&file_path)?,
            );
        }

        Ok(MaterializedNativePaimonCatalogOptionSet {
            options,
            temp_dir: Some(temp_dir),
        })
    }
}

impl MaterializedNativePaimonCatalogOptionSet {
    #[must_use]
    pub fn temp_dir(&self) -> Option<&Path> {
        self.temp_dir.as_deref()
    }
}

impl Drop for MaterializedNativePaimonCatalogOptionSet {
    fn drop(&mut self) {
        if let Some(temp_dir) = self.temp_dir.take() {
            if let Err(err) = fs::remove_dir_all(&temp_dir) {
                tracing::warn!(
                    temp_dir = %temp_dir.display(),
                    error = %err,
                    "failed to remove native Paimon temp directory"
                );
            }
        }
    }
}

#[async_trait]
impl PaimonEngineBackend for NativePaimonEngineBackend {
    async fn initialize_table(
        &self,
        _request: InitializePaimonTableBackendRequest,
    ) -> Result<InitializedPaimonTable, PaimonEngineError> {
        Err(native_backend_error())
    }

    async fn load_table(
        &self,
        _request: LoadPaimonEngineTableBackendRequest,
    ) -> Result<LoadedPaimonEngineTable, PaimonEngineError> {
        Err(native_backend_error())
    }

    async fn alter_table(
        &self,
        _request: AlterPaimonEngineTableBackendRequest,
    ) -> Result<AlteredPaimonEngineTable, PaimonEngineError> {
        Err(native_backend_error())
    }

    async fn prepare_commit(
        &self,
        _request: PreparePaimonCommitBackendRequest,
    ) -> Result<PreparedPaimonCommit, PaimonEngineError> {
        Err(native_backend_error())
    }

    async fn publish_commit(
        &self,
        _request: PublishPaimonCommitBackendRequest,
    ) -> Result<PublishedPaimonCommit, PaimonEngineError> {
        Err(native_backend_error())
    }

    async fn cleanup_staged_commit(
        &self,
        _request: CleanupStagedPaimonCommitBackendRequest,
    ) -> Result<(), PaimonEngineError> {
        Err(native_backend_error())
    }
}

#[must_use]
pub fn native_paimon_engine() -> DynPaimonEngine {
    new_default_paimon_engine(Arc::new(NativePaimonEngineBackend))
}

#[must_use]
pub fn native_default_paimon_engine() -> DefaultPaimonEngine<NativePaimonEngineBackend> {
    DefaultPaimonEngine::new(Arc::new(NativePaimonEngineBackend))
}

#[must_use]
pub fn native_backend_error() -> PaimonEngineError {
    if cfg!(feature = "paimon-engine") {
        PaimonEngineError::engine_unavailable(NATIVE_ENGINE_UNWIRED_DETAIL)
    } else {
        PaimonEngineError::engine_unavailable(NATIVE_ENGINE_DISABLED_DETAIL)
    }
}

fn storage_config_from_profile(
    storage_profile: &StorageProfile,
    profile_base_location: &Location,
    storage_credential: Option<&StorageCredential>,
) -> Result<NativePaimonStorageConfig, PaimonEngineError> {
    match storage_profile {
        StorageProfile::S3(profile) => Ok(NativePaimonStorageConfig::S3(s3_storage_config(
            profile,
            storage_credential,
        )?)),
        StorageProfile::Adls(profile) => Ok(NativePaimonStorageConfig::Adls(adls_storage_config(
            profile.clone(),
            storage_credential,
        )?)),
        StorageProfile::OneLake(profile) => Ok(NativePaimonStorageConfig::Adls(
            onelake_storage_config(profile.clone(), storage_credential)?,
        )),
        StorageProfile::Gcs(profile) => Ok(NativePaimonStorageConfig::Gcs(gcs_storage_config(
            profile,
            storage_credential,
        )?)),
        #[cfg(feature = "test-utils")]
        StorageProfile::Memory(_profile) => Ok(NativePaimonStorageConfig::Memory(
            NativePaimonMemoryConfig {
                base_location: profile_base_location.clone(),
            },
        )),
    }
}

fn s3_storage_config(
    profile: &S3Profile,
    storage_credential: Option<&StorageCredential>,
) -> Result<NativePaimonS3Config, PaimonEngineError> {
    let auth = match require_storage_credential(storage_credential, "s3")? {
        StorageCredential::S3(credential) => match credential {
            S3Credential::AccessKey(credential) => {
                NativePaimonS3Auth::AccessKey(credential.clone())
            }
            S3Credential::AwsSystemIdentity(credential) => {
                NativePaimonS3Auth::AwsSystemIdentity(credential.clone())
            }
            S3Credential::CloudflareR2(credential) => {
                NativePaimonS3Auth::CloudflareR2(credential.clone())
            }
            S3Credential::AliyunOss(credential) => {
                NativePaimonS3Auth::AliyunOss(credential.clone())
            }
        },
        other => {
            return Err(PaimonEngineError::validation(format!(
                "storage credential type '{}' does not match storage profile type 's3'",
                other.storage_type()
            )));
        }
    };

    Ok(NativePaimonS3Config {
        bucket: profile.bucket.clone(),
        key_prefix: profile.key_prefix.clone(),
        region: profile.region.clone(),
        endpoint: profile.endpoint.clone(),
        path_style_access: profile.path_style_access.unwrap_or_default(),
        auth,
    })
}

fn adls_storage_config(
    profile: GenericAdlsProfile,
    storage_credential: Option<&StorageCredential>,
) -> Result<NativePaimonAdlsConfig, PaimonEngineError> {
    let auth = match require_storage_credential(storage_credential, "adls")? {
        StorageCredential::Az(credential) => credential.clone(),
        other => {
            return Err(PaimonEngineError::validation(format!(
                "storage credential type '{}' does not match storage profile type 'adls'",
                other.storage_type()
            )));
        }
    };

    Ok(NativePaimonAdlsConfig {
        profile: NativePaimonAdlsProfile::Generic(profile),
        auth,
    })
}

fn onelake_storage_config(
    profile: OneLakeProfile,
    storage_credential: Option<&StorageCredential>,
) -> Result<NativePaimonAdlsConfig, PaimonEngineError> {
    let auth = match require_storage_credential(storage_credential, "onelake")? {
        StorageCredential::Az(credential) => credential.clone(),
        other => {
            return Err(PaimonEngineError::validation(format!(
                "storage credential type '{}' does not match storage profile type 'onelake'",
                other.storage_type()
            )));
        }
    };

    Ok(NativePaimonAdlsConfig {
        profile: NativePaimonAdlsProfile::OneLake(profile),
        auth,
    })
}

fn gcs_storage_config(
    profile: &GcsProfile,
    storage_credential: Option<&StorageCredential>,
) -> Result<NativePaimonGcsConfig, PaimonEngineError> {
    let auth = match require_storage_credential(storage_credential, "gcs")? {
        StorageCredential::Gcs(credential) => match credential {
            GcsCredential::ServiceAccountKey { key } => {
                NativePaimonGcsAuth::ServiceAccountKey(key.clone())
            }
            GcsCredential::GcpSystemIdentity {} => NativePaimonGcsAuth::GcpSystemIdentity,
        },
        other => {
            return Err(PaimonEngineError::validation(format!(
                "storage credential type '{}' does not match storage profile type 'gcs'",
                other.storage_type()
            )));
        }
    };

    Ok(NativePaimonGcsConfig {
        bucket: profile.bucket.clone(),
        key_prefix: profile.key_prefix.clone(),
        auth,
    })
}

fn require_storage_credential<'a>(
    storage_credential: Option<&'a StorageCredential>,
    storage_type: &str,
) -> Result<&'a StorageCredential, PaimonEngineError> {
    storage_credential.ok_or_else(|| {
        PaimonEngineError::validation(format!(
            "native Paimon backend requires storage credentials for {storage_type} warehouses"
        ))
    })
}

fn sanitize_file_name(file_name_hint: &str) -> String {
    let sanitized = file_name_hint
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "native-paimon-option.tmp".to_string()
    } else {
        sanitized
    }
}

fn file_path_to_option_string(path: &Path) -> Result<String, PaimonEngineError> {
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| PaimonEngineError::cleanup_failed("temp file path is not valid UTF-8"))
}

fn s3_catalog_auth(auth: &NativePaimonS3Auth) -> NativePaimonCatalogAuth {
    match auth {
        NativePaimonS3Auth::AccessKey(credential) => NativePaimonCatalogAuth::S3AccessKey {
            access_key_id: credential.access_key_id.clone(),
            secret_access_key: credential.secret_access_key.clone(),
        },
        NativePaimonS3Auth::AwsSystemIdentity(credential) => {
            NativePaimonCatalogAuth::S3SystemIdentity {
                external_id: credential.external_id.clone(),
            }
        }
        NativePaimonS3Auth::CloudflareR2(credential) => NativePaimonCatalogAuth::S3CloudflareR2 {
            access_key_id: credential.access_key_id.clone(),
            secret_access_key: credential.secret_access_key.clone(),
            token: credential.token.clone(),
            account_id: credential.account_id.clone(),
        },
        NativePaimonS3Auth::AliyunOss(credential) => NativePaimonCatalogAuth::AliyunOssAccessKey {
            access_key_id: credential.access_key_id.clone(),
            secret_access_key: credential.secret_access_key.clone(),
            external_id: credential.external_id.clone(),
        },
    }
}

fn maybe_insert_adls_endpoint_option(
    options: &mut HashMap<String, String>,
    profile: &NativePaimonAdlsProfile,
) {
    match profile {
        NativePaimonAdlsProfile::Generic(profile) => {
            if let Some(host) = &profile.host {
                options.insert(
                    "azure.endpoint".to_string(),
                    format!("https://{}.{host}", profile.account_name),
                );
            }
        }
        NativePaimonAdlsProfile::OneLake(profile) => {
            options.insert(
                "azure.endpoint".to_string(),
                profile
                    .base_location()
                    .ok()
                    .and_then(|location| location.host_str().map(|host| format!("https://{host}")))
                    .unwrap_or_else(|| "https://onelake.dfs.fabric.microsoft.com".to_string()),
            );
        }
    }
}

fn adls_catalog_auth(auth: &AzCredential) -> NativePaimonCatalogAuth {
    match auth {
        AzCredential::ClientCredentials {
            client_id,
            tenant_id,
            client_secret,
        } => NativePaimonCatalogAuth::AzClientCredentials {
            client_id: client_id.clone(),
            tenant_id: tenant_id.clone(),
            client_secret: client_secret.clone(),
        },
        AzCredential::SharedAccessKey { key } => {
            NativePaimonCatalogAuth::AzSharedAccessKey { key: key.clone() }
        }
        AzCredential::AzureSystemIdentity {} => NativePaimonCatalogAuth::AzSystemIdentity,
    }
}

fn gcs_catalog_auth(
    auth: &NativePaimonGcsAuth,
) -> Result<NativePaimonCatalogAuth, PaimonEngineError> {
    match auth {
        NativePaimonGcsAuth::ServiceAccountKey(key) => {
            let service_account_json = serde_json::to_string(key).map_err(|err| {
                PaimonEngineError::validation(format!(
                    "failed to serialize GCS service account key for native bootstrap: {err}"
                ))
            })?;
            Ok(NativePaimonCatalogAuth::GcsServiceAccountKey {
                service_account_json,
            })
        }
        NativePaimonGcsAuth::GcpSystemIdentity => Ok(NativePaimonCatalogAuth::GcsSystemIdentity),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        NativePaimonAdlsProfile, NativePaimonCatalogAuth, NativePaimonGcsAuth,
        NativePaimonRuntimeConfig, NativePaimonS3Auth, NativePaimonStorageConfig,
    };
    use crate::service::{
        Location,
        storage::{
            AzCredential, GcsCredential, GcsProfile, GcsServiceKey, GenericAdlsProfile,
            MemoryProfile, S3Credential, S3Profile, StorageCredential, StorageProfile,
            s3::{S3AccessKeyCredential, S3AwsSystemIdentityCredential},
        },
    };

    fn s3_profile() -> S3Profile {
        S3Profile::builder()
            .bucket("warehouse-bucket".to_string())
            .key_prefix("root".to_string())
            .region("us-east-1".to_string())
            .sts_enabled(true)
            .flavor(Default::default())
            .build()
    }

    #[test]
    fn builds_runtime_config_for_s3_warehouse() {
        let warehouse_location: Location =
            "s3://warehouse-bucket/root/warehouse-a".parse().unwrap();
        let config = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &StorageProfile::S3(s3_profile()),
            Some(&StorageCredential::S3(S3Credential::AccessKey(
                S3AccessKeyCredential {
                    access_key_id: "access".to_string(),
                    secret_access_key: "secret".to_string(),
                    external_id: None,
                },
            ))),
        )
        .unwrap();

        assert_eq!(config.warehouse_location, warehouse_location);
        match config.storage {
            NativePaimonStorageConfig::S3(config) => {
                assert_eq!(config.bucket, "warehouse-bucket");
                assert_eq!(config.key_prefix.as_deref(), Some("root"));
                assert!(matches!(config.auth, NativePaimonS3Auth::AccessKey(_)));
            }
            other => panic!("expected s3 config, got {other:?}"),
        }
    }

    #[test]
    fn builds_catalog_bootstrap_for_s3_warehouse() {
        let warehouse_location: Location =
            "s3://warehouse-bucket/root/warehouse-a".parse().unwrap();
        let runtime = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &StorageProfile::S3(s3_profile()),
            Some(&StorageCredential::S3(S3Credential::AccessKey(
                S3AccessKeyCredential {
                    access_key_id: "access".to_string(),
                    secret_access_key: "secret".to_string(),
                    external_id: None,
                },
            ))),
        )
        .unwrap();

        let bootstrap = runtime.catalog_bootstrap().unwrap();
        assert_eq!(
            bootstrap.options.get("warehouse"),
            Some(&"s3://warehouse-bucket/root/warehouse-a".to_string())
        );
        assert_eq!(
            bootstrap.options.get("s3.region"),
            Some(&"us-east-1".to_string())
        );
        assert!(matches!(
            bootstrap.auth,
            NativePaimonCatalogAuth::S3AccessKey { .. }
        ));
    }

    #[test]
    fn builds_catalog_option_set_for_s3_access_key() {
        let warehouse_location: Location =
            "s3://warehouse-bucket/root/warehouse-a".parse().unwrap();
        let runtime = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &StorageProfile::S3(s3_profile()),
            Some(&StorageCredential::S3(S3Credential::AccessKey(
                S3AccessKeyCredential {
                    access_key_id: "access".to_string(),
                    secret_access_key: "secret".to_string(),
                    external_id: None,
                },
            ))),
        )
        .unwrap();

        let option_set = runtime.catalog_option_set().unwrap();
        assert_eq!(
            option_set.options.get("metastore"),
            Some(&"filesystem".to_string())
        );
        assert_eq!(
            option_set.options.get("s3.access-key-id"),
            Some(&"access".to_string())
        );
        assert_eq!(
            option_set.options.get("s3.secret-access-key"),
            Some(&"secret".to_string())
        );
        assert!(option_set.temp_file_options.is_empty());
    }

    #[test]
    fn rejects_mismatched_storage_credentials() {
        let warehouse_location: Location =
            "s3://warehouse-bucket/root/warehouse-a".parse().unwrap();
        let err = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &StorageProfile::S3(s3_profile()),
            Some(&StorageCredential::Az(AzCredential::AzureSystemIdentity {})),
        )
        .unwrap_err();

        assert!(err.to_string().contains("does not match"));
    }

    #[test]
    fn rejects_warehouse_location_outside_storage_base_location() {
        let warehouse_location: Location = "s3://warehouse-bucket/other-root/warehouse-a"
            .parse()
            .unwrap();
        let err = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &StorageProfile::S3(s3_profile()),
            Some(&StorageCredential::S3(S3Credential::AwsSystemIdentity(
                S3AwsSystemIdentityCredential { external_id: None },
            ))),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("outside storage profile base location")
        );
    }

    #[test]
    fn builds_runtime_config_for_adls_warehouse() {
        let warehouse_location: Location =
            "abfss://fs@account.dfs.core.windows.net/root/warehouse-a"
                .parse()
                .unwrap();
        let profile = StorageProfile::Adls(GenericAdlsProfile {
            filesystem: "fs".to_string(),
            key_prefix: Some("root".to_string()),
            account_name: "account".to_string(),
            authority_host: None,
            host: None,
            sas_token_validity_seconds: None,
            allow_alternative_protocols: false,
            sas_enabled: true,
            storage_layout: None,
        });
        let config = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &profile,
            Some(&StorageCredential::Az(AzCredential::AzureSystemIdentity {})),
        )
        .unwrap();

        match config.storage {
            NativePaimonStorageConfig::Adls(config) => {
                assert!(matches!(
                    config.profile,
                    NativePaimonAdlsProfile::Generic(_)
                ));
                assert!(matches!(config.auth, AzCredential::AzureSystemIdentity {}));
            }
            other => panic!("expected adls config, got {other:?}"),
        }
    }

    #[test]
    fn builds_catalog_bootstrap_for_adls_client_credentials() {
        let warehouse_location: Location = "abfss://fs@account.custom.endpoint/root/warehouse-a"
            .parse()
            .unwrap();
        let profile = StorageProfile::Adls(GenericAdlsProfile {
            filesystem: "fs".to_string(),
            key_prefix: Some("root".to_string()),
            account_name: "account".to_string(),
            authority_host: None,
            host: Some("custom.endpoint".to_string()),
            sas_token_validity_seconds: None,
            allow_alternative_protocols: false,
            sas_enabled: true,
            storage_layout: None,
        });
        let runtime = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &profile,
            Some(&StorageCredential::Az(AzCredential::ClientCredentials {
                client_id: "client".to_string(),
                tenant_id: "tenant".to_string(),
                client_secret: "secret".to_string(),
            })),
        )
        .unwrap();

        let bootstrap = runtime.catalog_bootstrap().unwrap();
        assert_eq!(
            bootstrap.options.get("azure.endpoint"),
            Some(&"https://account.custom.endpoint".to_string())
        );
        assert!(matches!(
            bootstrap.auth,
            NativePaimonCatalogAuth::AzClientCredentials { .. }
        ));
    }

    #[test]
    fn builds_catalog_option_set_for_adls_shared_access_key() {
        let warehouse_location: Location =
            "abfss://fs@account.dfs.core.windows.net/root/warehouse-a"
                .parse()
                .unwrap();
        let runtime = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &StorageProfile::Adls(GenericAdlsProfile {
                filesystem: "fs".to_string(),
                key_prefix: Some("root".to_string()),
                account_name: "account".to_string(),
                authority_host: None,
                host: None,
                sas_token_validity_seconds: None,
                allow_alternative_protocols: false,
                sas_enabled: true,
                storage_layout: None,
            }),
            Some(&StorageCredential::Az(AzCredential::SharedAccessKey {
                key: "account-key".to_string(),
            })),
        )
        .unwrap();

        let option_set = runtime.catalog_option_set().unwrap();
        assert_eq!(
            option_set.options.get("azure.account-key"),
            Some(&"account-key".to_string())
        );
    }

    #[test]
    fn builds_runtime_config_for_gcs_warehouse() {
        let warehouse_location: Location =
            "gs://warehouse-bucket/root/warehouse-a".parse().unwrap();
        let profile = StorageProfile::Gcs(GcsProfile {
            bucket: "warehouse-bucket".to_string(),
            key_prefix: Some("root".to_string()),
            sts_enabled: true,
            storage_layout: None,
        });
        let config = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &profile,
            Some(&StorageCredential::Gcs(GcsCredential::ServiceAccountKey {
                key: GcsServiceKey {
                    r#type: "service_account".to_string(),
                    project_id: "project-1".to_string(),
                    private_key_id: "pk".to_string(),
                    private_key: "secret".to_string(),
                    client_email: "svc@example.com".to_string(),
                    client_id: "123".to_string(),
                    auth_uri: "https://accounts.google.com/o/oauth2/auth".to_string(),
                    token_uri: "https://oauth2.googleapis.com/token".to_string(),
                    auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs"
                        .to_string(),
                    client_x509_cert_url: "https://www.googleapis.com/robot/v1/metadata/x509/svc"
                        .to_string(),
                    universe_domain: "googleapis.com".to_string(),
                },
            })),
        )
        .unwrap();

        match config.storage {
            NativePaimonStorageConfig::Gcs(config) => {
                assert_eq!(config.bucket, "warehouse-bucket");
                assert!(matches!(
                    config.auth,
                    NativePaimonGcsAuth::ServiceAccountKey(_)
                ));
            }
            other => panic!("expected gcs config, got {other:?}"),
        }
    }

    #[test]
    fn builds_catalog_bootstrap_for_gcs_warehouse() {
        let warehouse_location: Location =
            "gs://warehouse-bucket/root/warehouse-a".parse().unwrap();
        let runtime = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &StorageProfile::Gcs(GcsProfile {
                bucket: "warehouse-bucket".to_string(),
                key_prefix: Some("root".to_string()),
                sts_enabled: true,
                storage_layout: None,
            }),
            Some(&StorageCredential::Gcs(GcsCredential::ServiceAccountKey {
                key: GcsServiceKey {
                    r#type: "service_account".to_string(),
                    project_id: "project-1".to_string(),
                    private_key_id: "pk".to_string(),
                    private_key: "secret".to_string(),
                    client_email: "svc@example.com".to_string(),
                    client_id: "123".to_string(),
                    auth_uri: "https://accounts.google.com/o/oauth2/auth".to_string(),
                    token_uri: "https://oauth2.googleapis.com/token".to_string(),
                    auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs"
                        .to_string(),
                    client_x509_cert_url: "https://www.googleapis.com/robot/v1/metadata/x509/svc"
                        .to_string(),
                    universe_domain: "googleapis.com".to_string(),
                },
            })),
        )
        .unwrap();

        let bootstrap = runtime.catalog_bootstrap().unwrap();
        assert_eq!(
            bootstrap.options.get("warehouse"),
            Some(&"gs://warehouse-bucket/root/warehouse-a".to_string())
        );
        match bootstrap.auth {
            NativePaimonCatalogAuth::GcsServiceAccountKey {
                service_account_json,
            } => {
                assert!(service_account_json.contains("\"project_id\":\"project-1\""));
            }
            other => panic!("expected gcs service account auth, got {other:?}"),
        }
    }

    #[test]
    fn builds_catalog_option_set_for_gcs_service_account() {
        let warehouse_location: Location =
            "gs://warehouse-bucket/root/warehouse-a".parse().unwrap();
        let runtime = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &StorageProfile::Gcs(GcsProfile {
                bucket: "warehouse-bucket".to_string(),
                key_prefix: Some("root".to_string()),
                sts_enabled: true,
                storage_layout: None,
            }),
            Some(&StorageCredential::Gcs(GcsCredential::ServiceAccountKey {
                key: GcsServiceKey {
                    r#type: "service_account".to_string(),
                    project_id: "project-1".to_string(),
                    private_key_id: "pk".to_string(),
                    private_key: "secret".to_string(),
                    client_email: "svc@example.com".to_string(),
                    client_id: "123".to_string(),
                    auth_uri: "https://accounts.google.com/o/oauth2/auth".to_string(),
                    token_uri: "https://oauth2.googleapis.com/token".to_string(),
                    auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs"
                        .to_string(),
                    client_x509_cert_url: "https://www.googleapis.com/robot/v1/metadata/x509/svc"
                        .to_string(),
                    universe_domain: "googleapis.com".to_string(),
                },
            })),
        )
        .unwrap();

        let option_set = runtime.catalog_option_set().unwrap();
        assert_eq!(
            option_set.options.get("metastore"),
            Some(&"filesystem".to_string())
        );
        assert_eq!(option_set.temp_file_options.len(), 1);
        assert_eq!(
            option_set.temp_file_options[0].option_key,
            "gcs.credential-path"
        );
        assert!(
            option_set.temp_file_options[0]
                .file_contents
                .contains("\"project_id\":\"project-1\"")
        );
    }

    #[test]
    fn materializes_temp_file_options_into_temp_dir() {
        let warehouse_location: Location =
            "gs://warehouse-bucket/root/warehouse-a".parse().unwrap();
        let runtime = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &StorageProfile::Gcs(GcsProfile {
                bucket: "warehouse-bucket".to_string(),
                key_prefix: Some("root".to_string()),
                sts_enabled: true,
                storage_layout: None,
            }),
            Some(&StorageCredential::Gcs(GcsCredential::ServiceAccountKey {
                key: GcsServiceKey {
                    r#type: "service_account".to_string(),
                    project_id: "project-1".to_string(),
                    private_key_id: "pk".to_string(),
                    private_key: "secret".to_string(),
                    client_email: "svc@example.com".to_string(),
                    client_id: "123".to_string(),
                    auth_uri: "https://accounts.google.com/o/oauth2/auth".to_string(),
                    token_uri: "https://oauth2.googleapis.com/token".to_string(),
                    auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs"
                        .to_string(),
                    client_x509_cert_url: "https://www.googleapis.com/robot/v1/metadata/x509/svc"
                        .to_string(),
                    universe_domain: "googleapis.com".to_string(),
                },
            })),
        )
        .unwrap();

        let materialized = runtime.catalog_option_set().unwrap().materialize().unwrap();
        let credential_path = materialized.options.get("gcs.credential-path").unwrap();
        let temp_dir = materialized.temp_dir().unwrap().to_path_buf();
        assert!(credential_path.starts_with(temp_dir.to_str().unwrap()));
        let file_contents = fs::read_to_string(credential_path).unwrap();
        assert!(file_contents.contains("\"project_id\":\"project-1\""));
        drop(materialized);
        assert!(!temp_dir.exists());
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn builds_runtime_config_for_memory_warehouse() {
        let profile = MemoryProfile::default();
        let warehouse_location = StorageProfile::Memory(profile.clone())
            .base_location()
            .unwrap();
        let config = NativePaimonRuntimeConfig::from_warehouse(
            &warehouse_location,
            &StorageProfile::Memory(profile.clone()),
            None,
        )
        .unwrap();

        match config.storage {
            NativePaimonStorageConfig::Memory(memory) => {
                assert_eq!(memory.base_location, warehouse_location);
            }
            other => panic!("expected memory config, got {other:?}"),
        }
    }
}
