use std::net::SocketAddr;

use picomq_runtime::{AuthMode, KafkaConfig, MetaBackend, ServerConfig};
use url::Url;

use crate::guc::Settings;

const MIB: u64 = 1024 * 1024;

pub(crate) struct Local {
    pub(crate) socket: Option<String>,
    pub(crate) port: u16,
    pub(crate) user: String,
}

pub(crate) fn server(settings: &Settings, local: &Local) -> Result<ServerConfig, String> {
    let storage = settings
        .storage
        .clone()
        .ok_or("pico.storage is not set; pico needs an object storage bucket URI")?;
    let database = match &settings.database_url {
        Some(url) => url.clone(),
        None => connection(settings, local)?,
    };
    let wal_cache = settings.wal_cache_mb * MIB;
    let defaults = ServerConfig::default();
    Ok(ServerConfig {
        node_id: settings.node_id,
        addr: address("pico.listen", &settings.listen)?,
        admin_addr: settings
            .admin_listen
            .as_deref()
            .map(|value| address("pico.admin_listen", value))
            .transpose()?,
        advertised_url: settings.advertised_url.clone(),
        kafka: settings
            .kafka_listen
            .as_deref()
            .map(|value| {
                Ok::<_, String>(KafkaConfig {
                    listen: address("pico.kafka_listen", value)?,
                    advertise: settings.kafka_advertise.clone(),
                })
            })
            .transpose()?,
        meta_backend: MetaBackend::parse(&database).map_err(|e| e.to_string())?,
        storage_uri: storage,
        wal_uri: Some(settings.wal.clone().unwrap_or(database)),
        cluster_id: settings.cluster_id.clone(),
        auth_mode: match settings.auth.as_str() {
            "off" => AuthMode::Off,
            "required" => AuthMode::Required,
            other => return Err(format!("pico.auth must be off or required, not {other:?}")),
        },
        insecure_allow_remote: settings.insecure_allow_remote,
        bootstrap_token: settings.bootstrap_token.clone(),
        schema_registry: settings.schema_registry.clone(),
        engine: s3stream::Config {
            wal_cache_size: wal_cache,
            wal_upload_threshold: wal_cache * 2 / 5,
            wal_upload_interval_ms: settings.wal_upload_interval_ms,
            block_cache_size: settings.block_cache_mb * MIB,
            ..defaults.engine.clone()
        },
        ..defaults
    })
}

fn connection(settings: &Settings, local: &Local) -> Result<String, String> {
    let socket = local.socket.as_deref().ok_or(
        "pico needs a filesystem unix socket (unix_socket_directories) or pico.database_url",
    )?;
    let mut url = Url::parse("postgresql://localhost").map_err(|e| e.to_string())?;
    url.set_username(settings.role.as_deref().unwrap_or(&local.user))
        .map_err(|()| "pico.role is not a valid user name".to_owned())?;
    url.set_port(Some(local.port))
        .map_err(|()| "the server port is not valid in a URL".to_owned())?;
    url.set_path(&settings.database);
    url.query_pairs_mut().append_pair("host", socket);
    Ok(url.into())
}

fn address(name: &str, value: &str) -> Result<SocketAddr, String> {
    value
        .parse()
        .map_err(|_| format!("{name} must be host:port, not {value:?}"))
}
