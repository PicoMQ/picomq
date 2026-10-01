use std::ffi::{CStr, CString};

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

type Text = GucSetting<Option<CString>>;

const DEFAULT_DATABASE: &CStr = c"postgres";
const DEFAULT_LISTEN: &CStr = c"127.0.0.1:4437";
const DEFAULT_ADMIN_LISTEN: &CStr = c"127.0.0.1:9090";
const DEFAULT_KAFKA_LISTEN: &CStr = c"127.0.0.1:9092";
const DEFAULT_CLUSTER_ID: &CStr = c"picomq";
const DEFAULT_AUTH: &CStr = c"off";
const DEFAULT_LOG: &CStr = c"warn";

static STORAGE: Text = Text::new(None);
static WAL: Text = Text::new(None);
static DATABASE: Text = Text::new(Some(DEFAULT_DATABASE));
static ROLE: Text = Text::new(None);
static DATABASE_URL: Text = Text::new(None);
static LISTEN: Text = Text::new(Some(DEFAULT_LISTEN));
static ADMIN_LISTEN: Text = Text::new(Some(DEFAULT_ADMIN_LISTEN));
static KAFKA_LISTEN: Text = Text::new(Some(DEFAULT_KAFKA_LISTEN));
static ADVERTISED_URL: Text = Text::new(None);
static KAFKA_ADVERTISE: Text = Text::new(None);
static CLUSTER_ID: Text = Text::new(Some(DEFAULT_CLUSTER_ID));
static AUTH: Text = Text::new(Some(DEFAULT_AUTH));
static BOOTSTRAP_TOKEN: Text = Text::new(None);
static LOG: Text = Text::new(Some(DEFAULT_LOG));
static SCHEMA_REGISTRY: Text = Text::new(None);
static NODE_ID: GucSetting<i32> = GucSetting::<i32>::new(1);
static WAL_CACHE_MB: GucSetting<i32> = GucSetting::<i32>::new(64);
static WAL_UPLOAD_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(0);
static BLOCK_CACHE_MB: GucSetting<i32> = GucSetting::<i32>::new(64);
static THREADS: GucSetting<i32> = GucSetting::<i32>::new(2);
static INSECURE_ALLOW_REMOTE: GucSetting<bool> = GucSetting::<bool>::new(false);

pub(crate) struct Settings {
    pub(crate) storage: Option<String>,
    pub(crate) wal: Option<String>,
    pub(crate) database: String,
    pub(crate) role: Option<String>,
    pub(crate) database_url: Option<String>,
    pub(crate) listen: String,
    pub(crate) admin_listen: Option<String>,
    pub(crate) kafka_listen: Option<String>,
    pub(crate) advertised_url: Option<String>,
    pub(crate) kafka_advertise: Option<String>,
    pub(crate) cluster_id: String,
    pub(crate) auth: String,
    pub(crate) bootstrap_token: Option<String>,
    pub(crate) log: String,
    pub(crate) schema_registry: Option<String>,
    pub(crate) node_id: i32,
    pub(crate) wal_cache_mb: u64,
    pub(crate) wal_upload_interval_ms: u64,
    pub(crate) block_cache_mb: u64,
    pub(crate) threads: usize,
    pub(crate) insecure_allow_remote: bool,
}

impl Settings {
    pub(crate) fn load() -> Self {
        Self {
            storage: value(&STORAGE),
            wal: value(&WAL),
            database: value(&DATABASE).unwrap_or_else(|| text_of(DEFAULT_DATABASE)),
            role: value(&ROLE),
            database_url: value(&DATABASE_URL),
            listen: value(&LISTEN).unwrap_or_else(|| text_of(DEFAULT_LISTEN)),
            admin_listen: value(&ADMIN_LISTEN),
            kafka_listen: value(&KAFKA_LISTEN),
            advertised_url: value(&ADVERTISED_URL),
            kafka_advertise: value(&KAFKA_ADVERTISE),
            cluster_id: value(&CLUSTER_ID).unwrap_or_else(|| text_of(DEFAULT_CLUSTER_ID)),
            auth: value(&AUTH).unwrap_or_else(|| text_of(DEFAULT_AUTH)),
            bootstrap_token: value(&BOOTSTRAP_TOKEN),
            log: value(&LOG).unwrap_or_else(|| text_of(DEFAULT_LOG)),
            schema_registry: value(&SCHEMA_REGISTRY),
            node_id: NODE_ID.get(),
            wal_cache_mb: WAL_CACHE_MB.get() as u64,
            wal_upload_interval_ms: WAL_UPLOAD_INTERVAL_MS.get() as u64,
            block_cache_mb: BLOCK_CACHE_MB.get() as u64,
            threads: THREADS.get() as usize,
            insecure_allow_remote: INSECURE_ALLOW_REMOTE.get(),
        }
    }
}

pub(crate) fn register() {
    text(
        c"pico.storage",
        c"Object storage bucket URI for stream data",
        &STORAGE,
    );
    secret(
        c"pico.wal",
        c"WAL location; empty keeps the WAL in this database",
        &WAL,
    );
    text(
        c"pico.database",
        c"Database holding pico metadata and the WAL",
        &DATABASE,
    );
    text(
        c"pico.role",
        c"Role pico connects as; empty uses the server's OS user",
        &ROLE,
    );
    secret(
        c"pico.database_url",
        c"Connection URL overriding pico.database and pico.role",
        &DATABASE_URL,
    );
    text(
        c"pico.listen",
        c"Address of the stream protocol listener",
        &LISTEN,
    );
    text(
        c"pico.admin_listen",
        c"Address of the admin listener; empty disables it",
        &ADMIN_LISTEN,
    );
    text(
        c"pico.kafka_listen",
        c"Address of the Kafka listener; empty disables it",
        &KAFKA_LISTEN,
    );
    text(
        c"pico.advertised_url",
        c"URL advertised to clients for redirects",
        &ADVERTISED_URL,
    );
    text(
        c"pico.kafka_advertise",
        c"host:port advertised in Kafka metadata",
        &KAFKA_ADVERTISE,
    );
    text(c"pico.cluster_id", c"Cluster identifier", &CLUSTER_ID);
    text(c"pico.auth", c"off or required", &AUTH);
    text(c"pico.log", c"Tracing filter for pico's log output", &LOG);
    text(
        c"pico.schema_registry",
        c"Bucket URI for the schema registry; empty disables it",
        &SCHEMA_REGISTRY,
    );
    secret(
        c"pico.auth_bootstrap_token",
        c"Root token seeded at startup",
        &BOOTSTRAP_TOKEN,
    );
    number(c"pico.node_id", c"Node identifier", &NODE_ID, 0, i32::MAX);
    number(
        c"pico.wal_cache_mb",
        c"Memory for records not yet packed into objects",
        &WAL_CACHE_MB,
        16,
        i32::MAX,
    );
    number(
        c"pico.wal_upload_interval_ms",
        c"Upload buffered records to object storage at least this often; 0 uploads by size only",
        &WAL_UPLOAD_INTERVAL_MS,
        0,
        i32::MAX,
    );
    number(
        c"pico.block_cache_mb",
        c"Memory for cached object blocks",
        &BLOCK_CACHE_MB,
        16,
        i32::MAX,
    );
    number(c"pico.threads", c"Worker threads", &THREADS, 1, 256);
    GucRegistry::define_bool_guc(
        c"pico.insecure_allow_remote",
        c"Permit non-loopback listeners with auth off",
        c"",
        &INSECURE_ALLOW_REMOTE,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}

fn text(name: &'static CStr, description: &'static CStr, setting: &'static Text) {
    string(name, description, setting, GucFlags::default());
}

fn secret(name: &'static CStr, description: &'static CStr, setting: &'static Text) {
    string(
        name,
        description,
        setting,
        GucFlags::SUPERUSER_ONLY | GucFlags::NO_SHOW_ALL,
    );
}

fn string(
    name: &'static CStr,
    description: &'static CStr,
    setting: &'static Text,
    flags: GucFlags,
) {
    GucRegistry::define_string_guc(
        name,
        description,
        c"",
        setting,
        GucContext::Postmaster,
        flags,
    );
}

fn number(
    name: &'static CStr,
    description: &'static CStr,
    setting: &'static GucSetting<i32>,
    min: i32,
    max: i32,
) {
    GucRegistry::define_int_guc(
        name,
        description,
        c"",
        setting,
        min,
        max,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}

fn value(setting: &Text) -> Option<String> {
    setting
        .get()
        .map(|value| text_of(&value))
        .filter(|value| !value.is_empty())
}

fn text_of(value: &CStr) -> String {
    value.to_string_lossy().into_owned()
}
