//! Optional Avro / JSON Schema / Protobuf validation.

use std::{
    collections::BTreeMap,
    io, result,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use jsonschema::ValidationError;
use object_store::{path::Path, DynObjectStore, ObjectStore};
use tracing::{debug, instrument};

pub mod avro;
pub mod json;
pub mod proto;
pub mod record;

pub use record::{Batch, Record};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Avro(Box<apache_avro::Error>),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("invalid record")]
    InvalidRecord,

    #[error("{0}")]
    Message(String),

    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),

    #[error("lock poisoned")]
    Poison,

    #[error(transparent)]
    ProtobufJsonMapping(#[from] protobuf_json_mapping::ParseError),

    #[error(transparent)]
    ProtobufJsonMappingPrint(#[from] protobuf_json_mapping::PrintError),

    #[error(transparent)]
    Protobuf(#[from] protobuf::Error),

    #[error("protobuf file descriptor missing")]
    ProtobufFileDescriptorMissing(Bytes),

    #[error("schema validation failed")]
    SchemaValidation,

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error("schema not found: {0}")]
    SchemaNotFound(String),

    #[error("unsupported schema format")]
    UnsupportedSchemaFormat,

    #[error("stream/topic without schema: {0}")]
    TopicWithoutSchema(String),

    #[error("{0}")]
    ProtobufParse(String),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

impl From<apache_avro::Error> for Error {
    fn from(value: apache_avro::Error) -> Self {
        Self::Avro(Box::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<ValidationError<'_>> for Error {
    fn from(_: ValidationError<'_>) -> Self {
        Self::SchemaValidation
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

pub trait Validator {
    fn validate(&self, batch: &Batch) -> Result<()>;
}

#[derive(Clone, Debug)]
pub enum Schema {
    Avro(Box<avro::Schema>),
    Json(Arc<json::Schema>),
    Proto(Box<proto::Schema>),
}

#[derive(Clone, Debug)]
struct CachedSchema {
    loaded_at: SystemTime,
    schema: Schema,
}

impl CachedSchema {
    fn new(schema: Schema) -> Self {
        Self {
            schema,
            loaded_at: SystemTime::now(),
        }
    }
}

impl Validator for Schema {
    #[instrument(skip(self, batch), ret)]
    fn validate(&self, batch: &Batch) -> Result<()> {
        match self {
            Self::Avro(schema) => schema.validate(batch),
            Self::Json(schema) => schema.validate(batch),
            Self::Proto(schema) => schema.validate(batch),
        }
    }
}

type SchemaCache = Arc<Mutex<BTreeMap<String, CachedSchema>>>;

#[derive(Clone, Debug)]
pub struct Registry {
    object_store: Arc<DynObjectStore>,
    schemas: SchemaCache,
    cache_expiry_after: Option<Duration>,
}

#[derive(Clone, Debug)]
pub struct Builder {
    object_store: Arc<DynObjectStore>,
    cache_expiry_after: Option<Duration>,
}

impl From<Arc<DynObjectStore>> for Builder {
    fn from(object_store: Arc<DynObjectStore>) -> Self {
        Self {
            object_store,
            cache_expiry_after: None,
        }
    }
}

impl From<Builder> for Registry {
    fn from(builder: Builder) -> Self {
        Self {
            object_store: builder.object_store,
            schemas: Arc::new(Mutex::new(BTreeMap::new())),
            cache_expiry_after: builder.cache_expiry_after,
        }
    }
}

impl Builder {
    pub fn new(object_store: impl ObjectStore) -> Self {
        Self {
            object_store: Arc::new(object_store),
            cache_expiry_after: None,
        }
    }

    pub fn with_cache_expiry_after(self, cache_expiry_after: Option<Duration>) -> Self {
        Self {
            cache_expiry_after,
            ..self
        }
    }

    pub fn build(self) -> Registry {
        Registry::from(self)
    }
}

fn schema_object_name(name: &str) -> &str {
    name.strip_prefix('/').unwrap_or(name)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaFormat {
    Avro,
    Json,
    Protobuf,
}

impl SchemaFormat {
    pub fn extension(self) -> &'static str {
        match self {
            Self::Avro => "avsc",
            Self::Json => "json",
            Self::Protobuf => "proto",
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            Self::Avro => "application/avro",
            Self::Json => "application/schema+json",
            Self::Protobuf => "application/x-protobuf",
        }
    }

    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext {
            "avsc" | "avro" => Some(Self::Avro),
            "json" => Some(Self::Json),
            "proto" | "protobuf" => Some(Self::Protobuf),
            _ => None,
        }
    }

    pub fn from_content_type(ct: &str) -> Option<Self> {
        let base = ct
            .split(';')
            .next()
            .unwrap_or(ct)
            .trim()
            .to_ascii_lowercase();
        match base.as_str() {
            "application/avro"
            | "application/vnd.apache.avro"
            | "application/avro+json"
            | "avro/binary" => Some(Self::Avro),
            "application/json"
            | "application/schema+json"
            | "application/schema+json; charset=utf-8" => Some(Self::Json),
            "application/x-protobuf"
            | "application/protobuf"
            | "application/vnd.google.protobuf"
            | "text/x-protobuf" => Some(Self::Protobuf),
            _ if base.ends_with("+json") && base.contains("schema") => Some(Self::Json),
            _ => None,
        }
    }
}

#[async_trait]
pub trait SchemaStore: Send + Sync {
    async fn schema(&self, name: &str) -> Result<Option<Schema>>;
    async fn put(&self, name: &str, format: SchemaFormat, bytes: Bytes) -> Result<()>;
    async fn get(&self, name: &str) -> Result<Option<(SchemaFormat, Bytes)>>;
    async fn delete(&self, name: &str) -> Result<bool>;
}

impl Registry {
    pub fn new(object_store: impl ObjectStore) -> Self {
        Builder::new(object_store).build()
    }

    pub fn builder(object_store: impl ObjectStore) -> Builder {
        Builder::new(object_store)
    }

    #[instrument(skip(self))]
    pub async fn schema(&self, name: &str) -> Result<Option<Schema>> {
        let name = schema_object_name(name);
        let proto = Path::from(format!("{name}.proto"));
        let json = Path::from(format!("{name}.json"));
        let avro = Path::from(format!("{name}.avsc"));

        if let Some(cached) = self.schemas.lock()?.get(name).cloned() {
            let expired = self.cache_expiry_after.is_some_and(|expiry| {
                SystemTime::now()
                    .duration_since(cached.loaded_at)
                    .unwrap_or_default()
                    > expiry
            });
            if !expired {
                return Ok(Some(cached.schema));
            }
            debug!(cache_expiry = name);
        }

        if let Ok(get_result) = self.object_store.get(&proto).await {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(proto::Schema::try_from)
                .map(Box::new)
                .map(Schema::Proto)
                .and_then(|schema| {
                    self.schemas
                        .lock()?
                        .insert(name.to_owned(), CachedSchema::new(schema.clone()));
                    Ok(Some(schema))
                })
        } else if let Ok(get_result) = self.object_store.get(&json).await {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(json::Schema::try_from)
                .map(Arc::new)
                .map(Schema::Json)
                .and_then(|schema| {
                    self.schemas
                        .lock()?
                        .insert(name.to_owned(), CachedSchema::new(schema.clone()));
                    Ok(Some(schema))
                })
        } else if let Ok(get_result) = self.object_store.get(&avro).await {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(avro::Schema::try_from)
                .map(Box::new)
                .map(Schema::Avro)
                .and_then(|schema| {
                    self.schemas
                        .lock()?
                        .insert(name.to_owned(), CachedSchema::new(schema.clone()));
                    Ok(Some(schema))
                })
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self, bytes))]
    pub async fn put(&self, name: &str, format: SchemaFormat, bytes: Bytes) -> Result<()> {
        let name = schema_object_name(name);
        if name.is_empty() {
            return Err(Error::Message("schema name is required".into()));
        }
        // Reject path escape / empty segments.
        if name
            .split('/')
            .any(|p| p.is_empty() || p == "." || p == "..")
        {
            return Err(Error::Message("invalid schema name".into()));
        }
        match format {
            SchemaFormat::Avro => {
                let _ = avro::Schema::try_from(bytes.clone())?;
            }
            SchemaFormat::Json => {
                let _ = json::Schema::try_from(bytes.clone())?;
            }
            SchemaFormat::Protobuf => {
                let _ = proto::Schema::try_from(bytes.clone())?;
            }
        }
        let path = Path::from(format!("{name}.{}", format.extension()));
        self.object_store
            .put(&path, object_store::PutPayload::from(bytes))
            .await?;
        // One format per name: drop siblings so lookup order cannot surprise.
        for other in [
            SchemaFormat::Protobuf,
            SchemaFormat::Json,
            SchemaFormat::Avro,
        ] {
            if other == format {
                continue;
            }
            let sibling = Path::from(format!("{name}.{}", other.extension()));
            let _ = self.object_store.delete(&sibling).await;
        }
        self.schemas.lock()?.remove(name);
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn get(&self, name: &str) -> Result<Option<(SchemaFormat, Bytes)>> {
        let name = schema_object_name(name);
        for format in [
            SchemaFormat::Protobuf,
            SchemaFormat::Json,
            SchemaFormat::Avro,
        ] {
            let path = Path::from(format!("{name}.{}", format.extension()));
            if let Ok(get_result) = self.object_store.get(&path).await {
                let bytes = get_result.bytes().await?;
                return Ok(Some((format, bytes)));
            }
        }
        Ok(None)
    }

    #[instrument(skip(self))]
    pub async fn delete(&self, name: &str) -> Result<bool> {
        let name = schema_object_name(name);
        let mut deleted = false;
        for format in [
            SchemaFormat::Protobuf,
            SchemaFormat::Json,
            SchemaFormat::Avro,
        ] {
            let path = Path::from(format!("{name}.{}", format.extension()));
            match self.object_store.delete(&path).await {
                Ok(()) => deleted = true,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        self.schemas.lock()?.remove(name);
        Ok(deleted)
    }

    #[instrument(skip(self, batch))]
    pub async fn validate(&self, name: &str, batch: &Batch) -> Result<()> {
        let Some(schema) = self.schema(name).await? else {
            debug!(no_schema_for = %name);
            return Ok(());
        };
        schema.validate(batch)
    }
}

#[async_trait]
impl SchemaStore for Registry {
    async fn schema(&self, name: &str) -> Result<Option<Schema>> {
        Registry::schema(self, name).await
    }

    async fn put(&self, name: &str, format: SchemaFormat, bytes: Bytes) -> Result<()> {
        Registry::put(self, name, format, bytes).await
    }

    async fn get(&self, name: &str) -> Result<Option<(SchemaFormat, Bytes)>> {
        Registry::get(self, name).await
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        Registry::delete(self, name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::{memory::InMemory, PutPayload};
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn memory_registry_loads_json_and_validates() -> Result<()> {
        let store = InMemory::new();
        let schema = br#"{
            "title": "Person",
            "type": "object",
            "properties": {
                "value": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" }
                    },
                    "required": ["name"]
                }
            }
        }"#;
        store
            .put(
                &Path::from("streams/orders.json"),
                PutPayload::from(Bytes::from_static(schema)),
            )
            .await?;

        let registry = Registry::new(store);
        let loaded = registry.schema("/streams/orders").await?;
        assert!(loaded.is_some());

        let ok = Batch::builder()
            .record(
                Record::builder()
                    .value(Bytes::from_static(br#"{"name":"alice"}"#))
                    .build(),
            )
            .build();
        registry.validate("/streams/orders", &ok).await?;

        let bad = Batch::builder()
            .record(
                Record::builder()
                    .value(Bytes::from_static(br#"{"name":1}"#))
                    .build(),
            )
            .build();
        assert!(matches!(
            registry.validate("/streams/orders", &bad).await,
            Err(Error::InvalidRecord)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn missing_schema_skips_validation() -> Result<()> {
        let registry = Registry::new(InMemory::new());
        let batch = Batch::builder()
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"anything"))
                    .build(),
            )
            .build();
        registry.validate("unknown", &batch).await?;
        Ok(())
    }

    #[test]
    fn schema_object_name_strips_slash() {
        assert_eq!(schema_object_name("/streams/orders"), "streams/orders");
        assert_eq!(schema_object_name("orders"), "orders");
    }

    #[tokio::test]
    async fn memory_registry_loads_proto_and_validates() -> Result<()> {
        let store = InMemory::new();
        let schema = br#"
syntax = "proto3";
message Key { int32 id = 1; }
message Value { string name = 1; }
"#;
        store
            .put(
                &Path::from("employee.proto"),
                PutPayload::from(Bytes::from_static(schema)),
            )
            .await?;

        let registry = Registry::new(store);
        assert!(registry.schema("employee").await?.is_some());

        let encoded = {
            let schema = proto::Schema::try_from(Bytes::from_static(schema))?;
            let key =
                schema.encode_from_value(proto::MessageKind::Key, &serde_json::json!({"id": 1}))?;
            let value = schema.encode_from_value(
                proto::MessageKind::Value,
                &serde_json::json!({"name": "bob"}),
            )?;
            (key, value)
        };
        let ok = Batch::builder()
            .record(Record::builder().key(encoded.0).value(encoded.1).build())
            .build();
        registry.validate("employee", &ok).await?;
        Ok(())
    }

    #[tokio::test]
    async fn put_get_delete_roundtrip() -> Result<()> {
        let store = InMemory::new();
        let registry = Registry::new(store);
        let body = Bytes::from_static(
            br#"{"title":"T","type":"object","properties":{"value":{"type":"object"}}}"#,
        );
        registry
            .put("shared/orders", SchemaFormat::Json, body.clone())
            .await?;
        let got = registry.get("shared/orders").await?.expect("present");
        assert_eq!(got.0, SchemaFormat::Json);
        assert_eq!(got.1, body);
        assert!(registry.delete("shared/orders").await?);
        assert!(registry.get("shared/orders").await?.is_none());
        Ok(())
    }
}
