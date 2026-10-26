//! Stream commands.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use picomq_client::{
    ClientConfig, ClientError, ConfigUpdate, CreateOptions, ErrorKind, Live, PicoClient, Protocol,
    ReadLimits, TopicConfig,
};

use crate::io::{note, print_record, stdin_records};
use crate::schema::SchemaCommand;

const LS_PAGE: u64 = 1_000;

#[derive(Debug, Args)]
pub struct Endpoint {
    /// Server base URL. Defaults to the profile's, else http://127.0.0.1:4437.
    #[arg(long, env = "PICO_ENDPOINT", global = true)]
    pub endpoint: Option<String>,

    /// Which protocol: the wire client commands speak, and the HTTP frontend
    /// `serve` serves (`pico` or `ds`; `serve` always adds the Kafka listener).
    /// Defaults to the profile's, else `pico`.
    #[arg(long, value_enum, env = "PICO_PROTOCOL", global = true)]
    pub protocol: Option<ProtocolArg>,

    /// Speak HTTP/2 only, without falling back to HTTP/1.1. One connection
    /// then carries many concurrent requests, which is what deep append
    /// pipelines need.
    #[arg(long, env = "PICO_HTTP2", global = true)]
    pub http2: bool,

    /// Saved profile to take connection defaults from (`pico config`).
    #[arg(long, env = "PICO_PROFILE", global = true)]
    pub profile: Option<String>,

    /// Bearer token. Defaults to the one stored for the profile
    /// (`pico auth login`).
    #[arg(long, env = "PICO_TOKEN", global = true, hide_env_values = true)]
    pub token: Option<String>,

    /// PEM file of CA certificates to trust for https endpoints, on top of
    /// the system's. Needed when a node's certificate comes from a private
    /// CA.
    #[arg(long, env = "PICO_CA_CERTIFICATE", global = true)]
    pub ca_certificate: Option<PathBuf>,
}

impl Endpoint {
    pub fn ca_certificate(&self) -> Result<Option<Vec<u8>>, String> {
        self.ca_certificate
            .as_ref()
            .map(|path| {
                std::fs::read(path)
                    .map_err(|error| format!("read CA certificate {}: {error}", path.display()))
            })
            .transpose()
    }

    pub fn resolve(self) -> Result<Target, String> {
        let profile = crate::config::selected(self.profile.as_deref())?;
        let ca_certificate = self.ca_certificate()?;
        let token = match self.token {
            Some(token) => Some(token),
            None => crate::auth::lookup(&crate::auth::profile_name(self.profile.as_deref())?)?,
        };
        Ok(Target {
            endpoint: self
                .endpoint
                .or(profile.endpoint)
                .unwrap_or_else(|| "http://127.0.0.1:4437".to_owned()),
            protocol: self
                .protocol
                .or(profile.protocol)
                .unwrap_or(ProtocolArg::Pico),
            http2: self.http2 || profile.http2.unwrap_or(false),
            token,
            ca_certificate,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Target {
    pub endpoint: String,
    pub protocol: ProtocolArg,
    pub http2: bool,
    pub token: Option<String>,
    pub ca_certificate: Option<Vec<u8>>,
}

impl Target {
    pub fn client_config(&self) -> ClientConfig {
        ClientConfig {
            http2: self.http2,
            token: self.token.clone(),
            ca_certificate: self.ca_certificate.clone(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProtocolArg {
    Pico,
    Ds,
    Kafka,
}

impl ProtocolArg {
    pub fn client_protocol(self) -> Result<Protocol, ClientError> {
        match self {
            Self::Pico => Ok(Protocol::Pico),
            Self::Ds => Ok(Protocol::Ds),
            Self::Kafka => Err(ClientError::unsupported(
                "kafka has no CLI client; use kcat or a Kafka client library",
            )),
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum StreamCommand {
    /// Create a topic (idempotent with the same settings).
    Create {
        stream: String,
        #[arg(long, default_value = "application/octet-stream")]
        content_type: String,
        /// Number of partitions.
        #[arg(long)]
        partitions: Option<u32>,
        /// Expire the topic after this many seconds without reads or appends.
        #[arg(long, conflicts_with = "expires_at")]
        ttl: Option<u64>,
        /// Expire the topic at this RFC 3339 time.
        #[arg(long)]
        expires_at: Option<String>,
        /// Keep data written within this many milliseconds.
        #[arg(long)]
        retention_ms: Option<u64>,
        /// Keep at least the newest this many bytes per partition.
        #[arg(long)]
        retention_bytes: Option<u64>,
        /// Bind this registered schema.
        #[arg(long)]
        schema: Option<String>,
        /// Validate appends against the bound schema.
        #[arg(long, requires = "schema")]
        schema_validate: bool,
        /// Create the topic already sealed.
        #[arg(long)]
        closed: bool,
    },
    /// Print a topic's settings.
    Describe { stream: String },
    /// Change a topic's settings. `none` removes a setting.
    Alter {
        stream: String,
        /// Grow the topic to this many partitions. Partitions are never removed.
        #[arg(long)]
        partitions: Option<u32>,
        #[arg(long, value_name = "MS|none")]
        retention_ms: Option<String>,
        #[arg(long, value_name = "BYTES|none")]
        retention_bytes: Option<String>,
        #[arg(long, value_name = "NAME|none")]
        schema: Option<String>,
        #[arg(long)]
        schema_validate: Option<bool>,
    },
    /// Register, print, or remove schemas.
    #[command(subcommand)]
    Schema(SchemaCommand),
    /// Print a stream's position and metadata.
    Head {
        stream: String,
        /// Partition of a multi-partition topic.
        #[arg(long)]
        partition: Option<u32>,
    },
    /// Append newline-delimited records from stdin.
    Append {
        stream: String,
        /// Partition of a multi-partition topic.
        #[arg(long)]
        partition: Option<u32>,
        #[arg(long, default_value = "application/octet-stream")]
        content_type: String,
        /// Records per request. The Durable Streams protocol allows only 1.
        #[arg(long, default_value_t = 1)]
        batch: usize,
    },
    /// Read records until caught up.
    Read {
        stream: String,
        /// Partition of a multi-partition topic.
        #[arg(long)]
        partition: Option<u32>,
        /// Position to read from. Defaults to the start of the stream.
        #[arg(long)]
        from: Option<String>,
    },
    /// Read records appended from now on.
    Tail {
        stream: String,
        /// Partition of a multi-partition topic.
        #[arg(long)]
        partition: Option<u32>,
        /// Keep waiting for new records instead of returning when caught up.
        #[arg(short, long)]
        follow: bool,
    },
    /// List streams under a prefix (Pico protocol only).
    Ls {
        #[arg(long, default_value = "/")]
        prefix: String,
        #[arg(long, default_value_t = 0)]
        limit: u64,
    },
    /// Seal a stream against further appends.
    Close { stream: String },
    /// Delete a stream and its records.
    Delete { stream: String },
    /// Drop records below a sequence number (Pico protocol only).
    Trim {
        stream: String,
        /// Partition of a multi-partition topic.
        #[arg(long)]
        partition: Option<u32>,
        #[arg(long)]
        seq: u64,
    },
}

pub async fn run(endpoint: &Target, command: StreamCommand) -> Result<i32, ClientError> {
    let protocol = endpoint.protocol.client_protocol()?;
    let client =
        picomq_client::connect_with(protocol, &endpoint.endpoint, &endpoint.client_config())?;

    match command {
        StreamCommand::Create {
            stream,
            content_type,
            partitions,
            ttl,
            expires_at,
            retention_ms,
            retention_bytes,
            schema,
            schema_validate,
            closed,
        } => {
            let options = CreateOptions {
                partitions,
                ttl_seconds: ttl,
                expires_at,
                retention_ms,
                retention_bytes,
                schema,
                schema_validate,
                closed,
                ..CreateOptions::new(content_type)
            };
            let created = client.create(&stream, &options).await?;
            note(format!("created={created}"));
        }
        StreamCommand::Describe { stream } => {
            let (Some(head), Some(config)) = (
                client.head(&stream, None).await?,
                client.config(&stream).await?,
            ) else {
                note("not found");
                return Ok(1);
            };
            println!(
                "content_type={}",
                head.content_type.as_deref().unwrap_or("-")
            );
            println!("closed={}", head.closed);
            println!("ttl={}", or_dash(head.ttl_seconds));
            println!("expires_at={}", head.expires_at.as_deref().unwrap_or("-"));
            print_config(&config);
        }
        StreamCommand::Alter {
            stream,
            partitions,
            retention_ms,
            retention_bytes,
            schema,
            schema_validate,
        } => {
            let update = ConfigUpdate {
                stream,
                schema: alter_value(schema, "--schema")?,
                schema_validate,
                retention_ms: alter_value(retention_ms, "--retention-ms")?,
                retention_bytes: alter_value(retention_bytes, "--retention-bytes")?,
                partitions,
            };
            print_config(&client.update_config(&update).await?);
        }
        StreamCommand::Schema(command) => {
            return crate::schema::run(client.as_ref(), command).await;
        }
        StreamCommand::Head { stream, partition } => {
            let Some(head) = client.head(&stream, partition).await? else {
                note("not found");
                return Ok(1);
            };
            println!(
                "partitions={} start={} next={} closed={} type={}",
                head.partitions,
                head.start.as_deref().unwrap_or("-"),
                head.next.as_deref().unwrap_or("-"),
                head.closed,
                head.content_type.as_deref().unwrap_or("-")
            );
        }
        StreamCommand::Append {
            stream,
            partition,
            content_type,
            batch,
        } => {
            if batch == 0 {
                return Err(ClientError::unsupported("--batch must be at least 1"));
            }
            for records in
                stdin_records(batch).map_err(|e| ClientError::transport(e.to_string()))?
            {
                let ack = client
                    .append(&stream, partition, &records, &content_type)
                    .await?;
                note(format!("start={} next={}", ack.start, ack.next));
            }
        }
        StreamCommand::Read {
            stream,
            partition,
            from,
        } => {
            let mut next = from.unwrap_or_else(|| client.beginning());
            loop {
                let page = client
                    .read(
                        &stream,
                        partition,
                        &next,
                        Live::Off,
                        ReadLimits::server_default(),
                    )
                    .await?;
                for record in &page.records {
                    print_record(record);
                }
                next = page.next;
                if page.up_to_date || page.records.is_empty() {
                    break;
                }
            }
        }
        StreamCommand::Tail {
            stream,
            partition,
            follow,
        } => {
            let Some(head) = client.head(&stream, partition).await? else {
                return Err(ClientError::new(404, ErrorKind::NotFound, "not_found"));
            };
            if partition.is_none() && head.partitions > 1 {
                return Err(ClientError::new(0, ErrorKind::BadRequest, "bad_request")
                    .with_message(Some(format!(
                        "{stream} has {} partitions; pass --partition",
                        head.partitions
                    ))));
            }

            let mut next = match client.now() {
                Ok(now) => now,
                Err(_) => head.next.ok_or_else(|| {
                    ClientError::new(0, ErrorKind::Other, "invalid_response")
                        .with_message(Some(format!("HEAD on {stream} reported no next position")))
                })?,
            };

            loop {
                let page = client
                    .read(
                        &stream,
                        partition,
                        &next,
                        Live::LongPoll,
                        ReadLimits::server_default(),
                    )
                    .await?;
                for record in &page.records {
                    print_record(record);
                }
                next = page.next;
                if page.closed && page.up_to_date {
                    break;
                }
                if !follow && page.up_to_date {
                    break;
                }
                if page.records.is_empty() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        StreamCommand::Ls { prefix, limit } => {
            let mut cursor: Option<String> = None;
            let mut listed = 0;

            'pages: loop {
                let page = client.list(&prefix, LS_PAGE, cursor.as_deref()).await?;
                for stream in &page.streams {
                    if limit > 0 && listed == limit {
                        break 'pages;
                    }
                    println!(
                        "{} partitions={} closed={}",
                        stream.name, stream.partitions, stream.closed
                    );
                    listed += 1;
                }

                match page.next {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
        }
        StreamCommand::Close { stream } => {
            note(format!("closed next={}", client.close(&stream).await?));
        }
        StreamCommand::Delete { stream } => {
            note(format!("deleted={}", client.delete(&stream).await?));
        }
        StreamCommand::Trim {
            stream,
            partition,
            seq,
        } => {
            if protocol != Protocol::Pico {
                return Err(ClientError::unsupported(
                    "trim is a Pico protocol operation; use --protocol pico",
                ));
            }
            let start = PicoClient::with_config(&endpoint.endpoint, &endpoint.client_config())?
                .trim(&stream, partition, seq)
                .await?;
            note(format!("trimmed start={start}"));
        }
    }

    Ok(0)
}

fn print_config(config: &TopicConfig) {
    println!("partitions={}", config.partitions);
    println!("retention.ms={}", or_dash(config.retention_ms));
    println!("retention.bytes={}", or_dash(config.retention_bytes));
    println!("schema={}", config.schema.as_deref().unwrap_or("-"));
    println!("schema_validate={}", config.schema_validate);
    println!(
        "kafka_topic={}",
        config.kafka_topic.as_deref().unwrap_or("-")
    );
}

fn or_dash(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn alter_value<T: FromStr>(
    value: Option<String>,
    flag: &str,
) -> Result<Option<Option<T>>, ClientError> {
    match value.as_deref() {
        None => Ok(None),
        Some("none") => Ok(Some(None)),
        Some(raw) => raw.parse().map(|value| Some(Some(value))).map_err(|_| {
            ClientError::new(0, ErrorKind::BadRequest, "bad_request")
                .with_message(Some(format!("{flag} takes a value or none, got {raw}")))
        }),
    }
}
