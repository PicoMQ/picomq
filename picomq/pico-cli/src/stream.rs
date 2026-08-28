//! Stream commands.
//!
//! The commands are written once against `pico_client::StreamApi` and
//! `--protocol` picks the wire, so the
//! two protocols cannot drift in the CLI's behavior. Positions are printed and
//! accepted as opaque strings (`--from`), which is a Pico `seq` or a Durable
//! Streams offset token depending on the protocol.

use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use pico_client::{ClientConfig, ClientError, ErrorKind, Live, PicoClient, Protocol, ReadLimits};

use crate::io::{note, print_record, stdin_records};

/// The connection flags, before a profile is applied.
///
/// Precedence: flag, then environment, then a saved profile, then the local
/// default.
#[derive(Debug, Args)]
pub struct Endpoint {
    /// Server base URL. Defaults to the profile's, else http://127.0.0.1:4437.
    #[arg(long, env = "PICO_ENDPOINT", global = true)]
    pub endpoint: Option<String>,

    /// Which protocol: the wire client commands speak, and the frontend
    /// `serve` serves. Defaults to the profile's, else `pico`.
    #[arg(long, value_enum, env = "PICO_PROTOCOL", global = true)]
    pub protocol: Option<ProtocolArg>,

    /// Speak HTTP/2 over cleartext. One connection then carries many
    /// concurrent requests, which is what deep append pipelines need. The
    /// server must have HTTP/2 enabled.
    #[arg(long, env = "PICO_HTTP2", global = true)]
    pub http2: bool,

    /// Saved profile to take connection defaults from (`pico config`).
    #[arg(long, env = "PICO_PROFILE", global = true)]
    pub profile: Option<String>,

    /// Bearer token. Defaults to the one stored for the profile
    /// (`pico auth login`).
    #[arg(long, env = "PICO_TOKEN", global = true, hide_env_values = true)]
    pub token: Option<String>,
}

impl Endpoint {
    /// Layer the flags over the selected profile.
    pub fn resolve(self) -> Result<Target, String> {
        let profile = crate::config::selected(self.profile.as_deref())?;
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
            // A bare flag cannot say "off", so a profile can only turn this on.
            http2: self.http2 || profile.http2.unwrap_or(false),
            token,
        })
    }
}

/// Where to talk, and how.
#[derive(Debug, Clone)]
pub struct Target {
    pub endpoint: String,
    pub protocol: ProtocolArg,
    pub http2: bool,
    pub token: Option<String>,
}

impl Target {
    pub fn client_config(&self) -> ClientConfig {
        ClientConfig {
            http2: self.http2,
            token: self.token.clone(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProtocolArg {
    /// The Pico protocol: record batches, numeric sequences, stream listing.
    Pico,
    /// The Durable Streams open protocol: raw bodies, opaque offsets.
    Ds,
    /// The Kafka wire protocol. Serve only: client commands use Kafka tooling.
    Kafka,
}

impl ProtocolArg {
    /// The client wire for stream commands. Kafka has no CLI client.
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

impl From<ProtocolArg> for pico_http::Protocol {
    fn from(value: ProtocolArg) -> Self {
        match value {
            ProtocolArg::Pico => Self::Pico,
            ProtocolArg::Ds => Self::Ds,
            ProtocolArg::Kafka => Self::Kafka,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum StreamCommand {
    /// Create a stream (idempotent).
    Create {
        stream: String,
        #[arg(long, default_value = "application/octet-stream")]
        content_type: String,
        /// Delete records older than this many seconds.
        #[arg(long)]
        ttl: Option<u64>,
    },
    /// Print a stream's position and metadata.
    Head { stream: String },
    /// Append newline-delimited records from stdin.
    Append {
        stream: String,
        #[arg(long, default_value = "application/octet-stream")]
        content_type: String,
        /// Records per request. The Durable Streams protocol allows only 1.
        #[arg(long, default_value_t = 1)]
        batch: usize,
    },
    /// Read records until caught up.
    Read {
        stream: String,
        /// Position to read from. Defaults to the start of the stream.
        #[arg(long)]
        from: Option<String>,
    },
    /// Read records appended from now on.
    Tail {
        stream: String,
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
        #[arg(long)]
        seq: u64,
    },
}

pub async fn run(endpoint: &Target, command: StreamCommand) -> Result<i32, ClientError> {
    let protocol = endpoint.protocol.client_protocol()?;
    let client =
        pico_client::connect_with(protocol, &endpoint.endpoint, &endpoint.client_config())?;

    match command {
        StreamCommand::Create {
            stream,
            content_type,
            ttl,
        } => {
            let created = client.create(&stream, &content_type, ttl).await?;
            note(format!("created={created}"));
        }
        StreamCommand::Head { stream } => {
            let Some(head) = client.head(&stream).await? else {
                note("not found");
                return Ok(1);
            };
            println!(
                "start={} next={} closed={} type={}",
                head.start,
                head.next,
                head.closed,
                head.content_type.as_deref().unwrap_or("-")
            );
        }
        StreamCommand::Append {
            stream,
            content_type,
            batch,
        } => {
            if batch == 0 {
                return Err(ClientError::unsupported("--batch must be at least 1"));
            }
            for records in
                stdin_records(batch).map_err(|e| ClientError::transport(e.to_string()))?
            {
                let ack = client.append(&stream, &records, &content_type).await?;
                note(format!("start={} next={}", ack.start, ack.next));
            }
        }
        StreamCommand::Read { stream, from } => {
            let mut next = from.unwrap_or_else(|| client.beginning());
            loop {
                let page = client
                    .read(&stream, &next, Live::Off, ReadLimits::server_default())
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
        StreamCommand::Tail { stream, follow } => {
            let mut next = match client.now() {
                Ok(now) => now,
                Err(_) => match client.head(&stream).await? {
                    Some(head) => head.next,
                    None => return Err(ClientError::new(404, ErrorKind::NotFound, "not_found")),
                },
            };
            loop {
                let page = client
                    .read(&stream, &next, Live::LongPoll, ReadLimits::server_default())
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
                    // The long poll already waited server-side. This only
                    // keeps a closed-then-reopened loop from spinning.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        StreamCommand::Ls { prefix, limit } => {
            let listing = client.list(&prefix, limit).await?;
            for stream in &listing.streams {
                println!(
                    "{} start={} next={} closed={}",
                    stream.name, stream.start, stream.next, stream.closed
                );
            }
            if listing.has_more {
                note("more streams available, raise --limit");
            }
        }
        StreamCommand::Close { stream } => {
            note(format!("closed next={}", client.close(&stream).await?));
        }
        StreamCommand::Delete { stream } => {
            note(format!("deleted={}", client.delete(&stream).await?));
        }
        StreamCommand::Trim { stream, seq } => {
            if protocol != Protocol::Pico {
                return Err(ClientError::unsupported(
                    "trim is a Pico protocol operation; use --protocol pico",
                ));
            }
            let http = pico_client::http_client(&endpoint.client_config())?;
            let start = PicoClient::with_http(&endpoint.endpoint, http, Default::default())
                .trim(&stream, seq)
                .await?;
            note(format!("trimmed start={start}"));
        }
    }

    Ok(0)
}
