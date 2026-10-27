//! Admin commands, spoken to a node's admin listener rather than the stream
//! protocol endpoint. Data goes to stdout, acknowledgements to stderr.

use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use picomq_client::ClientConfig;
use picomq_protocol::{check_path, stream_segment};
use serde_json::Value;

use crate::io::note;
use crate::stream::Endpoint;

const TRANSFER_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const TRANSFER_WAIT_POLL: Duration = Duration::from_millis(250);

#[derive(Debug, Args)]
pub struct AdminArgs {
    /// The node's admin listener base URL.
    #[arg(
        long,
        env = "PICO_ADMIN_ENDPOINT",
        default_value = "http://127.0.0.1:9090"
    )]
    pub admin_endpoint: String,

    /// Bearer token for the admin plane. Defaults to `PICO_TOKEN`, then the
    /// token stored for the default profile (`pico auth login`).
    #[arg(long, env = "PICO_ADMIN_TOKEN", hide_env_values = true)]
    pub token: Option<String>,

    /// Print raw JSON instead of formatted lines.
    #[arg(long)]
    pub json: bool,

    #[command(subcommand)]
    pub command: AdminCommand,
}

#[derive(Debug, Subcommand)]
pub enum AdminCommand {
    /// Cluster summary: identity and counts.
    Cluster,
    /// List registered nodes.
    Nodes,
    /// List in-flight partition moves.
    Transfers,
    /// One partition of a stream: owner, state, offsets, size.
    Stream {
        stream: String,
        /// The partition to inspect.
        #[arg(long, default_value_t = 0)]
        partition: u32,
    },
    /// Move a stream to another node.
    Transfer {
        stream: String,
        #[arg(long)]
        to_node: i32,
        /// The partition to move, required when the topic has more than one.
        #[arg(long)]
        partition: Option<u32>,
        /// Poll until the move completes.
        #[arg(long)]
        wait: bool,
    },
    /// Update a node's placement slot count.
    SetSlots {
        node: i32,
        #[arg(long)]
        slots: u32,
    },
}

pub async fn run(args: AdminArgs, flags: &Endpoint) -> Result<i32, String> {
    let token = match args.token {
        Some(token) => Some(token),
        None => match std::env::var("PICO_TOKEN") {
            Ok(token) => Some(token),
            Err(_) => crate::auth::lookup(&crate::auth::profile_name(None)?)?,
        },
    };
    let api = Api {
        base: args.admin_endpoint.trim_end_matches('/').to_owned(),
        http: picomq_client::http_client(&ClientConfig {
            ca_certificate: flags.ca_certificate()?,
            ..Default::default()
        })
        .map_err(|error| error.to_string())?,
        token,
    };
    match args.command {
        AdminCommand::Cluster => {
            let body = api.get("/admin/cluster").await?;
            if args.json {
                return print_json(&body);
            }
            println!(
                "cluster={} node={} epoch={} address={} registered={} appliedIndex={}",
                body["clusterId"].as_str().unwrap_or("-"),
                body["nodeId"],
                body["nodeEpoch"],
                body["advertisedAddress"].as_str().unwrap_or("-"),
                body["registered"],
                body["appliedIndex"],
            );
            println!(
                "streams={} objects={} gcBacklog={} pendingTransfers={} leaseHolder={}",
                body["streamCount"],
                body["objectCount"],
                body["gc"]["backlog"],
                body["pendingTransferCount"],
                body["leaseHolder"],
            );
        }
        AdminCommand::Nodes => {
            let nodes = api.every("/admin/nodes").await?;
            if args.json {
                return print_json(&Value::Array(nodes));
            }
            for node in &nodes {
                println!(
                    "node={} epoch={} address={} slots={} open={} closed={} fenced={}{}",
                    node["nodeId"],
                    node["nodeEpoch"],
                    node["advertisedAddress"].as_str().unwrap_or("-"),
                    node["slots"],
                    node["openingCount"],
                    node["placedCount"],
                    node["fenced"],
                    if node["local"] == Value::Bool(true) {
                        " (this node)"
                    } else {
                        ""
                    },
                );
            }
        }
        AdminCommand::Transfers => {
            let transfers = api.every("/admin/transfers").await?;
            if args.json {
                return print_json(&Value::Array(transfers));
            }
            for transfer in &transfers {
                let log = &transfer["log"];
                let moved = match (log["topic"].as_str(), log["group"].as_str()) {
                    (Some(topic), _) => format!("topic={topic} partition={}", log["partition"]),
                    (None, Some(group)) => format!("group={group}"),
                    (None, None) => "log=-".to_owned(),
                };
                println!(
                    "{moved} from={} to={}",
                    transfer["fromNode"], transfer["toNode"],
                );
            }
        }
        AdminCommand::Stream { stream, partition } => {
            let body = api.get(&partition_path(&stream, partition)).await?;
            if args.json {
                return print_json(&body);
            }
            println!(
                "stream={stream} partition={} id={} owner={} state={} epoch={}",
                body["index"],
                body["streamId"],
                body["ownerNodeId"],
                body["state"].as_str().unwrap_or("-"),
                body["epoch"],
            );
            println!(
                "start={} next={} current={} stored_bytes={}",
                body["startOffset"], body["nextOffset"], body["current"], body["storedBytes"],
            );
            if body["pendingTransfer"].is_object() {
                println!(
                    "transfer from={} to={}",
                    body["pendingTransfer"]["fromNode"], body["pendingTransfer"]["toNode"],
                );
            }
        }
        AdminCommand::Transfer {
            stream,
            to_node,
            partition,
            wait,
        } => {
            let mut request = serde_json::json!({ "topic": stream, "toNode": to_node });
            if let Some(partition) = partition {
                request["partition"] = partition.into();
            }

            let body = api.post("/admin/transfers", &request).await?;
            note(format!(
                "transfer accepted stream={} partition={} to={}",
                stream, body["partition"], to_node
            ));
            if wait {
                let moved = body["partition"]
                    .as_u64()
                    .and_then(|partition| u32::try_from(partition).ok())
                    .ok_or("the transfer reply names no partition")?;
                await_transfer(&api, &stream, moved, to_node).await?;
                note("transfer complete");
            }
            if args.json {
                return print_json(&body);
            }
        }
        AdminCommand::SetSlots { node, slots } => {
            let body = api
                .post(
                    &format!("/admin/nodes/{node}"),
                    &serde_json::json!({ "slots": slots }),
                )
                .await?;
            note(format!("node={} slots={}", body["nodeId"], body["slots"]));
            if args.json {
                return print_json(&body);
            }
        }
    }
    Ok(0)
}

struct Api {
    base: String,
    http: reqwest::Client,
    token: Option<String>,
}

impl Api {
    fn credentialed(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    fn url(&self, path: &str) -> Result<String, String> {
        check_path(path).map_err(|error| error.message)?;
        Ok(format!("{}{path}", self.base))
    }

    async fn get(&self, path: &str) -> Result<Value, String> {
        let response = self
            .credentialed(self.http.get(self.url(path)?))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        decode(response).await
    }

    async fn every(&self, path: &str) -> Result<Vec<Value>, String> {
        let mut items = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let page = match &cursor {
                Some(cursor) => self.get(&format!("{path}?cursor={cursor}")).await?,
                None => self.get(path).await?,
            };
            items.extend(page["items"].as_array().cloned().unwrap_or_default());
            match page["next"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => return Ok(items),
            }
        }
    }

    async fn post(&self, path: &str, body: &Value) -> Result<Value, String> {
        let response = self
            .credentialed(self.http.post(self.url(path)?))
            .json(body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        decode(response).await
    }
}

/// A failing response carries `{"error": message}`. Surface that message,
/// not just the status code.
async fn decode(response: reqwest::Response) -> Result<Value, String> {
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .map_err(|e| format!("{status}: {e}"))?;
    if status.is_success() {
        return Ok(body);
    }
    match body["error"].as_str() {
        Some(message) => Err(format!("{status}: {message}")),
        None => Err(format!("{status}: {body}")),
    }
}

fn partition_path(stream: &str, partition: u32) -> String {
    format!(
        "/admin/topics/{}/partitions/{partition}",
        stream_segment(stream)
    )
}

/// The move is done once no transfer is pending and the row points at the
/// target.
async fn await_transfer(
    api: &Api,
    stream: &str,
    partition: u32,
    to_node: i32,
) -> Result<(), String> {
    let deadline = Instant::now() + TRANSFER_WAIT_TIMEOUT;
    loop {
        let body = api.get(&partition_path(stream, partition)).await?;
        if body["pendingTransfer"].is_null() && body["ownerNodeId"] == to_node {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("transfer of {stream} did not settle in time"));
        }
        tokio::time::sleep(TRANSFER_WAIT_POLL).await;
    }
}

fn print_json(body: &Value) -> Result<i32, String> {
    println!(
        "{}",
        serde_json::to_string_pretty(body).map_err(|e| e.to_string())?
    );
    Ok(0)
}
