//! Admin commands, spoken to a node's admin listener rather than the stream
//! protocol endpoint. Data goes to stdout, acknowledgements to stderr.

use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use serde_json::Value;

use crate::io::note;

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
    /// Cluster overview: identity, counts, pending transfers.
    Cluster,
    /// List registered nodes.
    Nodes,
    /// Stream detail by name.
    Stream { stream: String },
    /// Move a stream to another node.
    Transfer {
        stream: String,
        #[arg(long)]
        to_node: i32,
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

pub async fn run(args: AdminArgs) -> Result<i32, String> {
    let token = match args.token {
        Some(token) => Some(token),
        None => match std::env::var("PICO_TOKEN") {
            Ok(token) => Some(token),
            Err(_) => crate::auth::lookup(&crate::auth::profile_name(None)?)?,
        },
    };
    let api = Api {
        base: args.admin_endpoint.trim_end_matches('/').to_owned(),
        http: reqwest::Client::new(),
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
                "streams={} objects={} gcBacklog={} leaseHolder={}",
                body["streamCount"],
                body["objectCount"],
                body["gc"]["backlog"],
                body["leaseHolder"],
            );
            for pending in body["pendingTransfers"].as_array().into_iter().flatten() {
                println!(
                    "transfer stream={} from={} to={}",
                    pending["streamId"], pending["fromNode"], pending["toNode"],
                );
            }
        }
        AdminCommand::Nodes => {
            let body = api.get("/admin/nodes").await?;
            if args.json {
                return print_json(&body);
            }
            for node in body["nodes"].as_array().into_iter().flatten() {
                println!(
                    "node={} epoch={} address={} slots={} opening={} placed={}{}",
                    node["nodeId"],
                    node["nodeEpoch"],
                    node["advertisedAddress"].as_str().unwrap_or("-"),
                    node["slots"],
                    node["openingCount"],
                    node["placedCount"],
                    if node["local"] == Value::Bool(true) {
                        " (this node)"
                    } else {
                        ""
                    },
                );
            }
        }
        AdminCommand::Stream { stream } => {
            let body = api.get(&stream_path(&stream)).await?;
            if args.json {
                return print_json(&body);
            }
            println!(
                "name={} id={} owner={} address={} local={}",
                body["name"].as_str().unwrap_or("-"),
                body["streamId"],
                body["ownerNodeId"],
                body["ownerAdvertisedAddress"].as_str().unwrap_or("-"),
                body["ownerLocal"],
            );
            println!(
                "state={} epoch={} start={} end={} closed={} type={}",
                body["state"].as_str().unwrap_or("-"),
                body["epoch"],
                body["startOffset"],
                body["endOffset"],
                body["closed"],
                body["contentType"].as_str().unwrap_or("-"),
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
            wait,
        } => {
            let body = api
                .post(
                    "/admin/transfer",
                    &serde_json::json!({ "stream": stream, "toNode": to_node }),
                )
                .await?;
            note(format!(
                "transfer accepted stream={} id={} to={}",
                stream, body["streamId"], to_node
            ));
            if wait {
                await_transfer(&api, &stream, to_node).await?;
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

    async fn get(&self, path: &str) -> Result<Value, String> {
        let response = self
            .credentialed(self.http.get(format!("{}{path}", self.base)))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        decode(response).await
    }

    async fn post(&self, path: &str, body: &Value) -> Result<Value, String> {
        let response = self
            .credentialed(self.http.post(format!("{}{path}", self.base)))
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

fn stream_path(stream: &str) -> String {
    let name = stream.strip_prefix('/').unwrap_or(stream);
    format!("/admin/streams/{name}")
}

/// The move is done once no transfer is pending and the row points at the
/// target.
async fn await_transfer(api: &Api, stream: &str, to_node: i32) -> Result<(), String> {
    let deadline = Instant::now() + TRANSFER_WAIT_TIMEOUT;
    loop {
        let body = api.get(&stream_path(stream)).await?;
        if body["pendingTransfer"].is_null() && body["nodeId"] == to_node {
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
