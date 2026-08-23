//! The `pico` binary: serve, stream commands, admin, bench, and config
//! profiles.

mod admin;
mod auth;
mod bench;
mod config;
mod io;
mod serve;
mod stream;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::admin::AdminArgs;
use crate::auth::AuthCommand;
use crate::bench::BenchArgs;
use crate::config::ConfigCommand;
use crate::serve::ServeArgs;
use crate::stream::{Endpoint, StreamCommand};

#[derive(Debug, Parser)]
#[command(
    name = "pico",
    version,
    about = "PicoMQ: durable streams on object storage"
)]
struct Cli {
    #[command(flatten)]
    endpoint: Endpoint,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
// Clap needs each args struct inline, and `serve` has far more flags than the
// client commands.
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Run a PicoMQ node and serve a stream protocol over HTTP.
    Serve(ServeArgs),

    #[command(flatten)]
    Stream(StreamCommand),

    /// Write and read a temporary stream, and report throughput and latency.
    Bench(BenchArgs),

    /// Inspect and operate a node via its admin listener.
    Admin(AdminArgs),

    /// Manage saved endpoints.
    #[command(subcommand)]
    Config(ConfigCommand),

    /// Manage stored credentials.
    #[command(subcommand)]
    Auth(AuthCommand),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    // A server narrates what it is doing. A client command should print only
    // what was asked for, so its logs stay at warnings unless RUST_LOG says
    // otherwise.
    let default = if matches!(cli.command, Command::Serve(_)) {
        "info"
    } else {
        "warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default)),
        )
        .init();

    match run(cli).await {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("pico: {error}");
            std::process::exit(1);
        }
    }
}

async fn run(cli: Cli) -> Result<i32, Box<dyn std::error::Error>> {
    let Cli { endpoint, command } = cli;
    // `config` edits the file the others read, so it must not need to resolve
    // against it (a broken profile would then be unfixable).
    if let Command::Config(command) = command {
        return Ok(config::run(command, &endpoint)?);
    }
    // Admin talks to the admin listener, not the stream endpoint, so it does
    // not resolve a profile.
    if let Command::Admin(args) = command {
        return Ok(admin::run(args).await?);
    }
    // Auth edits the stored credential the others read, so like `config` it
    // must not need a resolvable endpoint.
    if let Command::Auth(command) = command {
        return Ok(auth::run(command, &endpoint).await?);
    }
    let target = endpoint.resolve()?;
    match command {
        Command::Config(_) | Command::Admin(_) | Command::Auth(_) => unreachable!("handled above"),
        Command::Stream(command) => Ok(stream::run(&target, command).await?),
        Command::Bench(args) => Ok(bench::run(&target, args).await?),
        Command::Serve(args) => {
            let server = pico_runtime::start(args.into_config(target.protocol.into())?).await?;
            println!(
                "serving on http://{} (advertised {})",
                server.local_addr(),
                server.base_url()
            );
            if let Some(admin) = server.admin_addr() {
                println!("admin on http://{admin}");
            }
            tokio::signal::ctrl_c().await?;
            println!("shutting down");
            server.shutdown().await;
            Ok(0)
        }
    }
}
