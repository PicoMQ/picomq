mod certificates;
mod ds;
mod kafka;
mod known;
mod suite;
mod target;

use std::process::ExitCode;

use clap::Parser;

use crate::known::KnownFailures;
use crate::suite::Suite;
use crate::target::Target;

#[derive(Parser)]
#[command(
    name = "pico-conformance",
    about = "Run a protocol conformance suite against PicoMQ"
)]
struct Cli {
    #[arg(value_enum)]
    suite: Suite,

    #[arg(long, help = "HTTP URL of an existing node to run against")]
    target: Option<String>,

    #[arg(
        long,
        requires = "host_address",
        help = "Kafka bootstrap of an existing node, reachable from containers"
    )]
    bootstrap: Option<String>,

    #[arg(
        long,
        requires = "bootstrap",
        help = "Kafka address of the same node, reachable from this host"
    )]
    host_address: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    let cli = Cli::parse();
    let known = KnownFailures::load(cli.suite)?;

    let target = match (cli.target, cli.bootstrap) {
        (None, None) => Target::start(cli.suite).await?,
        (url, bootstrap) => Target::remote(url, bootstrap, cli.host_address),
    };

    let outcomes = cli.suite.run(&target).await;
    target.stop().await;

    let report = known.classify(outcomes?);
    print!("{report}");

    Ok(if report.passed() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}
