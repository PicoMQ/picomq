mod clients;
mod docker;
mod raw;
mod rebalance;
mod reference;
pub mod sasl;
mod scenario;
mod versions;

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;

use crate::kafka::clients::Clients;
use crate::kafka::reference::Reference;
use crate::kafka::sasl::Listener;
use crate::kafka::scenario::{Compare, Scenario};
use crate::suite::Outcome;
use crate::target::Target;

pub struct Broker<'a> {
    pub bootstrap: &'a str,
    pub host_address: &'a str,
}

pub async fn run(target: &Target) -> anyhow::Result<Vec<Outcome>> {
    let picomq = Broker {
        bootstrap: target.bootstrap().context("target has no kafka listener")?,
        host_address: target
            .host_address()
            .context("target has no kafka address reachable from this host")?,
    };

    let reference = Reference::start().await?;
    let clients = Clients::start(reference.network()).await?;
    clients.wait_for(reference.bootstrap()).await?;
    reference.grant(&clients).await?;

    let kafka = Broker {
        bootstrap: reference.bootstrap(),
        host_address: reference.host_address(),
    };
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    let mut outcomes = Vec::new();
    for scenario in Scenario::ALL {
        let topic = format!("{}-{suffix}", scenario.name());

        let expected = scenario.run(&clients, &kafka, &topic).await;
        let actual = scenario.run(&clients, &picomq, &topic).await;

        outcomes.push(outcome(
            scenario.name(),
            scenario.compare(),
            &topic,
            expected,
            actual,
            clients.take_tooling_failures(),
        ));
    }

    let Some(secured) = target.secured() else {
        eprintln!("sasl scenarios need an in-process target and were not run");
        return Ok(outcomes);
    };
    let secured_kafka = Listener {
        bootstrap: reference.sasl_bootstrap(),
        host_address: reference.sasl_host_address(),
        root: reference.root(),
        reader: reference.reader(),
        writer: reference.writer(),
    };
    let secured_picomq = Listener {
        bootstrap: secured.bootstrap(),
        host_address: secured.host_address(),
        root: secured.root(),
        reader: secured.reader(),
        writer: secured.writer(),
    };
    for scenario in sasl::Scenario::ALL {
        let topic = format!("{}-{suffix}", scenario.name());

        let expected = scenario.run(&clients, &secured_kafka, &topic).await;
        let actual = scenario.run(&clients, &secured_picomq, &topic).await;

        outcomes.push(outcome(
            scenario.name(),
            Compare::Equal,
            &topic,
            expected,
            actual,
            clients.take_tooling_failures(),
        ));
    }

    Ok(outcomes)
}

fn outcome(
    name: &str,
    compare: Compare,
    topic: &str,
    expected: Vec<String>,
    actual: Vec<String>,
    tooling: Vec<String>,
) -> Outcome {
    let expected = normalize(expected, topic);
    let actual = normalize(actual, topic);

    let mismatch = if tooling.is_empty() {
        compare.mismatch(&expected, &actual)
    } else {
        Some(format!("tooling failed:\n{}", tooling.join("\n")))
    };

    Outcome {
        case: name.to_owned(),
        passed: mismatch.is_none(),
        detail: mismatch,
    }
}

fn normalize(lines: Vec<String>, topic: &str) -> Vec<String> {
    lines
        .into_iter()
        .map(|line| line.replace(topic, "{topic}").trim_end().to_owned())
        .collect()
}
