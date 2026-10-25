mod clients;
mod docker;
mod raw;
mod rebalance;
pub mod reference;
pub mod sasl;
mod scenario;
mod versions;

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;

use crate::certificates::Certificates;
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

    let minted;
    let certificates = match target.certificates() {
        Some(certificates) => certificates,
        None => {
            minted = Certificates::mint()?;
            &minted
        }
    };
    let reference = Reference::start(certificates).await?;
    let clients = Clients::start(reference.network(), certificates.dir()).await?;
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

    let (Some(plaintext), Some(ssl)) = (target.sasl_plaintext(), target.sasl_ssl()) else {
        eprintln!("sasl scenarios need an in-process target and were not run");
        return Ok(outcomes);
    };
    let pairs = [
        (
            sasl::Scenario::ALL,
            Listener {
                bootstrap: reference.sasl_bootstrap(),
                host_address: reference.sasl_host_address(),
                tls: false,
                root: reference.root(),
                reader: reference.reader(),
                writer: reference.writer(),
            },
            Listener {
                bootstrap: plaintext.bootstrap(),
                host_address: plaintext.host_address(),
                tls: false,
                root: plaintext.root(),
                reader: plaintext.reader(),
                writer: plaintext.writer(),
            },
        ),
        (
            sasl::Scenario::SSL,
            Listener {
                bootstrap: reference.secure_bootstrap(),
                host_address: reference.secure_host_address(),
                tls: true,
                root: reference.root(),
                reader: reference.reader(),
                writer: reference.writer(),
            },
            Listener {
                bootstrap: ssl.bootstrap(),
                host_address: ssl.host_address(),
                tls: true,
                root: ssl.root(),
                reader: ssl.reader(),
                writer: ssl.writer(),
            },
        ),
    ];
    for (scenarios, kafka, picomq) in &pairs {
        for scenario in *scenarios {
            let topic = format!("{}-{suffix}", scenario.name());

            let expected = scenario.run(&clients, kafka, &topic).await;
            let actual = scenario.run(&clients, picomq, &topic).await;

            outcomes.push(outcome(
                scenario.name(),
                Compare::Equal,
                &topic,
                expected,
                actual,
                clients.take_tooling_failures(),
            ));
        }
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
