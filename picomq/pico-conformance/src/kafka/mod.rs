mod clients;
mod docker;
mod raw;
mod rebalance;
mod reference;
mod scenario;
mod versions;

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;

use crate::kafka::clients::Clients;
use crate::kafka::reference::Reference;
use crate::kafka::scenario::Scenario;
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
        let tooling = clients.take_tooling_failures();
        let mismatch = if tooling.is_empty() {
            scenario.compare().mismatch(&expected, &actual)
        } else {
            Some(format!("tooling failed:\n{}", tooling.join("\n")))
        };

        outcomes.push(Outcome {
            case: scenario.name().to_owned(),
            passed: mismatch.is_none(),
            detail: mismatch,
        });
    }

    Ok(outcomes)
}
