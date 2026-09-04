use crate::context::RuntimeContext;
use crate::metrics::ConnectorType;
use picomq_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStats};
use picomq_connector_sdk::now_millis;
use semver::Version;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use sysinfo::{Pid, ProcessesToUpdate, System};

const VERSION: &str = env!("CARGO_PKG_VERSION");

static SYSINFO: OnceLock<Mutex<System>> = OnceLock::new();

pub async fn get_runtime_stats(context: &Arc<RuntimeContext>) -> ConnectorRuntimeStats {
    let system = probe_system();

    let sources = context.sources.get_all().await;
    let sinks = context.sinks.get_all().await;

    let sources_total = context.metrics.get_sources_total();
    let sinks_total = context.metrics.get_sinks_total();
    let sources_running = context.metrics.get_sources_running();
    let sinks_running = context.metrics.get_sinks_running();

    let mut connectors = Vec::with_capacity(sources.len() + sinks.len());
    for source in &sources {
        let version_semver = numeric_version(&source.version);
        connectors.push(ConnectorStats {
            key: source.key.clone(),
            name: source.name.clone(),
            connector_type: "source".to_owned(),
            version: source.version.clone(),
            version_semver,
            status: source.status,
            enabled: source.enabled,
            messages_produced: Some(context.metrics.get_messages_produced(&source.key)),
            messages_sent: Some(context.metrics.get_messages_sent(&source.key)),
            messages_consumed: None,
            messages_processed: None,
            messages_filtered: Some(
                context
                    .metrics
                    .get_messages_filtered(&source.key, ConnectorType::Source),
            ),
            errors: context
                .metrics
                .get_errors(&source.key, ConnectorType::Source),
        });
    }
    for sink in &sinks {
        let version_semver = numeric_version(&sink.version);
        connectors.push(ConnectorStats {
            key: sink.key.clone(),
            name: sink.name.clone(),
            connector_type: "sink".to_owned(),
            version: sink.version.clone(),
            version_semver,
            status: sink.status,
            enabled: sink.enabled,
            messages_produced: None,
            messages_sent: None,
            messages_consumed: Some(context.metrics.get_messages_consumed(&sink.key)),
            messages_processed: Some(context.metrics.get_messages_processed(&sink.key)),
            messages_filtered: Some(
                context
                    .metrics
                    .get_messages_filtered(&sink.key, ConnectorType::Sink),
            ),
            errors: context.metrics.get_errors(&sink.key, ConnectorType::Sink),
        });
    }

    let now = now_millis() * 1000;
    let start = context.start_time_millis * 1000;
    let run_time = now.saturating_sub(start);

    ConnectorRuntimeStats {
        connectors_runtime_version: VERSION.to_owned(),
        connectors_runtime_version_semver: numeric_version(VERSION),
        process_id: system.process_id,
        cpu_usage: system.cpu_usage,
        total_cpu_usage: system.total_cpu_usage,
        memory_usage: system.memory_usage,
        total_memory: system.total_memory,
        available_memory: system.available_memory,
        run_time,
        start_time: start,
        sources_total,
        sources_running,
        sinks_total,
        sinks_running,
        connectors,
    }
}

fn numeric_version(version: &str) -> Option<u32> {
    let parsed = Version::from_str(version).ok()?;
    let major = u32::try_from(parsed.major).ok()?;
    let minor = u32::try_from(parsed.minor).ok()?;
    let patch = u32::try_from(parsed.patch).ok()?;
    Some(major * 1_000_000 + minor * 1_000 + patch)
}

struct SystemProbe {
    process_id: u32,
    cpu_usage: f32,
    total_cpu_usage: f32,
    memory_usage: u64,
    total_memory: u64,
    available_memory: u64,
}

fn probe_system() -> SystemProbe {
    let mut system = SYSINFO
        .get_or_init(|| Mutex::new(System::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let process_id = std::process::id();
    let pid = Pid::from_u32(process_id);
    system.refresh_cpu_all();
    system.refresh_memory();
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    let (cpu_usage, memory_usage) = system
        .process(pid)
        .map(|process| (process.cpu_usage(), process.memory()))
        .unwrap_or((0.0, 0));
    SystemProbe {
        process_id,
        cpu_usage,
        total_cpu_usage: system.global_cpu_usage(),
        memory_usage,
        total_memory: system.total_memory(),
        available_memory: system.available_memory(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_semver_string_when_converted_should_pack_components() {
        assert_eq!(numeric_version("1.2.3"), Some(1_002_003));
        assert_eq!(numeric_version("0.5.0-edge.6"), Some(5_000));
        assert_eq!(numeric_version("unknown"), None);
    }
}
