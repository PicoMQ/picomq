use std::fmt::Write;
use std::sync::atomic::Ordering;

use picomq_auth::json::audience_name;

use crate::histogram::Histogram;
use crate::instruments::{
    AUDIENCES, Instruments, ListenerKind, Method, OUTCOMES, Reason, STATUS_CLASSES,
    TransferOutcome, audience_slot,
};

pub(crate) fn render(instruments: &Instruments, logs_owned: u64) -> String {
    let mut out = String::new();

    family(&mut out, "pico_requests_total", "counter");
    for audience in AUDIENCES {
        for method in Method::ALL {
            for (class, label) in STATUS_CLASSES.iter().enumerate() {
                let value = instruments.http[audience_slot(audience)][method as usize][class]
                    .load(Ordering::Relaxed);
                if value > 0 {
                    let labels = format!(
                        r#"protocol="{}",operation="{}",status="{label}""#,
                        audience_name(audience),
                        method.label()
                    );
                    sample(&mut out, "pico_requests_total", &labels, value);
                }
            }
        }
    }
    for slot in 0..instruments.kafka.len() {
        for (outcome, label) in OUTCOMES.iter().enumerate() {
            let value = instruments.kafka[slot][outcome].load(Ordering::Relaxed);
            if value > 0 {
                let labels = format!(
                    r#"protocol="kafka",operation="{}",status="{label}""#,
                    kafka_name(instruments, slot)
                );
                sample(&mut out, "pico_requests_total", &labels, value);
            }
        }
    }

    family(&mut out, "pico_request_duration_seconds", "histogram");
    for audience in AUDIENCES {
        for method in Method::ALL {
            let labels = format!(
                r#"protocol="{}",operation="{}""#,
                audience_name(audience),
                method.label()
            );
            histogram(
                &mut out,
                &labels,
                &instruments.http_latency[audience_slot(audience)][method as usize],
            );
        }
    }
    for (slot, latency) in instruments.kafka_latency.iter().enumerate() {
        let labels = format!(
            r#"protocol="kafka",operation="{}""#,
            kafka_name(instruments, slot)
        );
        histogram(&mut out, &labels, latency);
    }

    for (name, counter) in [
        ("pico_records_in_total", &instruments.records_in),
        ("pico_bytes_in_total", &instruments.bytes_in),
        ("pico_records_out_total", &instruments.records_out),
        ("pico_bytes_out_total", &instruments.bytes_out),
        ("pico_group_rebalances_total", &instruments.rebalances),
    ] {
        family(&mut out, name, "counter");
        sample(&mut out, name, "", counter.load(Ordering::Relaxed));
    }

    family(&mut out, "pico_transfers_total", "counter");
    for outcome in TransferOutcome::ALL {
        let labels = format!(r#"status="{}""#, outcome.label());
        let value = instruments.transfers[outcome as usize].load(Ordering::Relaxed);
        sample(&mut out, "pico_transfers_total", &labels, value);
    }

    family(&mut out, "pico_auth_failures_total", "counter");
    for audience in AUDIENCES {
        for reason in Reason::ALL {
            let value = instruments.auth_failures[audience_slot(audience)][reason as usize]
                .load(Ordering::Relaxed);
            if value > 0 {
                let labels = format!(
                    r#"protocol="{}",reason="{}""#,
                    audience_name(audience),
                    reason.label()
                );
                sample(&mut out, "pico_auth_failures_total", &labels, value);
            }
        }
    }

    family(&mut out, "pico_connections", "gauge");
    for listener in ListenerKind::ALL {
        let labels = format!(r#"listener="{}""#, listener.label());
        let value = instruments.connections[listener as usize].load(Ordering::Relaxed);
        sample(&mut out, "pico_connections", &labels, value);
    }

    for (name, gauge) in [
        ("pico_live_readers", &instruments.live_readers),
        ("pico_groups_coordinated", &instruments.groups_coordinated),
    ] {
        family(&mut out, name, "gauge");
        sample(&mut out, name, "", gauge.load(Ordering::Relaxed));
    }
    family(&mut out, "pico_logs_owned", "gauge");
    sample(&mut out, "pico_logs_owned", "", logs_owned);

    out
}

fn kafka_name(instruments: &Instruments, slot: usize) -> String {
    instruments.kafka_names[slot]
        .get()
        .cloned()
        .unwrap_or_else(|| slot.to_string())
}

fn family(out: &mut String, name: &str, kind: &str) {
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn sample(out: &mut String, name: &str, labels: &str, value: impl std::fmt::Display) {
    if labels.is_empty() {
        let _ = writeln!(out, "{name} {value}");
    } else {
        let _ = writeln!(out, "{name}{{{labels}}} {value}");
    }
}

fn histogram(out: &mut String, labels: &str, histogram: &Histogram) {
    let count = histogram.count();
    if count == 0 {
        return;
    }

    for (bound, cumulative) in histogram.cumulative() {
        let le = match bound {
            Some(micros) => format!("{}", micros as f64 / 1_000_000.0),
            None => "+Inf".to_owned(),
        };
        let _ = writeln!(
            out,
            "pico_request_duration_seconds_bucket{{{labels},le=\"{le}\"}} {cumulative}"
        );
    }
    let _ = writeln!(
        out,
        "pico_request_duration_seconds_sum{{{labels}}} {}",
        histogram.sum_seconds()
    );
    let _ = writeln!(
        out,
        "pico_request_duration_seconds_count{{{labels}}} {count}"
    );
}
