use std::time::{Duration, Instant};

use picomq_auth::Audience;
use picomq_metrics::{Method, Metrics};

const EVENTS: u32 = 10_000_000;
const CEILING: Duration = Duration::from_nanos(100);

#[test]
#[ignore = "release builds only, run with cargo test --release -- --ignored"]
fn one_request_costs_less_than_the_ceiling() {
    Metrics::install();

    let started = Instant::now();
    for event in 0..EVENTS {
        let elapsed = Duration::from_micros(u64::from(event % 20_000));
        picomq_metrics::http(Audience::Pico, Method::Post, 200, elapsed);
        picomq_metrics::written(1, 16);
    }
    let each = started.elapsed() / EVENTS;

    assert!(each < CEILING, "{each:?} per request");
}
