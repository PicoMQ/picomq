use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::Duration;

use picomq_auth::Audience;
use picomq_metrics::{ListenerKind, Method, Metrics, Reason, TransferOutcome};

struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

#[allow(unsafe_code)]
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const EVENTS: u64 = 1_000_000;

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

#[test]
fn recording_never_allocates() {
    let metrics = Metrics::install();
    picomq_metrics::name_kafka_operation(0, "Produce");

    let before = allocations();
    for event in 0..EVENTS {
        let elapsed = Duration::from_micros(event % 20_000_000);
        picomq_metrics::http(Audience::Pico, Method::Post, 200, elapsed);
        picomq_metrics::kafka(0, event % 2 == 0, elapsed);
        picomq_metrics::written(1, 16);
        picomq_metrics::read(1, 16);
        picomq_metrics::rebalance();
        picomq_metrics::transfer(TransferOutcome::Completed);
        picomq_metrics::auth_failure(Audience::Kafka, Reason::Credential);
        drop(picomq_metrics::connection(ListenerKind::Stream));
        drop(picomq_metrics::live_reader());
        drop(picomq_metrics::coordinated_group());
    }
    let recorded = allocations() - before;

    assert_eq!(recorded, 0);
    let text = metrics.render(0);
    assert!(
        text.contains(
            r#"pico_requests_total{protocol="pico",operation="POST",status="2xx"} 1000000"#
        )
    );
    assert!(text.contains(
        r#"pico_requests_total{protocol="kafka",operation="Produce",status="ok"} 500000"#
    ));
    assert!(text.contains(
        r#"pico_request_duration_seconds_count{protocol="pico",operation="POST"} 1000000"#
    ));
    assert!(text.contains("pico_records_in_total 1000000"));
    assert!(text.contains(r#"pico_connections{listener="stream"} 0"#));
    assert!(text.contains("pico_groups_coordinated 0"));
}
