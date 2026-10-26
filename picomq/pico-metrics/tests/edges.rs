use std::time::Duration;

use picomq_auth::Audience;
use picomq_metrics::{Method, Metrics};

#[test]
fn inputs_outside_every_range_land_in_the_edge_slots() {
    let metrics = Metrics::install();

    picomq_metrics::http(Audience::Admin, Method::Other, 0, Duration::MAX);
    picomq_metrics::http(Audience::Admin, Method::Other, 999, Duration::ZERO);
    picomq_metrics::kafka(i16::MIN, false, Duration::MAX);
    picomq_metrics::kafka(i16::MAX, true, Duration::ZERO);
    picomq_metrics::name_kafka_operation(i16::MAX, "Unknown");

    let text = metrics.render(0);
    assert!(
        text.contains(r#"pico_requests_total{protocol="admin",operation="OTHER",status="1xx"} 1"#)
    );
    assert!(
        text.contains(r#"pico_requests_total{protocol="admin",operation="OTHER",status="5xx"} 1"#)
    );
    assert!(
        text.contains(r#"pico_requests_total{protocol="kafka",operation="Unknown",status="ok"} 1"#)
    );
    assert!(
        text.contains(
            r#"pico_requests_total{protocol="kafka",operation="Unknown",status="error"} 1"#
        )
    );
    assert!(text.contains(
        r#"pico_request_duration_seconds_bucket{protocol="admin",operation="OTHER",le="+Inf"} 2"#
    ));
    assert!(text.contains(
        r#"pico_request_duration_seconds_bucket{protocol="admin",operation="OTHER",le="10"} 1"#
    ));
}
