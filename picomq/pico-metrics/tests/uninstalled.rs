use picomq_metrics::Metrics;

#[test]
fn recording_before_install_is_dropped() {
    picomq_metrics::rebalance();
    let open = picomq_metrics::live_reader();

    let metrics = Metrics::install();
    drop(open);

    let text = metrics.render(0);
    assert!(text.contains("pico_group_rebalances_total 0"));
    assert!(text.contains("pico_live_readers 0"));
}
