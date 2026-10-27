mod common;

use std::time::{Duration, Instant};

use common::{Console, create, issue};
use picomq_auth::{Audience, Operation, ResourceSet, Scope};
use picomq_server::{Log, Partition, PicoNode};
use serde_json::Value;

async fn committed(node: &PicoNode, name: &str, records: u64) {
    let topic = node.topics().get(name).await.unwrap().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let view = node.views().load();
        let end: u64 = (0..topic.partitions)
            .filter_map(|index| {
                let log = Log::Partition(Partition::new(topic.id, index));
                log.stream_id(&view.state).unwrap()
            })
            .filter_map(|stream_id| view.state.streams().get(&stream_id))
            .map(|row| row.end_offset)
            .sum();
        if end >= records {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{name} never committed {records}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn names(ranked: &[(picomq_server::Topic, impl Copy)]) -> Vec<&str> {
    ranked
        .iter()
        .map(|(topic, _)| topic.name.spelling())
        .collect()
}

#[tokio::test]
async fn topics_rank_by_stored_bytes_and_by_write_rate() {
    let console = Console::start().await;
    create(&console.node, "/big", 2).await;
    create(&console.node, "/small", 1).await;
    create(&console.node, "/idle", 1).await;
    console.append("/big", 0, 20).await;
    console.append("/big", 1, 20).await;
    console.append("/small", 0, 5).await;
    committed(&console.node, "/big", 40).await;
    committed(&console.node, "/small", 5).await;
    let overview = console.node.overview();

    let first = overview.rankings_at(1_000_000).await.unwrap();
    assert_eq!(names(&first.by_size), ["/big", "/small", "/idle"]);
    assert!(first.by_size[0].1 > first.by_size[1].1);
    assert_eq!(first.by_size[2].1, 0);
    assert!(first.by_write_rate.is_empty(), "one pass has no rate");
    assert_eq!(first.write_rate_since_ms, None);

    console.append("/small", 0, 60).await;
    committed(&console.node, "/small", 65).await;
    let cached = overview.rankings_at(1_059_000).await.unwrap();
    assert_eq!(
        cached.computed_at_ms, 1_000_000,
        "within 60s the pass is reused"
    );

    let second = overview.rankings_at(1_060_000).await.unwrap();
    assert_eq!(second.computed_at_ms, 1_060_000);
    assert_eq!(second.write_rate_since_ms, Some(1_000_000));
    assert_eq!(names(&second.by_write_rate), ["/small"]);
    assert_eq!(second.by_write_rate[0].1, 1.0);

    console.server.shutdown().await;
}

#[tokio::test]
async fn the_overview_needs_cluster_read_and_reports_its_age() {
    let console = Console::start().await;
    create(&console.node, "/seen", 1).await;
    let topic_reader = issue(
        &console.node,
        "team/reader",
        Scope {
            streams: ResourceSet::prefix("/"),
            ops: [Operation::Read, Operation::Head, Operation::List].into(),
            audiences: [Audience::Admin].into(),
            ..Scope::default()
        },
    )
    .await;

    let (status, _) = console.get("/admin/overview", &topic_reader).await;
    assert_eq!(status, 403);

    let (status, overview) = console.get("/admin/overview", &console.root).await;
    assert_eq!(status, 200, "{overview}");
    assert!(overview["computedAtMs"].as_i64().unwrap() > 0);
    assert_eq!(overview["topicsBySize"][0]["topic"], "/seen");
    assert_eq!(overview["topicsBySize"][0]["storedBytes"], 0);
    assert_eq!(overview["topicsByWriteRate"], Value::Array(Vec::new()));
    assert_eq!(overview["writeRateSinceMs"], Value::Null);

    console.server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn the_overview_pass_stays_bounded_at_the_scale_target() {
    let console = Console::start().await;
    let topics = console.node.topics();
    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..100_000 {
        while tasks.len() >= 256 {
            tasks.join_next().await.unwrap().unwrap();
        }
        let topics = topics.clone();
        tasks.spawn(async move {
            let mut command =
                picomq_server::CreateCommand::new(format!("/scale/t{n:06}"), "text/plain");
            command.partitions = 10;
            topics.create(command).await.unwrap();
        });
    }
    while let Some(task) = tasks.join_next().await {
        task.unwrap();
    }

    let started = Instant::now();
    let rankings = console
        .node
        .overview()
        .rankings_at(1_000_000)
        .await
        .unwrap();
    let elapsed = started.elapsed();

    println!("overview pass over 100k topics and 1M partitions: {elapsed:?}");
    assert_eq!(rankings.by_size.len(), 10);
    assert!(elapsed < Duration::from_secs(5));
}
