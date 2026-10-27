mod common;

use std::time::Duration;

use common::{Console, create, issue};
use picomq_auth::{Audience, Operation, ResourceSet, Scope};
use picomq_server::CreateCommand;
use serde_json::{Value, json};

fn names(items: &[Value]) -> Vec<&str> {
    items.iter().map(|item| item.as_str().unwrap()).collect()
}

#[tokio::test]
async fn topics_page_by_name_and_only_show_what_the_caller_may_list() {
    let console = Console::start().await;
    for n in 0..25 {
        create(&console.node, &format!("/team/t{n:02}"), 1).await;
    }
    for n in 0..60 {
        create(&console.node, &format!("/other/t{n:02}"), 1).await;
    }
    create(&console.node, "/team/zz", 1).await;
    let team = issue(
        &console.node,
        "team/reader",
        Scope {
            streams: ResourceSet::prefix("/team/"),
            ops: [Operation::List, Operation::Head].into(),
            audiences: [Audience::Admin].into(),
            ..Scope::default()
        },
    )
    .await;

    let (all, pages) = console
        .every("/admin/topics?limit=10", &console.root, "name")
        .await;
    assert_eq!(all.len(), 86);
    assert_eq!(pages, 9);

    let (prefixed, _) = console
        .every("/admin/topics?prefix=/team/&limit=7", &console.root, "name")
        .await;
    assert_eq!(prefixed.len(), 26);

    let (first_status, first) = console.get("/admin/topics?limit=2", &team).await;
    assert_eq!(first_status, 200);
    assert_eq!(first["items"], json!([]), "the budget ends before /team/");
    let (visible, _) = console.every("/admin/topics?limit=2", &team, "name").await;
    assert_eq!(visible.len(), 26);
    assert!(
        names(&visible)
            .iter()
            .all(|name| name.starts_with("/team/"))
    );

    console.server.shutdown().await;
}

#[tokio::test]
async fn partitions_page_by_index_and_filter_by_owner() {
    let console = Console::start().await;
    create(&console.node, "/wide", 25).await;
    console.append("/wide", 0, 3).await;

    let (indexes, pages) = console
        .every(
            "/admin/topics/%2Fwide/partitions?limit=10",
            &console.root,
            "index",
        )
        .await;
    assert_eq!(
        indexes
            .iter()
            .map(|index| index.as_u64().unwrap())
            .collect::<Vec<_>>(),
        (0..25).collect::<Vec<_>>()
    );
    assert_eq!(pages, 3);

    let (_, first) = console
        .get("/admin/topics/%2Fwide/partitions?limit=1", &console.root)
        .await;
    let partition = &first["items"][0];
    assert_eq!(partition["index"], 0);
    assert_eq!(partition["ownerNodeId"], 1);
    assert_eq!(partition["state"], "opened");
    assert_eq!(partition["startOffset"], 0);
    assert_eq!(partition["nextOffset"], 3);
    assert_eq!(partition["current"], true);

    let (elsewhere, _) = console
        .every(
            "/admin/topics/%2Fwide/partitions?node=99&limit=1",
            &console.root,
            "index",
        )
        .await;
    assert!(elsewhere.is_empty());

    console.server.shutdown().await;
}

#[tokio::test]
async fn a_topic_reports_its_owners_and_stored_bytes() {
    let console = Console::start().await;
    create(&console.node, "/sized", 3).await;
    console.append("/sized", 1, 5).await;

    let mut detail = Value::Null;
    for _ in 0..50 {
        detail = console.get("/admin/topics/sized", &console.root).await.1;
        if detail["storedBytes"].as_u64().unwrap_or(0) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert_eq!(detail["name"], "/sized");
    assert_eq!(detail["partitions"], 3);
    assert!(detail["storedBytes"].as_u64().unwrap() > 0, "{detail}");
    let owned: u64 = detail["owners"]
        .as_array()
        .unwrap()
        .iter()
        .map(|owner| owner["partitions"].as_u64().unwrap())
        .sum();
    assert_eq!(owned, 3);

    console.server.shutdown().await;
}

#[tokio::test]
async fn topics_are_created_changed_and_deleted() {
    let console = Console::start().await;
    let create = |body: Value| {
        console
            .http
            .post(console.admin("/admin/topics"))
            .bearer_auth(&console.root)
            .json(&body)
            .send()
    };
    let body = json!({
        "name": "/made",
        "partitions": 4,
        "contentType": "application/json",
        "retentionMs": 60_000,
    });

    let created = create(body.clone()).await.unwrap();
    assert_eq!(created.status(), 201);
    let again = create(body).await.unwrap();
    assert_eq!(again.status(), 200);
    let unnamed = create(json!({ "partitions": 2 })).await.unwrap();
    assert_eq!(unnamed.status(), 400);

    let changed = console
        .http
        .patch(console.admin("/admin/topics/made"))
        .bearer_auth(&console.root)
        .json(&json!({ "retentionBytes": 1_000_000, "partitions": 6 }))
        .send()
        .await
        .unwrap();
    assert_eq!(changed.status(), 200);
    let (_, detail) = console.get("/admin/topics/made", &console.root).await;
    assert_eq!(detail["partitions"], 6);
    assert_eq!(detail["kafkaTopic"], "made");
    assert_eq!(detail["contentType"], "application/json");
    assert_eq!(detail["retentionMs"], 60_000);
    assert_eq!(detail["retentionBytes"], 1_000_000);

    let deleted = console
        .http
        .delete(console.admin("/admin/topics/made"))
        .bearer_auth(&console.root)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    assert_eq!(
        console.get("/admin/topics/made", &console.root).await.0,
        404
    );

    console.server.shutdown().await;
}

#[tokio::test]
async fn a_trim_drops_records_below_the_offset() {
    let console = Console::start().await;
    create(&console.node, "/trimmed", 1).await;
    console.append("/trimmed", 0, 5).await;

    let trimmed = console
        .http
        .post(console.admin("/admin/topics/trimmed/partitions/0/trim"))
        .bearer_auth(&console.root)
        .json(&json!({ "before": 3 }))
        .send()
        .await
        .unwrap();
    assert_eq!(trimmed.status(), 200);
    let (_, page) = console
        .get("/admin/topics/trimmed/partitions", &console.root)
        .await;
    assert_eq!(page["items"][0]["startOffset"], 3);

    console.server.shutdown().await;
}

#[tokio::test]
async fn topic_routes_check_each_operation_and_refuse_auto_prefix_scopes() {
    let console = Console::start().await;
    create(&console.node, "/team/orders", 1).await;
    create(&console.node, "/other/orders", 1).await;
    let reader = issue(
        &console.node,
        "team/reader",
        Scope {
            streams: ResourceSet::prefix("/team/"),
            ops: [Operation::List, Operation::Head].into(),
            audiences: [Audience::Admin].into(),
            ..Scope::default()
        },
    )
    .await;
    let tenant = issue(
        &console.node,
        "tenant/a",
        Scope {
            auto_prefix: true,
            audiences: [Audience::Admin].into(),
            ..Scope::root()
        },
    )
    .await;

    assert_eq!(
        console.get("/admin/topics/team%2Forders", &reader).await.0,
        200
    );
    assert_eq!(
        console.get("/admin/topics/other%2Forders", &reader).await.0,
        403
    );
    let deleted = console
        .http
        .delete(console.admin("/admin/topics/team%2Forders"))
        .bearer_auth(&reader)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 403);
    assert_eq!(console.get("/admin/topics", &tenant).await.0, 403);

    console.server.shutdown().await;
}

#[tokio::test]
#[ignore]
async fn topic_and_partition_pages_stay_bounded_at_the_scale_target() {
    let console = Console::start().await;
    for n in 0..100_000 {
        let mut command = CreateCommand::new(format!("/scale/t{n:06}"), "text/plain");
        command.partitions = 1;
        console.node.topics().create(command).await.unwrap();
    }
    create(&console.node, "/scale/wide", 10_000).await;

    for (path, expected) in [
        ("/admin/topics?limit=1000", 100_001),
        (
            "/admin/topics/%2Fscale%2Fwide/partitions?limit=1000",
            10_000,
        ),
    ] {
        let mut cursor: Option<String> = None;
        let mut seen = 0;
        let mut slowest = Duration::ZERO;

        loop {
            let url = match &cursor {
                Some(cursor) => format!("{path}&cursor={cursor}"),
                None => path.to_owned(),
            };
            let started = std::time::Instant::now();
            let (status, page) = console.get(&url, &console.root).await;
            slowest = slowest.max(started.elapsed());
            assert_eq!(status, 200);

            seen += page["items"].as_array().unwrap().len();
            match page["next"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => break,
            }
        }

        println!("{path}: {seen} items, slowest page {slowest:?}");
        assert_eq!(seen, expected);
        assert!(slowest < Duration::from_millis(250));
    }

    let started = std::time::Instant::now();
    let (status, detail) = console
        .get("/admin/topics/%2Fscale%2Fwide", &console.root)
        .await;
    println!("detail of 10k partitions: {:?}", started.elapsed());
    assert_eq!(status, 200);
    assert_eq!(detail["partitions"], 10_000);
    assert!(started.elapsed() < Duration::from_millis(250));

    console.server.shutdown().await;
}
