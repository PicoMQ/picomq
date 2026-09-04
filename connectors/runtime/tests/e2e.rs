use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use picomq_runtime::{KafkaConfig, MetaBackend, PicoServer, ServerConfig};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use regex::Regex;
use tempfile::TempDir;

const RUNTIME_BIN: &str = env!("CARGO_BIN_EXE_pico-connectors");
const STDOUT_SINK: (&str, &str) = (
    "picomq-connector-stdout-sink",
    "picomq_connector_stdout_sink",
);
const RANDOM_SOURCE: (&str, &str) = (
    "picomq-connector-random-source",
    "picomq_connector_random_source",
);
const WAIT_TIMEOUT: Duration = Duration::from_secs(60);

fn loopback() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn free_port() -> SocketAddr {
    let listener = TcpListener::bind(loopback()).unwrap();
    listener.local_addr().unwrap()
}

fn broker_config(dir: &Path, node_epoch: i64, listen: SocketAddr) -> ServerConfig {
    ServerConfig {
        node_epoch,
        addr: loopback(),
        admin_addr: None,
        kafka: Some(KafkaConfig {
            listen,
            advertise: None,
        }),
        meta_backend: MetaBackend::parse(&format!("sqlite:{}", dir.join("meta.db").display()))
            .unwrap(),
        storage_uri: format!("1@file://{}", dir.join("objects").display()),
        wal_uri: Some(format!(
            "2@file://{}?batchInterval=5",
            dir.join("wal").display()
        )),
        ..Default::default()
    }
}

async fn start_broker(dir: &Path, node_epoch: i64, listen: SocketAddr) -> (PicoServer, String) {
    let server = picomq_runtime::start(broker_config(dir, node_epoch, listen))
        .await
        .unwrap();
    let bootstrap = server.kafka_addr().unwrap().to_string();
    (server, bootstrap)
}

fn build_artifact(package: &str, file_name: &str) -> PathBuf {
    static BUILD_LOCK: Mutex<()> = Mutex::new(());
    let bin_dir = PathBuf::from(RUNTIME_BIN).parent().unwrap().to_path_buf();
    let target_dir = bin_dir.parent().unwrap().to_path_buf();
    let artifact = bin_dir.join(file_name);
    if !artifact.exists() {
        let _guard = BUILD_LOCK.lock().unwrap();
        if !artifact.exists() {
            let status = Command::new(env!("CARGO"))
                .args(["build", "-p", package, "--target-dir"])
                .arg(&target_dir)
                .status()
                .unwrap();
            assert!(status.success(), "failed to build {package}");
        }
    }
    assert!(artifact.exists(), "missing artifact {}", artifact.display());
    artifact
}

fn plugin_path(plugin: (&str, &str)) -> String {
    let (package, lib_name) = plugin;
    let extension = match std::env::consts::OS {
        "macos" => "dylib",
        "windows" => "dll",
        _ => "so",
    };
    let library = build_artifact(package, &format!("lib{lib_name}.{extension}"));
    library.with_extension("").display().to_string()
}

struct PicoProcess {
    child: Child,
}

impl PicoProcess {
    async fn spawn(dir: &Path, node_epoch: i64, kafka_listen: SocketAddr) -> Self {
        let binary = build_artifact(
            "picomq-cli",
            &format!("pico{}", std::env::consts::EXE_SUFFIX),
        );
        let child = Command::new(binary)
            .args([
                "serve",
                "--listen",
                "127.0.0.1:0",
                "--admin-listen",
                "127.0.0.1:0",
                "--kafka-listen",
                &kafka_listen.to_string(),
                "--node-epoch",
                &node_epoch.to_string(),
                "--meta-url",
                &format!("sqlite:{}", dir.join("meta.db").display()),
                "--storage",
                &format!("1@file://{}", dir.join("objects").display()),
                "--wal",
                &format!("2@file://{}?batchInterval=5", dir.join("wal").display()),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let bootstrap = kafka_listen.to_string();
        wait_until("pico kafka listener up", || async {
            tokio::task::spawn_blocking({
                let bootstrap = bootstrap.clone();
                move || {
                    reader(&bootstrap)
                        .fetch_metadata(None, Duration::from_secs(2))
                        .is_ok()
                }
            })
            .await
            .unwrap()
        })
        .await;
        Self { child }
    }

    fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for PicoProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Runtime {
    child: Child,
    api: String,
    logs: Arc<Mutex<String>>,
    _dir: Option<TempDir>,
}

impl Runtime {
    fn spawn(bootstrap: &str, connectors: &[(&str, String)]) -> Self {
        let dir = TempDir::new().unwrap();
        let mut runtime = Self::spawn_in(dir.path(), bootstrap, connectors, &[]);
        runtime._dir = Some(dir);
        runtime
    }

    fn spawn_in(
        dir: &Path,
        bootstrap: &str,
        connectors: &[(&str, String)],
        extra_env: &[(&str, &str)],
    ) -> Self {
        let connectors_dir = dir.join("connectors");
        std::fs::create_dir_all(&connectors_dir).unwrap();
        for (file, contents) in connectors {
            std::fs::write(connectors_dir.join(format!("{file}.toml")), contents).unwrap();
        }
        let http_addr = free_port();
        let config = format!(
            r#"
[http]
enabled = true
address = "{http_addr}"

[kafka]
bootstrap_servers = "{bootstrap}"
client_id = "picomq-connectors-test"

[state]
path = "{state}"
storage = "file"

[connectors]
config_type = "local"
config_dir = "{connectors}"

[logging]
format = "text"
"#,
            state = dir.join("state").display(),
            connectors = connectors_dir.display(),
        );
        let config_path = dir.join("config.toml");
        std::fs::write(&config_path, config).unwrap();

        let mut child = Command::new(RUNTIME_BIN)
            .env("PICOMQ_CONNECTORS_CONFIG_PATH", &config_path)
            .env("RUST_LOG", "info")
            .env("NO_COLOR", "1")
            .envs(extra_env.iter().copied())
            .current_dir(dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let logs = Arc::new(Mutex::new(String::new()));
        for reader in [
            Box::new(child.stdout.take().unwrap()) as Box<dyn std::io::Read + Send>,
            Box::new(child.stderr.take().unwrap()),
        ] {
            let logs = logs.clone();
            std::thread::spawn(move || {
                for line in BufReader::new(reader).lines().map_while(Result::ok) {
                    logs.lock().unwrap().push_str(&line);
                    logs.lock().unwrap().push('\n');
                }
            });
        }
        Self {
            child,
            api: format!("http://{http_addr}"),
            logs,
            _dir: None,
        }
    }

    fn kill(mut self) -> String {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.logs()
    }

    async fn wait_exit(&mut self) -> String {
        let start = Instant::now();
        loop {
            if self.child.try_wait().unwrap().is_some() {
                tokio::time::sleep(Duration::from_millis(200)).await;
                return self.logs();
            }
            assert!(
                start.elapsed() < WAIT_TIMEOUT,
                "timed out waiting for runtime exit"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn logs(&self) -> String {
        let ansi = Regex::new("\x1b\\[[0-9;]*m").unwrap();
        ansi.replace_all(&self.logs.lock().unwrap(), "")
            .into_owned()
    }

    async fn wait_healthy(&self) {
        let client = reqwest::Client::new();
        wait_until("runtime healthy", || async {
            client
                .get(format!("{}/health", self.api))
                .send()
                .await
                .map(|response| response.status().is_success())
                .unwrap_or(false)
        })
        .await;
    }

    async fn restart_source(&self, key: &str) {
        let response = reqwest::Client::new()
            .post(format!("{}/sources/{key}/restart", self.api))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success(), "{}", response.status());
    }

    async fn source_status(&self, key: &str) -> serde_json::Value {
        reqwest::Client::new()
            .get(format!("{}/sources/{key}", self.api))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    async fn sink_status(&self, key: &str) -> serde_json::Value {
        reqwest::Client::new()
            .get(format!("{}/sinks/{key}", self.api))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    fn sink_received_by_topic(&self) -> BTreeMap<String, usize> {
        sink_received_by_topic(&self.logs())
    }
}

fn sink_received_by_topic(logs: &str) -> BTreeMap<String, usize> {
    let pattern = Regex::new(
        r"Stdout sink with ID: \d+ received: (\d+) messages, schema: \w+, topic: ([^,]+),",
    )
    .unwrap();
    let mut totals = BTreeMap::new();
    for capture in pattern.captures_iter(logs) {
        let count: usize = capture[1].parse().unwrap();
        *totals.entry(capture[2].to_owned()).or_insert(0) += count;
    }
    totals
}

fn sink_message_offsets(logs: &str) -> BTreeSet<u64> {
    let pattern = Regex::new(r"Message offset: (\d+),").unwrap();
    pattern
        .captures_iter(logs)
        .map(|capture| capture[1].parse().unwrap())
        .collect()
}

fn restored_messages_produced(logs: &str) -> Option<usize> {
    Regex::new(
        r"Restored state for Random source connector with ID: \d+\. Messages produced: (\d+)",
    )
    .unwrap()
    .captures(logs)
    .map(|capture| capture[1].parse().unwrap())
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            eprintln!("--- pico-connectors logs ---\n{}", self.logs());
        }
    }
}

async fn wait_until<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while !condition().await {
        assert!(
            start.elapsed() < WAIT_TIMEOUT,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn reader(bootstrap: &str) -> StreamConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("e2e-reader-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .create()
        .unwrap()
}

fn list_topics(bootstrap: &str) -> BTreeSet<String> {
    let metadata = reader(bootstrap)
        .fetch_metadata(None, Duration::from_secs(10))
        .unwrap();
    metadata
        .topics()
        .iter()
        .map(|topic| topic.name().to_owned())
        .collect()
}

async fn read_topic(bootstrap: &str, topic: &str) -> Vec<serde_json::Value> {
    let consumer = reader(bootstrap);
    let Ok((_, high)) = consumer.fetch_watermarks(topic, 0, Duration::from_secs(5)) else {
        return Vec::new();
    };
    if high <= 0 {
        return Vec::new();
    }
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&assignment).unwrap();
    let mut records = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(10), consumer.recv()).await {
            Ok(Ok(message)) => {
                let payload = message.payload().unwrap_or_default();
                records.push(serde_json::from_slice(payload).unwrap());
                if message.offset() + 1 >= high {
                    break;
                }
            }
            Ok(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => break,
            Ok(Err(error)) => panic!("read error on {topic}: {error}"),
            Err(_) => break,
        }
    }
    records
}

async fn sequences(bootstrap: &str, topics: &[String]) -> BTreeSet<usize> {
    let mut all = BTreeSet::new();
    for topic in topics {
        for record in read_topic(bootstrap, topic).await {
            all.insert(record["sequence"].as_u64().unwrap() as usize);
        }
    }
    all
}

fn random_source(key: &str, topic_toml: &str, plugin_config: &str) -> String {
    format!(
        r#"
type = "source"
key = "{key}"
enabled = true
version = 0
name = "Random source"
path = "{path}"
verbose = true

[[topics]]
{topic_toml}
schema = "json"
batch_length = 100
linger_time = "5ms"
create_topics = true

[plugin_config]
{plugin_config}
"#,
        path = plugin_path(RANDOM_SOURCE),
    )
}

fn stdout_sink(key: &str, subscription_toml: &str) -> String {
    stdout_sink_with(key, subscription_toml, false)
}

fn stdout_sink_with(key: &str, subscription_toml: &str, print_payload: bool) -> String {
    format!(
        r#"
type = "sink"
key = "{key}"
enabled = true
version = 0
name = "Stdout sink"
path = "{path}"
verbose = true

[[topics]]
{subscription_toml}
schema = "json"
batch_length = 100
poll_interval = "20ms"
properties = {{ "auto.commit.interval.ms" = "500", "session.timeout.ms" = "6000" }}

[plugin_config]
print_payload = {print_payload}
"#,
        path = plugin_path(STDOUT_SINK),
    )
}

fn contiguous(sequences: &BTreeSet<usize>, expected: usize) -> bool {
    sequences.len() == expected && sequences.iter().copied().eq(0..expected)
}

#[tokio::test(flavor = "multi_thread")]
async fn given_static_route_when_source_produces_should_sink_consume_every_message() {
    let dir = TempDir::new().unwrap();
    let (server, bootstrap) = start_broker(dir.path(), 1, loopback()).await;
    let runtime = Runtime::spawn(
        &bootstrap,
        &[
            (
                "random",
                random_source(
                    "random",
                    r#"topic = "orders""#,
                    "interval = \"20ms\"\nmax_count = 50\nmessages_range = [5, 10]",
                ),
            ),
            ("stdout", stdout_sink("stdout", r#"topics = ["orders"]"#)),
        ],
    );
    runtime.wait_healthy().await;

    wait_until("50 messages in orders", || async {
        contiguous(&sequences(&bootstrap, &["orders".to_owned()]).await, 50)
    })
    .await;
    wait_until("sink consumed 50", || async {
        runtime.sink_received_by_topic().get("orders").copied() == Some(50)
    })
    .await;

    let status = runtime.source_status("random").await;
    assert_eq!(status["status"], "running", "{status}");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn given_field_route_when_source_produces_should_fan_out_per_user_and_pattern_sink_should_see_all()
 {
    let dir = TempDir::new().unwrap();
    let (server, bootstrap) = start_broker(dir.path(), 1, loopback()).await;
    let runtime = Runtime::spawn(
        &bootstrap,
        &[
            (
                "random",
                random_source(
                    "random",
                    r#"topic = { strategy = "field", path = "user_id", template = "users-{value}" }"#,
                    "interval = \"20ms\"\nmax_count = 30\nmessages_range = [5, 10]\nuser_pool = 3",
                ),
            ),
            ("stdout", stdout_sink("stdout", r#"pattern = "users-.*""#)),
        ],
    );
    runtime.wait_healthy().await;

    let expected: Vec<String> = (0..3).map(|user| format!("users-user-{user}")).collect();
    wait_until("3 user topics", || async {
        let topics = list_topics(&bootstrap);
        expected.iter().all(|topic| topics.contains(topic))
    })
    .await;
    wait_until("30 messages across user topics", || async {
        contiguous(&sequences(&bootstrap, &expected).await, 30)
    })
    .await;
    for topic in &expected {
        for record in read_topic(&bootstrap, topic).await {
            assert_eq!(
                format!("users-{}", record["user_id"].as_str().unwrap()),
                *topic
            );
        }
    }
    wait_until("pattern sink consumed 30", || async {
        let received = runtime.sink_received_by_topic();
        received.values().sum::<usize>() == 30 && received.len() == 3
    })
    .await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn given_hash_route_when_source_produces_should_land_in_at_most_buckets_topics() {
    let dir = TempDir::new().unwrap();
    let (server, bootstrap) = start_broker(dir.path(), 1, loopback()).await;
    let runtime = Runtime::spawn(
        &bootstrap,
        &[(
            "random",
            random_source(
                "random",
                r#"topic = { strategy = "hash", path = "user_id", buckets = 4, template = "shard-{value}" }"#,
                "interval = \"20ms\"\nmax_count = 40\nmessages_range = [5, 10]\nuser_pool = 16",
            ),
        )],
    );
    runtime.wait_healthy().await;

    wait_until("40 messages across shards", || async {
        let shards: Vec<String> = list_topics(&bootstrap)
            .into_iter()
            .filter(|topic| topic.starts_with("shard-"))
            .collect();
        !shards.is_empty() && contiguous(&sequences(&bootstrap, &shards).await, 40)
    })
    .await;
    let shards: BTreeSet<String> = list_topics(&bootstrap)
        .into_iter()
        .filter(|topic| topic.starts_with("shard-"))
        .collect();
    assert!(shards.len() > 1 && shards.len() <= 4, "{shards:?}");
    for shard in &shards {
        let bucket: u32 = shard.trim_start_matches("shard-").parse().unwrap();
        assert!(bucket < 4);
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn given_running_source_when_restarted_via_api_should_resume_from_checkpoint() {
    let dir = TempDir::new().unwrap();
    let (server, bootstrap) = start_broker(dir.path(), 1, loopback()).await;
    let runtime = Runtime::spawn(
        &bootstrap,
        &[(
            "random",
            random_source(
                "random",
                r#"topic = "restart""#,
                "interval = \"50ms\"\nmax_count = 200\nmessages_range = [5, 10]",
            ),
        )],
    );
    runtime.wait_healthy().await;

    let topics = vec!["restart".to_owned()];
    wait_until("at least 40 messages before restart", || async {
        sequences(&bootstrap, &topics).await.len() >= 40
    })
    .await;
    runtime.restart_source("random").await;

    wait_until("state restored on restart", || async {
        runtime
            .logs()
            .contains("Restored state for Random source connector")
    })
    .await;
    let restored = Regex::new(
        r"Restored state for Random source connector with ID: \d+\. Messages produced: (\d+)",
    )
    .unwrap()
    .captures(&runtime.logs())
    .map(|capture| capture[1].parse::<usize>().unwrap())
    .unwrap();
    assert!(restored >= 40, "restored {restored}");

    wait_until("200 contiguous sequences after restart", || async {
        contiguous(&sequences(&bootstrap, &topics).await, 200)
    })
    .await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn given_broker_outage_when_batch_fails_should_nack_and_replay_without_gaps() {
    let dir = TempDir::new().unwrap();
    let listen = free_port();
    let bootstrap = listen.to_string();
    let pico = PicoProcess::spawn(dir.path(), 1, listen).await;
    let runtime = Runtime::spawn(
        &bootstrap,
        &[(
            "random",
            random_source(
                "random",
                r#"topic = "outage"
properties = { "message.timeout.ms" = "2000" }"#,
                "interval = \"200ms\"\nmessages_range = [5, 10]",
            ),
        )],
    );
    runtime.wait_healthy().await;

    let topics = vec!["outage".to_owned()];
    wait_until("messages before outage", || async {
        sequences(&bootstrap, &topics).await.len() >= 20
    })
    .await;
    pico.kill();

    wait_until("source reports failed send", || async {
        runtime.logs().contains("Failed to send")
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let failed_batches = runtime.logs().matches("Failed to send").count();
    assert!(failed_batches >= 1);
    let status = runtime.source_status("random").await;
    assert_eq!(status["status"], "error", "{status}");

    let pico = PicoProcess::spawn(dir.path(), 2, listen).await;
    wait_until("source status recovers to running", || async {
        runtime.source_status("random").await["status"] == "running"
    })
    .await;
    let status = runtime.source_status("random").await;
    assert!(status["last_error"].is_null(), "{status}");
    let before_recovery = sequences(&bootstrap, &topics).await;
    let resume_target = before_recovery.len() + 50;
    wait_until("contiguous sequences after recovery", || async {
        let observed = sequences(&bootstrap, &topics).await;
        observed.len() >= resume_target && contiguous(&observed, observed.len())
    })
    .await;
    let observed = sequences(&bootstrap, &topics).await;
    assert!(
        contiguous(&observed, observed.len()),
        "gaps after replay: {observed:?}"
    );
    pico.kill();
}

#[tokio::test(flavor = "multi_thread")]
async fn given_runtime_killed_when_restarted_should_resume_source_from_file_checkpoint() {
    let broker_dir = TempDir::new().unwrap();
    let runtime_dir = TempDir::new().unwrap();
    let (server, bootstrap) = start_broker(broker_dir.path(), 1, loopback()).await;
    let connectors = [(
        "random",
        random_source(
            "random",
            r#"topic = "crash-source""#,
            "interval = \"50ms\"\nmax_count = 200\nmessages_range = [5, 10]",
        ),
    )];
    let topics = vec!["crash-source".to_owned()];

    let runtime = Runtime::spawn_in(runtime_dir.path(), &bootstrap, &connectors, &[]);
    runtime.wait_healthy().await;
    wait_until("at least 40 messages before kill", || async {
        sequences(&bootstrap, &topics).await.len() >= 40
    })
    .await;
    runtime.kill();
    let produced_before_kill = sequences(&bootstrap, &topics).await.len();
    assert!(produced_before_kill < 200);

    let runtime = Runtime::spawn_in(runtime_dir.path(), &bootstrap, &connectors, &[]);
    runtime.wait_healthy().await;
    wait_until("state restored after kill", || async {
        restored_messages_produced(&runtime.logs()).is_some()
    })
    .await;
    let restored = restored_messages_produced(&runtime.logs()).unwrap();
    assert!(
        restored > 0 && restored <= produced_before_kill,
        "restored {restored}"
    );

    wait_until("200 contiguous sequences after kill", || async {
        contiguous(&sequences(&bootstrap, &topics).await, 200)
    })
    .await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn given_runtime_killed_when_restarted_should_resume_sink_from_committed_offsets() {
    let broker_dir = TempDir::new().unwrap();
    let runtime_dir = TempDir::new().unwrap();
    let (server, bootstrap) = start_broker(broker_dir.path(), 1, loopback()).await;
    let connectors = [
        (
            "random",
            random_source(
                "random",
                r#"topic = "crash-sink""#,
                "interval = \"200ms\"\nmax_count = 120\nmessages_range = [5, 10]",
            ),
        ),
        (
            "stdout",
            stdout_sink_with("stdout", r#"topics = ["crash-sink"]"#, true),
        ),
    ];

    let runtime = Runtime::spawn_in(runtime_dir.path(), &bootstrap, &connectors, &[]);
    runtime.wait_healthy().await;
    wait_until("sink consumed at least 30 before kill", || async {
        sink_message_offsets(&runtime.logs()).len() >= 30
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let first_run = sink_message_offsets(&runtime.kill());
    let first_run_max = *first_run.iter().max().unwrap();
    assert!(first_run.contains(&0));

    let runtime = Runtime::spawn_in(runtime_dir.path(), &bootstrap, &connectors, &[]);
    runtime.wait_healthy().await;

    let (topic_high, _) = wait_for_topic_drained(&bootstrap, "crash-sink", &runtime).await;
    let second_run = sink_message_offsets(&runtime.logs());
    let second_run_min = *second_run.iter().min().unwrap();
    assert!(
        second_run_min > 0,
        "sink restarted from earliest instead of committed offset"
    );
    assert!(
        second_run_min <= first_run_max + 1,
        "sink skipped offsets: first run ended at {first_run_max}, second started at {second_run_min}"
    );
    let union: BTreeSet<u64> = first_run.union(&second_run).copied().collect();
    let expected: BTreeSet<u64> = (0..topic_high).collect();
    assert_eq!(union, expected, "offsets lost across the restart");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn given_sink_consume_fails_transiently_when_retried_should_deliver_every_offset() {
    let broker_dir = TempDir::new().unwrap();
    let runtime_dir = TempDir::new().unwrap();
    let (server, bootstrap) = start_broker(broker_dir.path(), 1, loopback()).await;
    let connectors = [
        (
            "random",
            random_source(
                "random",
                r#"topic = "flaky-sink""#,
                "interval = \"100ms\"\nmax_count = 120\nmessages_range = [5, 10]",
            ),
        ),
        (
            "stdout",
            stdout_sink_with("stdout", r#"topics = ["flaky-sink"]"#, true),
        ),
    ];

    let runtime = Runtime::spawn_in(
        runtime_dir.path(),
        &bootstrap,
        &connectors,
        &[("PICOMQ_CONNECTORS_FAULT_SINK_CONSUME_FAIL", "3:2")],
    );
    runtime.wait_healthy().await;
    let (high, _) = wait_for_topic_drained(&bootstrap, "flaky-sink", &runtime).await;
    let logs = runtime.kill();
    assert!(
        logs.contains("Fault injection: failing sink consume attempt 2 of 2"),
        "{logs}"
    );
    assert!(logs.contains("Retrying in"), "{logs}");
    let offsets = sink_message_offsets(&logs);
    let expected: BTreeSet<u64> = (0..high).collect();
    assert_eq!(
        offsets, expected,
        "sink lost or skipped offsets across retries"
    );
    let duplicates = Regex::new(r"Message offset: (\d+),")
        .unwrap()
        .captures_iter(&logs)
        .count();
    assert_eq!(duplicates, offsets.len(), "sink saw duplicate offsets");
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn given_sink_consume_fails_permanently_when_restarted_should_replay_from_committed_offset() {
    let broker_dir = TempDir::new().unwrap();
    let runtime_dir = TempDir::new().unwrap();
    let (server, bootstrap) = start_broker(broker_dir.path(), 1, loopback()).await;
    let connectors = [
        (
            "random",
            random_source(
                "random",
                r#"topic = "dead-sink""#,
                "interval = \"100ms\"\nmax_count = 120\nmessages_range = [5, 10]",
            ),
        ),
        (
            "stdout",
            stdout_sink_with("stdout", r#"topics = ["dead-sink"]"#, true),
        ),
    ];

    let runtime = Runtime::spawn_in(
        runtime_dir.path(),
        &bootstrap,
        &connectors,
        &[("PICOMQ_CONNECTORS_FAULT_SINK_CONSUME_FAIL", "3:1000")],
    );
    runtime.wait_healthy().await;
    wait_until("sink reports error", || async {
        runtime.sink_status("stdout").await["status"] == "error"
    })
    .await;
    let status = runtime.sink_status("stdout").await;
    assert!(
        status["last_error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("failed to consume batch"),
        "{status}"
    );
    let topics = vec!["dead-sink".to_owned()];
    wait_until("source finished producing", || async {
        sequences(&bootstrap, &topics).await.len() == 120
    })
    .await;
    let first_run = sink_message_offsets(&runtime.kill());
    let first_run_max = *first_run.iter().max().unwrap();
    assert!(first_run.contains(&0));
    assert!(
        first_run_max < 119,
        "sink kept consuming after permanent failure"
    );

    let runtime = Runtime::spawn_in(runtime_dir.path(), &bootstrap, &connectors, &[]);
    runtime.wait_healthy().await;
    let (high, _) = wait_for_topic_drained(&bootstrap, "dead-sink", &runtime).await;
    let second_run = sink_message_offsets(&runtime.logs());
    let second_run_min = *second_run.iter().min().unwrap();
    assert_eq!(
        second_run_min,
        first_run_max + 1,
        "sink restarted somewhere other than the first uncommitted batch"
    );
    let union: BTreeSet<u64> = first_run.union(&second_run).copied().collect();
    let expected: BTreeSet<u64> = (0..high).collect();
    assert_eq!(union, expected, "offsets lost across the failure");
    server.shutdown().await;
}

async fn wait_for_topic_drained(bootstrap: &str, topic: &str, runtime: &Runtime) -> (u64, usize) {
    let topics = vec![topic.to_owned()];
    wait_until("source finished producing", || async {
        sequences(bootstrap, &topics).await.len() == 120
    })
    .await;
    let high = u64::try_from(
        reader(bootstrap)
            .fetch_watermarks(topic, 0, Duration::from_secs(5))
            .unwrap()
            .1,
    )
    .unwrap();
    wait_until("sink drained topic", || async {
        sink_message_offsets(&runtime.logs()).contains(&(high - 1))
    })
    .await;
    (high, sink_message_offsets(&runtime.logs()).len())
}

#[tokio::test(flavor = "multi_thread")]
async fn given_crash_between_broker_ack_and_checkpoint_when_restarted_should_redeliver_batch() {
    let broker_dir = TempDir::new().unwrap();
    let runtime_dir = TempDir::new().unwrap();
    let (server, bootstrap) = start_broker(broker_dir.path(), 1, loopback()).await;
    let connectors = [(
        "random",
        random_source(
            "random",
            r#"topic = "crash-ack""#,
            "interval = \"50ms\"\nmax_count = 100\nmessages_range = [5, 10]",
        ),
    )];
    let topics = vec!["crash-ack".to_owned()];

    let mut runtime = Runtime::spawn_in(
        runtime_dir.path(),
        &bootstrap,
        &connectors,
        &[("PICOMQ_CONNECTORS_FAULT_CRASH_AFTER_SEND", "4")],
    );
    runtime.wait_healthy().await;
    let logs = runtime.wait_exit().await;
    assert!(logs.contains("Fault injection: aborting"), "{logs}");

    let records_at_crash = read_topic(&bootstrap, "crash-ack").await;
    let sequences_at_crash: BTreeSet<usize> = records_at_crash
        .iter()
        .map(|record| record["sequence"].as_u64().unwrap() as usize)
        .collect();
    assert_eq!(records_at_crash.len(), sequences_at_crash.len());

    let runtime = Runtime::spawn_in(runtime_dir.path(), &bootstrap, &connectors, &[]);
    runtime.wait_healthy().await;
    wait_until("state restored after ack-gap crash", || async {
        restored_messages_produced(&runtime.logs()).is_some()
    })
    .await;
    let restored = restored_messages_produced(&runtime.logs()).unwrap();
    assert!(
        restored < records_at_crash.len(),
        "checkpoint {restored} should lag the {} records already in the topic",
        records_at_crash.len()
    );

    wait_until("100 contiguous sequences after ack-gap crash", || async {
        contiguous(&sequences(&bootstrap, &topics).await, 100)
    })
    .await;
    let records = read_topic(&bootstrap, "crash-ack").await;
    assert!(
        records.len() > 100,
        "expected redelivered duplicates, topic has exactly {} records",
        records.len()
    );
    server.shutdown().await;
}
