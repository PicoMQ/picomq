use bytes::Bytes;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, CreateTopicsResponse,
    FetchRequest, FetchResponse, GroupId, LeaveGroupRequest, LeaveGroupResponse,
    ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, ProduceRequest, ProduceResponse,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
    TopicName,
};
use kafka_protocol::protocol::{Encodable, StrBytes};

use crate::kafka::clients::{CERTIFICATES, Clients, KCAT};
use crate::kafka::raw::{self, Connection, Reply, decode, describe, encode};
use crate::kafka::scenario::{delete_group_offsets, group_records};

pub const READER_GROUPS: &str = "readers";
const PLAIN: &str = "PLAIN";
const RECORDS: &str = "k1:v1\nk2:v2\nk3:v3\n";
const HANDSHAKE_VERSION: i16 = 1;
const AUTHENTICATE_VERSION: i16 = 2;
const API_VERSIONS_VERSION: i16 = 3;
const METADATA_VERSION: i16 = 12;
const PRODUCE_VERSION: i16 = 9;
const FETCH_VERSION: i16 = 12;
const LIST_OFFSETS_VERSION: i16 = 7;
const LEAVE_GROUP_VERSION: i16 = 4;
const CREATE_TOPICS_VERSION: i16 = 7;

pub struct Credentials {
    pub username: String,
    pub password: String,
}

pub struct Listener<'a> {
    pub bootstrap: &'a str,
    pub host_address: &'a str,
    pub tls: bool,
    pub root: &'a Credentials,
    pub reader: &'a Credentials,
    pub writer: &'a Credentials,
}

#[derive(Debug, Clone, Copy)]
pub enum Scenario {
    Handshake,
    Authenticate,
    LegacyHandshake,
    Required,
    ProduceConsume,
    Reader,
    Writer,
    Group,
    SslProduceConsume,
    SslRequired,
}

impl Scenario {
    pub const ALL: &[Scenario] = &[
        Scenario::Handshake,
        Scenario::Authenticate,
        Scenario::LegacyHandshake,
        Scenario::Required,
        Scenario::ProduceConsume,
        Scenario::Reader,
        Scenario::Writer,
        Scenario::Group,
    ];

    pub const SSL: &[Scenario] = &[Scenario::SslProduceConsume, Scenario::SslRequired];

    pub fn name(self) -> &'static str {
        match self {
            Scenario::Handshake => "sasl-handshake",
            Scenario::Authenticate => "sasl-authenticate",
            Scenario::LegacyHandshake => "sasl-legacy-handshake",
            Scenario::Required => "sasl-required",
            Scenario::ProduceConsume => "sasl-produce-consume",
            Scenario::Reader => "sasl-reader",
            Scenario::Writer => "sasl-writer",
            Scenario::Group => "sasl-group",
            Scenario::SslProduceConsume => "sasl-ssl-produce-consume",
            Scenario::SslRequired => "sasl-ssl-required",
        }
    }

    pub async fn run(self, clients: &Clients, listener: &Listener<'_>, topic: &str) -> Vec<String> {
        match self {
            Scenario::Handshake => handshake(listener.host_address).await,
            Scenario::Authenticate => authenticate(listener).await,
            Scenario::LegacyHandshake => legacy_handshake(listener).await,
            Scenario::Required => required(clients, listener).await,
            Scenario::ProduceConsume => produce_consume(clients, listener, topic).await,
            Scenario::Reader => reader(clients, listener, topic).await,
            Scenario::Writer => writer(clients, listener, topic).await,
            Scenario::Group => group(clients, listener, topic).await,
            Scenario::SslProduceConsume => produce_consume(clients, listener, topic).await,
            Scenario::SslRequired => ssl_required(clients, listener).await,
        }
    }
}

impl Credentials {
    fn token(&self) -> Bytes {
        Bytes::from(format!("\0{}\0{}", self.username, self.password))
    }
}

impl Listener<'_> {
    fn security(&self) -> &'static str {
        if self.tls {
            "SASL_SSL"
        } else {
            "SASL_PLAINTEXT"
        }
    }

    fn kcat(&self, security: &str, credentials: &Credentials) -> Vec<String> {
        let mut settings = vec![
            format!("security.protocol={security}"),
            "sasl.mechanisms=PLAIN".to_owned(),
            format!("sasl.username={}", credentials.username),
            format!("sasl.password={}", credentials.password),
        ];
        if self.tls {
            settings.push(format!("ssl.ca.location={CERTIFICATES}/ca.pem"));
        }

        let mut args = vec!["-b".to_owned(), self.bootstrap.to_owned()];
        for setting in settings {
            args.extend(["-X".to_owned(), setting]);
        }
        args
    }

    fn properties(&self, credentials: &Credentials) -> String {
        let mut properties = format!(
            "security.protocol={}\nsasl.mechanism=PLAIN\n\
             sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule \
             required username=\"{}\" password=\"{}\";\n",
            self.security(),
            credentials.username,
            credentials.password
        );
        if self.tls {
            properties.push_str(&format!(
                "ssl.truststore.type=PEM\nssl.truststore.location={CERTIFICATES}/ca.pem\n"
            ));
        }
        properties
    }
}

async fn handshake(address: &str) -> Vec<String> {
    let mut lines = Vec::new();

    if let Some(mut connection) = Connection::open(address).await {
        lines.push(format!(
            "handshake: {}",
            handshake_on(&mut connection, HANDSHAKE_VERSION, PLAIN).await
        ));
        lines.push(format!(
            "handshake again: {}",
            handshake_on(&mut connection, HANDSHAKE_VERSION, PLAIN).await
        ));
        lines.push(format!(
            "after handshake again: {}",
            metadata_on(&mut connection).await
        ));
    }

    if let Some(mut connection) = Connection::open(address).await {
        lines.push(format!(
            "unsupported mechanism: {}",
            handshake_on(&mut connection, HANDSHAKE_VERSION, "SCRAM-SHA-256").await
        ));
        lines.push(format!(
            "after unsupported: {}",
            metadata_on(&mut connection).await
        ));
    }

    if let Some(mut connection) = Connection::open(address).await {
        lines.push(format!(
            "api versions first: {}",
            api_versions_on(&mut connection).await
        ));
        lines.push(format!(
            "handshake after api versions: {}",
            handshake_on(&mut connection, HANDSHAKE_VERSION, PLAIN).await
        ));
    }

    lines
}

async fn authenticate(listener: &Listener<'_>) -> Vec<String> {
    let address = listener.host_address;
    let mut lines = Vec::new();

    if let Some(mut connection) = Connection::open(address).await {
        handshake_on(&mut connection, HANDSHAKE_VERSION, PLAIN).await;
        lines.push(format!(
            "authenticate: {}",
            authenticate_on(&mut connection, listener.root).await
        ));
        lines.push(format!("metadata: {}", metadata_on(&mut connection).await));
        lines.push(format!(
            "handshake after authentication: {}",
            handshake_on(&mut connection, HANDSHAKE_VERSION, PLAIN).await
        ));
        lines.push(format!(
            "authenticate again: {}",
            authenticate_on(&mut connection, listener.root).await
        ));
        lines.push(format!(
            "metadata after: {}",
            metadata_on(&mut connection).await
        ));
    }

    let wrong = Credentials {
        username: listener.root.username.clone(),
        password: "not-the-password".to_owned(),
    };
    if let Some(mut connection) = Connection::open(address).await {
        handshake_on(&mut connection, HANDSHAKE_VERSION, PLAIN).await;
        lines.push(format!(
            "wrong password: {}",
            authenticate_on(&mut connection, &wrong).await
        ));
        lines.push(format!(
            "after failure: {}",
            metadata_on(&mut connection).await
        ));
    }

    let unknown = Credentials {
        username: "nobody".to_owned(),
        password: listener.root.password.clone(),
    };
    if let Some(mut connection) = Connection::open(address).await {
        handshake_on(&mut connection, HANDSHAKE_VERSION, PLAIN).await;
        lines.push(format!(
            "unknown user: {}",
            authenticate_on(&mut connection, &unknown).await
        ));
    }

    if let Some(mut connection) = Connection::open(address).await {
        lines.push(format!(
            "without handshake: {}",
            authenticate_on(&mut connection, listener.root).await
        ));
    }

    lines
}

async fn legacy_handshake(listener: &Listener<'_>) -> Vec<String> {
    let Some(mut connection) = Connection::open(listener.host_address).await else {
        return vec!["connect: closed".to_owned()];
    };

    vec![
        format!(
            "handshake v0: {}",
            handshake_on(&mut connection, 0, PLAIN).await
        ),
        format!(
            "token: {}",
            describe(connection.send_bare(listener.root.token()).await)
        ),
        format!("metadata: {}", metadata_on(&mut connection).await),
    ]
}

async fn required(clients: &Clients, listener: &Listener<'_>) -> Vec<String> {
    let body = encode(&MetadataRequest::default(), METADATA_VERSION);
    let header = ApiKey::Metadata.request_header_version(METADATA_VERSION);
    let raw = raw::exchange(
        listener.host_address,
        ApiKey::Metadata,
        METADATA_VERSION,
        header,
        body,
    )
    .await;

    let kcat = clients
        .kcat(KCAT, &["-b", listener.bootstrap, "-L"], None)
        .await;

    vec![
        format!("metadata without sasl: {}", describe(raw)),
        format!("kcat without sasl: {}", status("metadata", kcat.ok)),
    ]
}

async fn produce_consume(clients: &Clients, listener: &Listener<'_>, topic: &str) -> Vec<String> {
    let mut lines = vec![
        create(clients, listener, listener.root, topic).await,
        produce(clients, listener, listener.root, topic).await,
    ];

    lines.extend(consume(clients, listener, listener.root, topic).await);

    lines
}

async fn reader(clients: &Clients, listener: &Listener<'_>, topic: &str) -> Vec<String> {
    let mut lines = vec![
        create(clients, listener, listener.root, topic).await,
        produce(clients, listener, listener.root, topic).await,
    ];

    lines.extend(consume(clients, listener, listener.reader, topic).await);
    lines.push(format!(
        "as reader {}",
        produce(clients, listener, listener.reader, topic).await
    ));

    if let Some(mut connection) = Connection::open(listener.host_address).await {
        handshake_on(&mut connection, HANDSHAKE_VERSION, PLAIN).await;
        lines.push(format!(
            "authenticate reader: {}",
            authenticate_on(&mut connection, listener.reader).await
        ));
        lines.push(format!(
            "raw produce as reader: {}",
            produce_on(&mut connection, topic).await
        ));
        lines.push(format!(
            "raw list offsets as reader: {}",
            list_offsets_on(&mut connection, topic).await
        ));
        lines.push(format!(
            "raw create topic as reader: {}",
            create_topic_on(&mut connection, &format!("{topic}-more")).await
        ));
    }

    lines
}

async fn writer(clients: &Clients, listener: &Listener<'_>, topic: &str) -> Vec<String> {
    let mut lines = vec![
        create(clients, listener, listener.root, topic).await,
        format!(
            "as writer {}",
            produce(clients, listener, listener.writer, topic).await
        ),
    ];

    lines.extend(consume(clients, listener, listener.root, topic).await);

    if let Some(mut connection) = Connection::open(listener.host_address).await {
        handshake_on(&mut connection, HANDSHAKE_VERSION, PLAIN).await;
        lines.push(format!(
            "authenticate writer: {}",
            authenticate_on(&mut connection, listener.writer).await
        ));
        lines.push(format!(
            "raw list offsets as writer: {}",
            list_offsets_on(&mut connection, topic).await
        ));
        lines.push(format!(
            "raw fetch as writer: {}",
            fetch_on(&mut connection, topic).await
        ));
    }

    lines
}

async fn group(clients: &Clients, listener: &Listener<'_>, topic: &str) -> Vec<String> {
    let own = format!("{READER_GROUPS}-{topic}");
    let other = format!("others-{topic}");
    let mut lines = vec![
        create(clients, listener, listener.root, topic).await,
        produce(clients, listener, listener.root, topic).await,
    ];

    let reader = listener.properties(listener.reader);
    for (name, group) in [("own group", &own), ("other group", &other)] {
        let records = group_records(clients, listener.bootstrap, topic, group, Some(&reader)).await;
        lines.push(format!("{name}: {} records", records.len()));
        lines.extend(records);
    }

    if let Some(mut connection) = Connection::open(listener.host_address).await {
        handshake_on(&mut connection, HANDSHAKE_VERSION, PLAIN).await;
        authenticate_on(&mut connection, listener.reader).await;
        lines.push(format!(
            "raw leave other group: {}",
            leave_group_on(&mut connection, &other).await
        ));
    }

    for (name, credentials) in [("reader", listener.reader), ("root", listener.root)] {
        let config = listener.properties(credentials);
        let (_, rows) =
            delete_group_offsets(clients, listener.bootstrap, topic, &own, Some(&config)).await;
        lines.extend(rows.into_iter().map(|row| format!("{name}: {row}")));
    }

    lines
}

async fn create(
    clients: &Clients,
    listener: &Listener<'_>,
    credentials: &Credentials,
    topic: &str,
) -> String {
    let output = clients
        .java(
            "kafka-topics",
            &[
                "--bootstrap-server",
                listener.bootstrap,
                "--command-config",
                "/dev/stdin",
                "--create",
                "--topic",
                topic,
                "--partitions",
                "1",
                "--replication-factor",
                "1",
            ],
            Some(&listener.properties(credentials)),
        )
        .await;

    status("create", output.ok)
}

async fn produce(
    clients: &Clients,
    listener: &Listener<'_>,
    credentials: &Credentials,
    topic: &str,
) -> String {
    let args = kcat_args(listener, credentials, &["-P", "-t", topic, "-K:"]);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();

    let output = clients.kcat(KCAT, &args, Some(RECORDS)).await;

    status("produce", output.ok)
}

async fn consume(
    clients: &Clients,
    listener: &Listener<'_>,
    credentials: &Credentials,
    topic: &str,
) -> Vec<String> {
    let args = kcat_args(
        listener,
        credentials,
        &[
            "-C",
            "-t",
            topic,
            "-o",
            "beginning",
            "-e",
            "-f",
            "%p %o %k %s\\n",
        ],
    );
    let args: Vec<&str> = args.iter().map(String::as_str).collect();

    let output = clients.kcat(KCAT, &args, None).await;

    output
        .stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect()
}

fn kcat_args(listener: &Listener<'_>, credentials: &Credentials, tail: &[&str]) -> Vec<String> {
    let mut args = listener.kcat(listener.security(), credentials);
    args.extend(tail.iter().map(|arg| (*arg).to_owned()));
    args
}

async fn ssl_required(clients: &Clients, listener: &Listener<'_>) -> Vec<String> {
    let wrong = Credentials {
        username: listener.root.username.clone(),
        password: "not-the-password".to_owned(),
    };
    let attempts = [
        (
            "without tls",
            listener.kcat("SASL_PLAINTEXT", listener.root),
        ),
        ("wrong password", listener.kcat("SASL_SSL", &wrong)),
        ("root", listener.kcat("SASL_SSL", listener.root)),
    ];

    let mut lines = Vec::new();
    for (name, mut args) in attempts {
        args.extend(["-L".to_owned(), "-m".to_owned(), "10".to_owned()]);
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let output = clients.kcat(KCAT, &args, None).await;
        lines.push(format!("{name}: {}", status("metadata", output.ok)));
    }
    lines
}

async fn handshake_on(connection: &mut Connection, version: i16, mechanism: &str) -> String {
    let request =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from(mechanism.to_owned()));

    match send(connection, ApiKey::SaslHandshake, version, &request).await {
        Reply::Frame(mut frame) => {
            match decode::<SaslHandshakeResponse>(&mut frame, ApiKey::SaslHandshake, version) {
                Ok(response) => format!(
                    "error {}, mechanisms [{}]",
                    response.error_code,
                    response
                        .mechanisms
                        .iter()
                        .map(|mechanism| mechanism.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                Err(_) => "unreadable".to_owned(),
            }
        }
        reply => describe(reply),
    }
}

async fn authenticate_on(connection: &mut Connection, credentials: &Credentials) -> String {
    let request = SaslAuthenticateRequest::default().with_auth_bytes(credentials.token());

    match send(
        connection,
        ApiKey::SaslAuthenticate,
        AUTHENTICATE_VERSION,
        &request,
    )
    .await
    {
        Reply::Frame(mut frame) => match decode::<SaslAuthenticateResponse>(
            &mut frame,
            ApiKey::SaslAuthenticate,
            AUTHENTICATE_VERSION,
        ) {
            Ok(response) => format!(
                "error {}, lifetime {}",
                response.error_code, response.session_lifetime_ms
            ),
            Err(_) => "unreadable".to_owned(),
        },
        reply => describe(reply),
    }
}

async fn api_versions_on(connection: &mut Connection) -> String {
    let request = ApiVersionsRequest::default()
        .with_client_software_name(StrBytes::from_static_str("pico-conformance"))
        .with_client_software_version(StrBytes::from_static_str("1"));

    match send(
        connection,
        ApiKey::ApiVersions,
        API_VERSIONS_VERSION,
        &request,
    )
    .await
    {
        Reply::Frame(mut frame) => match decode::<ApiVersionsResponse>(
            &mut frame,
            ApiKey::ApiVersions,
            API_VERSIONS_VERSION,
        ) {
            Ok(response) => format!("error {}", response.error_code),
            Err(_) => "unreadable".to_owned(),
        },
        reply => describe(reply),
    }
}

async fn metadata_on(connection: &mut Connection) -> String {
    let request = MetadataRequest::default();

    describe(send(connection, ApiKey::Metadata, METADATA_VERSION, &request).await)
}

async fn produce_on(connection: &mut Connection, topic: &str) -> String {
    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(5000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name(topic))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(raw::record_batch())),
                ]),
        ]);

    match send(connection, ApiKey::Produce, PRODUCE_VERSION, &request).await {
        Reply::Frame(mut frame) => {
            match decode::<ProduceResponse>(&mut frame, ApiKey::Produce, PRODUCE_VERSION) {
                Ok(response) => response
                    .responses
                    .first()
                    .and_then(|topic| topic.partition_responses.first())
                    .map(|partition| format!("error {}", partition.error_code))
                    .unwrap_or_else(|| "no partitions".to_owned()),
                Err(_) => "unreadable".to_owned(),
            }
        }
        reply => describe(reply),
    }
}

async fn list_offsets_on(connection: &mut Connection, topic: &str) -> String {
    let request = ListOffsetsRequest::default()
        .with_replica_id((-1).into())
        .with_topics(vec![
            ListOffsetsTopic::default()
                .with_name(topic_name(topic))
                .with_partitions(vec![
                    ListOffsetsPartition::default()
                        .with_partition_index(0)
                        .with_timestamp(-1),
                ]),
        ]);

    match send(
        connection,
        ApiKey::ListOffsets,
        LIST_OFFSETS_VERSION,
        &request,
    )
    .await
    {
        Reply::Frame(mut frame) => {
            match decode::<ListOffsetsResponse>(
                &mut frame,
                ApiKey::ListOffsets,
                LIST_OFFSETS_VERSION,
            ) {
                Ok(response) => response
                    .topics
                    .first()
                    .and_then(|topic| topic.partitions.first())
                    .map(|partition| format!("error {}", partition.error_code))
                    .unwrap_or_else(|| "no partitions".to_owned()),
                Err(_) => "unreadable".to_owned(),
            }
        }
        reply => describe(reply),
    }
}

async fn leave_group_on(connection: &mut Connection, group: &str) -> String {
    let request = LeaveGroupRequest::default()
        .with_group_id(GroupId(StrBytes::from(group.to_owned())))
        .with_members(vec![
            MemberIdentity::default().with_member_id(StrBytes::from_static_str("ghost")),
        ]);

    match send(
        connection,
        ApiKey::LeaveGroup,
        LEAVE_GROUP_VERSION,
        &request,
    )
    .await
    {
        Reply::Frame(mut frame) => {
            match decode::<LeaveGroupResponse>(&mut frame, ApiKey::LeaveGroup, LEAVE_GROUP_VERSION)
            {
                Ok(response) => format!(
                    "error {} members {:?}",
                    response.error_code,
                    response
                        .members
                        .iter()
                        .map(|member| member.error_code)
                        .collect::<Vec<_>>()
                ),
                Err(_) => "unreadable".to_owned(),
            }
        }
        reply => describe(reply),
    }
}

async fn fetch_on(connection: &mut Connection, topic: &str) -> String {
    let request = FetchRequest::default()
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic_name(topic))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1 << 20),
                ]),
        ]);

    match send(connection, ApiKey::Fetch, FETCH_VERSION, &request).await {
        Reply::Frame(mut frame) => {
            match decode::<FetchResponse>(&mut frame, ApiKey::Fetch, FETCH_VERSION) {
                Ok(response) => response
                    .responses
                    .first()
                    .and_then(|topic| topic.partitions.first())
                    .map(|partition| format!("error {}", partition.error_code))
                    .unwrap_or_else(|| "no partitions".to_owned()),
                Err(_) => "unreadable".to_owned(),
            }
        }
        reply => describe(reply),
    }
}

async fn create_topic_on(connection: &mut Connection, topic: &str) -> String {
    let request = CreateTopicsRequest::default()
        .with_timeout_ms(5000)
        .with_topics(vec![
            CreatableTopic::default()
                .with_name(topic_name(topic))
                .with_num_partitions(1)
                .with_replication_factor(1),
        ]);

    match send(
        connection,
        ApiKey::CreateTopics,
        CREATE_TOPICS_VERSION,
        &request,
    )
    .await
    {
        Reply::Frame(mut frame) => {
            match decode::<CreateTopicsResponse>(
                &mut frame,
                ApiKey::CreateTopics,
                CREATE_TOPICS_VERSION,
            ) {
                Ok(response) => response
                    .topics
                    .first()
                    .map(|topic| format!("error {}", topic.error_code))
                    .unwrap_or_else(|| "no topics".to_owned()),
                Err(_) => "unreadable".to_owned(),
            }
        }
        reply => describe(reply),
    }
}

async fn send(
    connection: &mut Connection,
    api: ApiKey,
    version: i16,
    request: &impl Encodable,
) -> Reply {
    let header_version = api.request_header_version(version);

    connection
        .send(api, version, header_version, encode(request, version))
        .await
}

fn topic_name(topic: &str) -> TopicName {
    TopicName(StrBytes::from(topic.to_owned()))
}

fn status(step: &str, ok: bool) -> String {
    format!("{step} {}", if ok { "ok" } else { "failed" })
}
