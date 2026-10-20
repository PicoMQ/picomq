use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::BrokerId;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, FetchRequest, MetadataRequest, ProduceRequest,
    RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{
    Decodable, Encodable, HeaderVersion, StrBytes, encode_request_header_into_buffer,
};
use kafka_protocol::records::{
    Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, Record,
    RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use picomq_kafka::{BrokerContext, HandlerOutcome, KafkaListener, ListenerConfig, dispatch};
use picomq_metadata::{CommandSink, LocalSink};
use picomq_server::{NodeConfig, PicoNode};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait};
use tokio::net::TcpListener;

fn tn(name: &str) -> TopicName {
    TopicName(StrBytes::from(name.to_owned()))
}

async fn test_broker() -> BrokerContext {
    broker_with(None).await
}

async fn broker_with(registry: Option<Arc<dyn picomq_schema::SchemaStore>>) -> BrokerContext {
    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(2));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(3));
    let node = PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 1,
            http_address: "http://127.0.0.1:4001".into(),
            protocol_addresses: std::collections::BTreeMap::from([(
                picomq_kafka::PROTOCOL_NAME.to_owned(),
                "127.0.0.1:19092".to_owned(),
            )]),
            ..Default::default()
        },
        sink,
        views,
        object_storage,
        wal_storage,
        registry,
    )
    .await
    .unwrap();
    BrokerContext::new(&node)
}

fn encode_request<T: Encodable>(
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    body: &T,
) -> Vec<u8> {
    let mut frame = BytesMut::new();
    let header = RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str("test")));
    encode_request_header_into_buffer(&mut frame, &header).unwrap();
    body.encode(&mut frame, version).unwrap();
    frame.to_vec()
}

fn kafka_batch(payload: &[u8]) -> Bytes {
    timed_batch(payload, 1)
}

fn timed_batch(payload: &[u8], timestamp: i64) -> Bytes {
    let records = vec![Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: NO_SEQUENCE,
        timestamp,
        key: None,
        value: Some(Bytes::copy_from_slice(payload)),
        headers: Default::default(),
    }];
    let mut out = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut out,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    out.freeze()
}

#[tokio::test]
async fn api_versions_handler_lists_supported_apis() {
    let broker = test_broker().await;
    let req = encode_request(
        ApiKey::ApiVersions,
        3,
        1,
        &ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str("picomq"))
            .with_client_software_version(StrBytes::from_static_str("0.1")),
    );
    let outcome = dispatch(&broker, &req).await.unwrap();
    let mut buf = match resolve(outcome).await {
        HandlerOutcome::Response(frame) => frame.0,
        _ => panic!("expected response"),
    };
    ResponseHeader::decode(&mut buf, 0).unwrap();
    let response = ApiVersionsResponse::decode(&mut buf, 3).unwrap();
    assert_eq!(response.error_code, 0);
    assert!(
        response
            .api_keys
            .iter()
            .any(|api| api.api_key == ApiKey::Metadata as i16)
    );

    let produce = response
        .api_keys
        .iter()
        .find(|api| api.api_key == ApiKey::Produce as i16)
        .unwrap();
    assert_eq!(produce.min_version, 0);
}

#[tokio::test]
async fn advertised_produce_versions_below_three_are_still_refused() {
    let broker = test_broker().await;

    let mut req = BytesMut::new();
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::Produce as i16)
        .with_request_api_version(2)
        .with_correlation_id(1);
    encode_request_header_into_buffer(&mut req, &header).unwrap();

    assert!(dispatch(&broker, &req).await.is_err());
}

#[tokio::test]
async fn future_api_versions_request_gets_the_supported_range() {
    let broker = test_broker().await;

    let mut req = BytesMut::new();
    let header = RequestHeader::default()
        .with_request_api_key(ApiKey::ApiVersions as i16)
        .with_request_api_version(99)
        .with_correlation_id(1);
    encode_request_header_into_buffer(&mut req, &header).unwrap();
    ApiVersionsRequest::default().encode(&mut req, 3).unwrap();

    let mut buf = response_body(&broker, &req).await;
    ResponseHeader::decode(&mut buf, 0).unwrap();
    let response = ApiVersionsResponse::decode(&mut buf, 0).unwrap();

    assert_eq!(response.error_code, 35);
    assert_eq!(response.api_keys.len(), 1);
    assert_eq!(response.api_keys[0].api_key, ApiKey::ApiVersions as i16);
    assert_eq!(response.api_keys[0].max_version, 4);
}

async fn resolve(outcome: HandlerOutcome) -> HandlerOutcome {
    match outcome {
        HandlerOutcome::Deferred(deferred) => deferred.await.unwrap(),
        other => other,
    }
}

async fn response_body(broker: &BrokerContext, req: &[u8]) -> Bytes {
    match resolve(dispatch(broker, req).await.unwrap()).await {
        HandlerOutcome::Response(frame) => frame.0,
        _ => panic!("expected response"),
    }
}

async fn call<Request: Encodable, Response: Decodable + HeaderVersion>(
    broker: &BrokerContext,
    api_key: ApiKey,
    version: i16,
    request: &Request,
) -> Response {
    let mut buf = response_body(broker, &encode_request(api_key, version, 1, request)).await;
    ResponseHeader::decode(&mut buf, Response::header_version(version)).unwrap();
    Response::decode(&mut buf, version).unwrap()
}

#[tokio::test]
async fn metadata_create_produce_and_fetch_roundtrip() {
    use kafka_protocol::messages::{FetchResponse, MetadataResponse, ProduceResponse};
    use kafka_protocol::records::RecordBatchDecoder;

    let broker = test_broker().await;
    let metadata_req = encode_request(
        ApiKey::Metadata,
        12,
        2,
        &MetadataRequest::default()
            .with_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(tn("events"))),
            ]))
            .with_allow_auto_topic_creation(true),
    );
    let mut buf = response_body(&broker, &metadata_req).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let metadata = MetadataResponse::decode(&mut buf, 12).unwrap();
    assert_eq!(metadata.topics.len(), 1);
    assert_eq!(metadata.topics[0].error_code, 0);
    let topic_id = metadata.topics[0].topic_id;
    assert!(!topic_id.is_nil());

    for (correlation, payload) in [(3, b"evt1"), (4, b"evt2")] {
        let produce_req = encode_request(
            ApiKey::Produce,
            10,
            correlation,
            &ProduceRequest::default().with_acks(1).with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(tn("events"))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(kafka_batch(payload))),
                    ]),
            ]),
        );
        let mut buf = response_body(&broker, &produce_req).await;
        ResponseHeader::decode(&mut buf, 1).unwrap();
        let produced = ProduceResponse::decode(&mut buf, 10).unwrap();
        let partition = &produced.responses[0].partition_responses[0];
        assert_eq!(partition.error_code, 0);
        assert_eq!(partition.base_offset, correlation as i64 - 3);
    }

    // Fetch v13+ addresses the topic by UUID, the way real clients do.
    let fetch_req = encode_request(
        ApiKey::Fetch,
        13,
        5,
        &FetchRequest::default()
            .with_max_wait_ms(0)
            .with_min_bytes(1)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic_id(topic_id)
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0),
                    ]),
            ]),
    );
    let mut buf = response_body(&broker, &fetch_req).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let fetched = FetchResponse::decode(&mut buf, 13).unwrap();
    let partition = &fetched.responses[0].partitions[0];
    assert_eq!(partition.error_code, 0);
    assert_eq!(partition.high_watermark, 2);
    // Stored batches carry broker-assigned base offsets: record offsets are
    // absolute, exactly what consumers advance on.
    let mut records_buf = partition.records.clone().unwrap();
    let sets = RecordBatchDecoder::decode_all(&mut records_buf).unwrap();
    let offsets: Vec<i64> = sets
        .iter()
        .flat_map(|set| set.records.iter().map(|record| record.offset))
        .collect();
    assert_eq!(offsets, vec![0, 1]);

    // Unknown topic UUID reports UNKNOWN_TOPIC_ID per partition.
    let unknown_req = encode_request(
        ApiKey::Fetch,
        13,
        6,
        &FetchRequest::default()
            .with_max_wait_ms(0)
            .with_min_bytes(1)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic_id(uuid::Uuid::new_v4())
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0),
                    ]),
            ]),
    );
    let mut buf = response_body(&broker, &unknown_req).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let unknown = FetchResponse::decode(&mut buf, 13).unwrap();
    assert_eq!(unknown.responses[0].partitions[0].error_code, 100);

    // Past-the-end fetch offsets are OFFSET_OUT_OF_RANGE, so consumers can
    // apply auto.offset.reset.
    let oor_req = encode_request(
        ApiKey::Fetch,
        13,
        7,
        &FetchRequest::default()
            .with_max_wait_ms(0)
            .with_min_bytes(1)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic_id(topic_id)
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(50),
                    ]),
            ]),
    );
    let mut buf = response_body(&broker, &oor_req).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let oor = FetchResponse::decode(&mut buf, 13).unwrap();
    assert_eq!(oor.responses[0].partitions[0].error_code, 1);
}

async fn metadata_topics(
    broker: &BrokerContext,
    version: i16,
    topics: Option<Vec<MetadataRequestTopic>>,
) -> Vec<String> {
    use kafka_protocol::messages::MetadataResponse;

    let request = encode_request(
        ApiKey::Metadata,
        version,
        1,
        &MetadataRequest::default().with_topics(topics),
    );

    let mut body = response_body(broker, &request).await;
    ResponseHeader::decode(&mut body, ApiKey::Metadata.response_header_version(version)).unwrap();

    MetadataResponse::decode(&mut body, version)
        .unwrap()
        .topics
        .iter()
        .filter_map(|topic| topic.name.as_ref().map(|name| name.0.to_string()))
        .collect()
}

#[tokio::test]
async fn metadata_empty_topic_list_means_all_topics_only_at_v0() {
    let broker = test_broker().await;

    let named = Some(vec![
        MetadataRequestTopic::default().with_name(Some(tn("listed"))),
    ]);
    assert_eq!(metadata_topics(&broker, 12, named).await, ["listed"]);

    assert_eq!(metadata_topics(&broker, 0, Some(vec![])).await, ["listed"]);
    assert_eq!(metadata_topics(&broker, 1, None).await, ["listed"]);

    assert!(metadata_topics(&broker, 1, Some(vec![])).await.is_empty());
    assert!(metadata_topics(&broker, 12, Some(vec![])).await.is_empty());
}

#[tokio::test]
async fn produce_rejects_a_batch_with_a_bad_crc() {
    use kafka_protocol::messages::ProduceResponse;

    let broker = test_broker().await;

    let create = Some(vec![
        MetadataRequestTopic::default().with_name(Some(tn("checked"))),
    ]);
    metadata_topics(&broker, 12, create).await;

    let mut batch = BytesMut::from(&kafka_batch(b"value")[..]);
    batch[17] ^= 0xff;

    let produce = encode_request(
        ApiKey::Produce,
        10,
        2,
        &ProduceRequest::default().with_acks(1).with_topic_data(vec![
            TopicProduceData::default()
                .with_name(tn("checked"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(batch.freeze())),
                ]),
        ]),
    );

    let mut buf = response_body(&broker, &produce).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let response = ProduceResponse::decode(&mut buf, 10).unwrap();

    assert_eq!(response.responses[0].partition_responses[0].error_code, 2);
}

async fn produce_at(broker: &BrokerContext, topic: &str, timestamp: i64) {
    let produce = encode_request(
        ApiKey::Produce,
        10,
        1,
        &ProduceRequest::default().with_acks(1).with_topic_data(vec![
            TopicProduceData::default()
                .with_name(tn(topic))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(timed_batch(b"value", timestamp))),
                ]),
        ]),
    );

    response_body(broker, &produce).await;
}

async fn offset_for(broker: &BrokerContext, topic: &str, timestamp: i64) -> (i64, i64) {
    use kafka_protocol::messages::ListOffsetsRequest;
    use kafka_protocol::messages::ListOffsetsResponse;
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};

    let request = encode_request(
        ApiKey::ListOffsets,
        7,
        1,
        &ListOffsetsRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(tn(topic))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(0)
                            .with_timestamp(timestamp),
                    ]),
            ]),
    );

    let mut buf = response_body(broker, &request).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let response = ListOffsetsResponse::decode(&mut buf, 7).unwrap();
    let partition = &response.topics[0].partitions[0];

    (partition.offset, partition.timestamp)
}

#[tokio::test]
async fn list_offsets_resolves_timestamps_like_kafka() {
    let broker = test_broker().await;

    let create = Some(vec![
        MetadataRequestTopic::default().with_name(Some(tn("timed"))),
    ]);
    metadata_topics(&broker, 12, create).await;

    for timestamp in [10, 30, 20] {
        produce_at(&broker, "timed", timestamp).await;
    }

    assert_eq!(offset_for(&broker, "timed", 0).await, (0, 10));
    assert_eq!(offset_for(&broker, "timed", 20).await, (1, 30));
    assert_eq!(offset_for(&broker, "timed", 31).await, (-1, -1));
    assert_eq!(offset_for(&broker, "timed", -3).await, (1, 30));
}

#[tokio::test]
async fn listener_serves_api_versions() {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let broker = Arc::new(test_broker().await);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        KafkaListener::new(
            ListenerConfig {
                addr,
                ..Default::default()
            },
            broker,
        )
        .serve(listener)
        .await
        .unwrap();
    });

    let req = encode_request(
        ApiKey::ApiVersions,
        3,
        42,
        &ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str("picomq"))
            .with_client_software_version(StrBytes::from_static_str("0.1")),
    );
    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut sized = BytesMut::new();
    sized.extend_from_slice(&(req.len() as i32).to_be_bytes());
    sized.extend_from_slice(&req);
    client.write_all(&sized).await.unwrap();

    let mut len_buf = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut len_buf))
        .await
        .unwrap()
        .unwrap();
    let size = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; size];
    client.read_exact(&mut body).await.unwrap();
    let mut buf = Bytes::from(body);
    let header = ResponseHeader::decode(&mut buf, 0).unwrap();
    assert_eq!(header.correlation_id, 42);

    server.abort();
}

/// An acks=0 produce yields no response frame, so the ordered writer must
/// skip its slot instead of wedging every later response.
#[tokio::test]
async fn listener_pipeline_survives_acks_zero() {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let broker = test_broker().await;
    // Create the topic ahead of time via auto-create metadata.
    let metadata_req = encode_request(
        ApiKey::Metadata,
        12,
        1,
        &MetadataRequest::default()
            .with_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(tn("fire"))),
            ]))
            .with_allow_auto_topic_creation(true),
    );
    dispatch(&broker, &metadata_req).await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let broker = Arc::new(broker);
    let server = tokio::spawn(async move {
        KafkaListener::new(
            ListenerConfig {
                addr,
                ..Default::default()
            },
            broker,
        )
        .serve(listener)
        .await
        .unwrap();
    });

    let produce_req = encode_request(
        ApiKey::Produce,
        10,
        7,
        &ProduceRequest::default().with_acks(0).with_topic_data(vec![
            TopicProduceData::default()
                .with_name(tn("fire"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(kafka_batch(b"fire-and-forget"))),
                ]),
        ]),
    );
    let versions_req = encode_request(
        ApiKey::ApiVersions,
        3,
        8,
        &ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str("picomq"))
            .with_client_software_version(StrBytes::from_static_str("0.1")),
    );

    let mut client = TcpStream::connect(addr).await.unwrap();
    let mut pipelined = BytesMut::new();
    for req in [&produce_req, &versions_req] {
        pipelined.extend_from_slice(&(req.len() as i32).to_be_bytes());
        pipelined.extend_from_slice(req);
    }
    client.write_all(&pipelined).await.unwrap();

    // The first (and only) response frame must be the ApiVersions reply.
    let mut len_buf = [0u8; 4];
    tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut len_buf))
        .await
        .expect("pipeline wedged after acks=0 produce")
        .unwrap();
    let size = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; size];
    client.read_exact(&mut body).await.unwrap();
    let mut buf = Bytes::from(body);
    let header = ResponseHeader::decode(&mut buf, 0).unwrap();
    assert_eq!(header.correlation_id, 8);

    server.abort();
}

/// FindCoordinator is a read: probing arbitrary group ids must not mint
/// durable streams, and fetching offsets for an unknown group returns -1
/// sentinels rather than an error.
#[tokio::test]
async fn find_coordinator_probe_creates_nothing() {
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
    use kafka_protocol::messages::{
        FindCoordinatorRequest, FindCoordinatorResponse, GroupId, OffsetFetchRequest,
        OffsetFetchResponse,
    };
    use kafka_protocol::protocol::HeaderVersion;

    let broker = test_broker().await;
    let find = encode_request(
        ApiKey::FindCoordinator,
        3,
        30,
        &FindCoordinatorRequest::default()
            .with_key(StrBytes::from_static_str("probe"))
            .with_key_type(0),
    );
    let mut buf = response_body(&broker, &find).await;
    ResponseHeader::decode(&mut buf, FindCoordinatorResponse::header_version(3)).unwrap();
    let found = FindCoordinatorResponse::decode(&mut buf, 3).unwrap();
    assert_eq!(found.error_code, 0);
    assert_eq!(found.node_id.0, 1);

    let group_log = picomq_server::GroupCoordinator::log("probe");
    assert!(broker.views.load().state.get_kv(&group_log.key()).is_none());

    let fetch = encode_request(
        ApiKey::OffsetFetch,
        7,
        31,
        &OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("probe")))
            .with_topics(Some(vec![
                OffsetFetchRequestTopic::default()
                    .with_name(tn("events"))
                    .with_partition_indexes(vec![0]),
            ])),
    );
    let mut buf = response_body(&broker, &fetch).await;
    ResponseHeader::decode(&mut buf, OffsetFetchResponse::header_version(7)).unwrap();
    let fetched = OffsetFetchResponse::decode(&mut buf, 7).unwrap();
    assert_eq!(fetched.error_code, 0);
    assert_eq!(fetched.topics[0].partitions[0].committed_offset, -1);
    assert!(broker.views.load().state.get_kv(&group_log.key()).is_none());
}

/// Join, rebalance, fencing, and expiry across two members.
#[tokio::test]
async fn classic_group_two_member_rebalance_and_expiry() {
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
    use kafka_protocol::messages::{
        GroupId, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
        SyncGroupRequest, SyncGroupResponse,
    };
    use kafka_protocol::protocol::HeaderVersion;

    let broker = Arc::new(test_broker().await);
    let group_id = GroupId(StrBytes::from_static_str("pair"));
    let session_timeout_ms = 2_000;

    let join_req = |member_id: StrBytes, correlation_id| {
        encode_request(
            ApiKey::JoinGroup,
            5,
            correlation_id,
            &JoinGroupRequest::default()
                .with_group_id(group_id.clone())
                .with_session_timeout_ms(session_timeout_ms)
                .with_rebalance_timeout_ms(10_000)
                .with_member_id(member_id)
                .with_protocol_type(StrBytes::from_static_str("consumer"))
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(StrBytes::from_static_str("range"))
                        .with_metadata(Bytes::from_static(b"subscription")),
                ]),
        )
    };
    let decode_join = |mut buf: Bytes| {
        ResponseHeader::decode(&mut buf, JoinGroupResponse::header_version(5)).unwrap();
        JoinGroupResponse::decode(&mut buf, 5).unwrap()
    };
    let heartbeat_req = |member_id: StrBytes, generation_id, correlation_id| {
        encode_request(
            ApiKey::Heartbeat,
            4,
            correlation_id,
            &HeartbeatRequest::default()
                .with_group_id(group_id.clone())
                .with_generation_id(generation_id)
                .with_member_id(member_id),
        )
    };
    let heartbeat_code = |mut buf: Bytes| {
        ResponseHeader::decode(&mut buf, HeartbeatResponse::header_version(4)).unwrap();
        HeartbeatResponse::decode(&mut buf, 4).unwrap().error_code
    };

    // Member 1 bootstraps the group and becomes leader of generation 1.
    let required = decode_join(response_body(&broker, &join_req(StrBytes::default(), 40)).await);
    assert_eq!(required.error_code, 79);
    let member1 = required.member_id;
    let joined1 = decode_join(response_body(&broker, &join_req(member1.clone(), 41)).await);
    assert_eq!(joined1.error_code, 0);
    assert_eq!(joined1.generation_id, 1);
    assert_eq!(joined1.leader, member1);

    let sync_req = |member_id: StrBytes, generation_id, assignments, correlation_id| {
        encode_request(
            ApiKey::SyncGroup,
            5,
            correlation_id,
            &SyncGroupRequest::default()
                .with_group_id(group_id.clone())
                .with_generation_id(generation_id)
                .with_member_id(member_id)
                .with_protocol_type(Some(StrBytes::from_static_str("consumer")))
                .with_protocol_name(Some(StrBytes::from_static_str("range")))
                .with_assignments(assignments),
        )
    };
    let decode_sync = |mut buf: Bytes| {
        ResponseHeader::decode(&mut buf, SyncGroupResponse::header_version(5)).unwrap();
        SyncGroupResponse::decode(&mut buf, 5).unwrap()
    };
    let synced = decode_sync(
        response_body(
            &broker,
            &sync_req(
                member1.clone(),
                1,
                vec![
                    SyncGroupRequestAssignment::default()
                        .with_member_id(member1.clone())
                        .with_assignment(Bytes::from_static(b"all")),
                ],
                42,
            ),
        )
        .await,
    );
    assert_eq!(synced.error_code, 0);

    // Member 2's join and the leader's rejoin complete as generation 2.
    let required = decode_join(response_body(&broker, &join_req(StrBytes::default(), 43)).await);
    assert_eq!(required.error_code, 79);
    let member2 = required.member_id;

    let rejoin_broker = Arc::clone(&broker);
    let rejoin1 = {
        let req = join_req(member1.clone(), 44);
        tokio::spawn(async move { response_body(&rejoin_broker, &req).await })
    };
    let joined2 = decode_join(response_body(&broker, &join_req(member2.clone(), 45)).await);
    let rejoined1 = decode_join(rejoin1.await.unwrap());
    assert_eq!(joined2.error_code, 0);
    assert_eq!(rejoined1.error_code, 0);
    assert_eq!(joined2.generation_id, 2);
    assert_eq!(rejoined1.generation_id, 2);
    // The incumbent leader is retained and only it sees the member list.
    assert_eq!(rejoined1.leader, member1);
    assert_eq!(joined2.leader, member1);
    assert_eq!(rejoined1.members.len(), 2);
    assert!(joined2.members.is_empty());

    let synced1 = decode_sync(
        response_body(
            &broker,
            &sync_req(
                member1.clone(),
                2,
                vec![
                    SyncGroupRequestAssignment::default()
                        .with_member_id(member1.clone())
                        .with_assignment(Bytes::from_static(b"left")),
                    SyncGroupRequestAssignment::default()
                        .with_member_id(member2.clone())
                        .with_assignment(Bytes::from_static(b"right")),
                ],
                46,
            ),
        )
        .await,
    );
    assert_eq!(synced1.error_code, 0);
    assert_eq!(synced1.assignment, Bytes::from_static(b"left"));
    let synced2 =
        decode_sync(response_body(&broker, &sync_req(member2.clone(), 2, Vec::new(), 47)).await);
    assert_eq!(synced2.error_code, 0);
    assert_eq!(synced2.assignment, Bytes::from_static(b"right"));

    // A heartbeat carrying the previous generation is fenced.
    let code = heartbeat_code(response_body(&broker, &heartbeat_req(member1.clone(), 1, 48)).await);
    assert_eq!(code, 22);

    // Member 2 goes silent. Member 1 keeps heartbeating: first inside the
    // session timeout (fine), then after member 2's session lapses, at which
    // point the coordinator expires it and demands a rebalance.
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let code = heartbeat_code(response_body(&broker, &heartbeat_req(member1.clone(), 2, 49)).await);
    assert_eq!(code, 0);
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let code = heartbeat_code(response_body(&broker, &heartbeat_req(member1.clone(), 2, 50)).await);
    assert_eq!(code, 27);
}

/// Static membership: a restarted instance rejoining with its
/// group.instance.id gets the same member id back, and requests carrying a
/// mismatched instance id are fenced.
#[tokio::test]
async fn static_membership_rejoin_and_fencing() {
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::{
        GroupId, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
    };
    use kafka_protocol::protocol::HeaderVersion;

    let broker = test_broker().await;
    let group_id = GroupId(StrBytes::from_static_str("static"));
    let instance = StrBytes::from_static_str("instance-1");

    let join_req = |member_id: StrBytes, correlation_id| {
        encode_request(
            ApiKey::JoinGroup,
            5,
            correlation_id,
            &JoinGroupRequest::default()
                .with_group_id(group_id.clone())
                .with_session_timeout_ms(10_000)
                .with_rebalance_timeout_ms(10_000)
                .with_member_id(member_id)
                .with_group_instance_id(Some(instance.clone()))
                .with_protocol_type(StrBytes::from_static_str("consumer"))
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(StrBytes::from_static_str("range"))
                        .with_metadata(Bytes::from_static(b"subscription")),
                ]),
        )
    };
    let decode_join = |mut buf: Bytes| {
        ResponseHeader::decode(&mut buf, JoinGroupResponse::header_version(5)).unwrap();
        JoinGroupResponse::decode(&mut buf, 5).unwrap()
    };

    let required = decode_join(response_body(&broker, &join_req(StrBytes::default(), 60)).await);
    assert_eq!(required.error_code, 79);
    let member = required.member_id;
    let joined = decode_join(response_body(&broker, &join_req(member.clone(), 61)).await);
    assert_eq!(joined.error_code, 0);
    assert_eq!(joined.generation_id, 1);

    // Restarted process: empty member id + the same instance id resolves to
    // the existing member instead of registering a duplicate.
    let rejoined = decode_join(response_body(&broker, &join_req(StrBytes::default(), 62)).await);
    assert_eq!(rejoined.error_code, 0);
    assert_eq!(rejoined.member_id, member);
    assert_eq!(rejoined.generation_id, 2);

    // A request using the member id without its instance id is fenced.
    let heartbeat = encode_request(
        ApiKey::Heartbeat,
        4,
        63,
        &HeartbeatRequest::default()
            .with_group_id(group_id)
            .with_generation_id(rejoined.generation_id)
            .with_member_id(member),
    );
    let mut buf = response_body(&broker, &heartbeat).await;
    ResponseHeader::decode(&mut buf, HeartbeatResponse::header_version(4)).unwrap();
    assert_eq!(
        HeartbeatResponse::decode(&mut buf, 4).unwrap().error_code,
        82
    );
}

#[tokio::test]
async fn connect_shaped_group_passes_foreign_protocols_through() {
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
    use kafka_protocol::messages::{
        DescribeGroupsRequest, DescribeGroupsResponse, GroupId, JoinGroupRequest,
        JoinGroupResponse, SyncGroupRequest, SyncGroupResponse,
    };
    use kafka_protocol::protocol::HeaderVersion;

    let broker = test_broker().await;
    let group_id = GroupId(StrBytes::from_static_str("connect-cluster"));
    let worker_state = Bytes::from_static(b"\x00\x02sessioned worker metadata");
    let join = |member_id: StrBytes, correlation_id| {
        encode_request(
            ApiKey::JoinGroup,
            7,
            correlation_id,
            &JoinGroupRequest::default()
                .with_group_id(group_id.clone())
                .with_session_timeout_ms(10_000)
                .with_rebalance_timeout_ms(60_000)
                .with_member_id(member_id)
                .with_protocol_type(StrBytes::from_static_str("connect"))
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(StrBytes::from_static_str("sessioned"))
                        .with_metadata(worker_state.clone()),
                    JoinGroupRequestProtocol::default()
                        .with_name(StrBytes::from_static_str("compatible"))
                        .with_metadata(Bytes::from_static(b"older")),
                ]),
        )
    };
    let mut buf = response_body(&broker, &join(StrBytes::default(), 60)).await;
    ResponseHeader::decode(&mut buf, JoinGroupResponse::header_version(7)).unwrap();
    let required = JoinGroupResponse::decode(&mut buf, 7).unwrap();
    assert_eq!(required.error_code, 79);
    let mut buf = response_body(&broker, &join(required.member_id.clone(), 61)).await;
    ResponseHeader::decode(&mut buf, JoinGroupResponse::header_version(7)).unwrap();
    let joined = JoinGroupResponse::decode(&mut buf, 7).unwrap();
    assert_eq!(joined.error_code, 0);
    assert_eq!(joined.protocol_type.as_ref().unwrap().as_str(), "connect");
    assert_eq!(joined.protocol_name.as_ref().unwrap().as_str(), "sessioned");
    assert_eq!(joined.leader, joined.member_id);
    assert_eq!(joined.members[0].metadata, worker_state);

    let task_assignment = Bytes::from_static(b"\x00\x02connector and task ids");
    let sync = encode_request(
        ApiKey::SyncGroup,
        5,
        62,
        &SyncGroupRequest::default()
            .with_group_id(group_id.clone())
            .with_generation_id(joined.generation_id)
            .with_member_id(joined.member_id.clone())
            .with_protocol_type(Some(StrBytes::from_static_str("connect")))
            .with_protocol_name(Some(StrBytes::from_static_str("sessioned")))
            .with_assignments(vec![
                SyncGroupRequestAssignment::default()
                    .with_member_id(joined.member_id.clone())
                    .with_assignment(task_assignment.clone()),
            ]),
    );
    let mut buf = response_body(&broker, &sync).await;
    ResponseHeader::decode(&mut buf, SyncGroupResponse::header_version(5)).unwrap();
    let synced = SyncGroupResponse::decode(&mut buf, 5).unwrap();
    assert_eq!(synced.error_code, 0);
    assert_eq!(synced.protocol_type.as_ref().unwrap().as_str(), "connect");
    assert_eq!(synced.assignment, task_assignment);

    let describe = encode_request(
        ApiKey::DescribeGroups,
        5,
        63,
        &DescribeGroupsRequest::default().with_groups(vec![group_id]),
    );
    let mut buf = response_body(&broker, &describe).await;
    ResponseHeader::decode(&mut buf, DescribeGroupsResponse::header_version(5)).unwrap();
    let described = DescribeGroupsResponse::decode(&mut buf, 5).unwrap();
    let group = &described.groups[0];
    assert_eq!(group.error_code, 0);
    assert_eq!(group.group_state.as_str(), "Stable");
    assert_eq!(group.protocol_type.as_str(), "connect");
    assert_eq!(group.protocol_data.as_str(), "sessioned");
    assert_eq!(group.members[0].member_metadata, worker_state);
    assert_eq!(group.members[0].member_assignment, task_assignment);
}

#[tokio::test]
async fn classic_group_lifecycle_and_offset_replay() {
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
    use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
    use kafka_protocol::messages::{
        FindCoordinatorRequest, FindCoordinatorResponse, GroupId, HeartbeatRequest,
        HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
        LeaveGroupResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
        OffsetFetchResponse, SyncGroupRequest, SyncGroupResponse,
    };
    use kafka_protocol::protocol::HeaderVersion;

    let broker = test_broker().await;
    broker
        .topics
        .create(picomq_server::CreateCommand::new(
            "/events",
            "application/octet-stream",
        ))
        .await
        .unwrap();
    let group_id = GroupId(StrBytes::from_static_str("workers"));

    let find = encode_request(
        ApiKey::FindCoordinator,
        3,
        20,
        &FindCoordinatorRequest::default()
            .with_key(StrBytes::from_static_str("workers"))
            .with_key_type(0),
    );
    let mut buf = response_body(&broker, &find).await;
    ResponseHeader::decode(&mut buf, FindCoordinatorResponse::header_version(3)).unwrap();
    let found = FindCoordinatorResponse::decode(&mut buf, 3).unwrap();
    assert_eq!(found.error_code, 0);
    assert_eq!(found.node_id.0, 1);
    assert_eq!(found.host.as_str(), "127.0.0.1");
    assert_eq!(found.port, 19092);

    let join = |member_id: StrBytes, correlation_id| {
        encode_request(
            ApiKey::JoinGroup,
            5,
            correlation_id,
            &JoinGroupRequest::default()
                .with_group_id(group_id.clone())
                .with_session_timeout_ms(10_000)
                .with_rebalance_timeout_ms(10_000)
                .with_member_id(member_id)
                .with_protocol_type(StrBytes::from_static_str("consumer"))
                .with_protocols(vec![
                    JoinGroupRequestProtocol::default()
                        .with_name(StrBytes::from_static_str("range"))
                        .with_metadata(Bytes::from_static(b"subscription")),
                ]),
        )
    };
    let mut buf = response_body(&broker, &join(StrBytes::default(), 21)).await;
    ResponseHeader::decode(&mut buf, JoinGroupResponse::header_version(5)).unwrap();
    let member_required = JoinGroupResponse::decode(&mut buf, 5).unwrap();
    assert_eq!(member_required.error_code, 79);
    assert!(!member_required.member_id.is_empty());

    let mut buf = response_body(&broker, &join(member_required.member_id.clone(), 22)).await;
    ResponseHeader::decode(&mut buf, JoinGroupResponse::header_version(5)).unwrap();
    let joined = JoinGroupResponse::decode(&mut buf, 5).unwrap();
    assert_eq!(joined.error_code, 0);
    assert_eq!(joined.generation_id, 1);
    assert_eq!(joined.leader, joined.member_id);
    assert_eq!(joined.members.len(), 1);

    let assignment = Bytes::from_static(b"assignment");
    let sync = encode_request(
        ApiKey::SyncGroup,
        5,
        23,
        &SyncGroupRequest::default()
            .with_group_id(group_id.clone())
            .with_generation_id(joined.generation_id)
            .with_member_id(joined.member_id.clone())
            .with_protocol_type(Some(StrBytes::from_static_str("consumer")))
            .with_protocol_name(Some(StrBytes::from_static_str("range")))
            .with_assignments(vec![
                SyncGroupRequestAssignment::default()
                    .with_member_id(joined.member_id.clone())
                    .with_assignment(assignment.clone()),
            ]),
    );
    let mut buf = response_body(&broker, &sync).await;
    ResponseHeader::decode(&mut buf, SyncGroupResponse::header_version(5)).unwrap();
    let synced = SyncGroupResponse::decode(&mut buf, 5).unwrap();
    assert_eq!(synced.error_code, 0);
    assert_eq!(synced.assignment, assignment);

    let heartbeat = encode_request(
        ApiKey::Heartbeat,
        4,
        24,
        &HeartbeatRequest::default()
            .with_group_id(group_id.clone())
            .with_generation_id(joined.generation_id)
            .with_member_id(joined.member_id.clone()),
    );
    let mut buf = response_body(&broker, &heartbeat).await;
    ResponseHeader::decode(&mut buf, HeartbeatResponse::header_version(4)).unwrap();
    assert_eq!(
        HeartbeatResponse::decode(&mut buf, 4).unwrap().error_code,
        0
    );

    let commit = encode_request(
        ApiKey::OffsetCommit,
        7,
        25,
        &OffsetCommitRequest::default()
            .with_group_id(group_id.clone())
            .with_generation_id_or_member_epoch(joined.generation_id)
            .with_member_id(joined.member_id.clone())
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(tn("events"))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(42)
                            .with_committed_leader_epoch(3)
                            .with_committed_metadata(Some(StrBytes::from_static_str("checkpoint"))),
                    ]),
            ]),
    );
    let mut buf = response_body(&broker, &commit).await;
    ResponseHeader::decode(&mut buf, OffsetCommitResponse::header_version(7)).unwrap();
    let committed = OffsetCommitResponse::decode(&mut buf, 7).unwrap();
    assert_eq!(committed.topics[0].partitions[0].error_code, 0);

    let fetch = encode_request(
        ApiKey::OffsetFetch,
        7,
        26,
        &OffsetFetchRequest::default()
            .with_group_id(group_id.clone())
            .with_topics(Some(vec![
                OffsetFetchRequestTopic::default()
                    .with_name(tn("events"))
                    .with_partition_indexes(vec![0]),
            ]))
            .with_require_stable(true),
    );
    let mut buf = response_body(&broker, &fetch).await;
    ResponseHeader::decode(&mut buf, OffsetFetchResponse::header_version(7)).unwrap();
    let fetched = OffsetFetchResponse::decode(&mut buf, 7).unwrap();
    assert_eq!(fetched.error_code, 0);
    let partition = &fetched.topics[0].partitions[0];
    assert_eq!(partition.committed_offset, 42);
    assert_eq!(partition.committed_leader_epoch, 3);
    assert_eq!(partition.metadata.as_ref().unwrap().as_str(), "checkpoint");

    let leave = encode_request(
        ApiKey::LeaveGroup,
        2,
        27,
        &LeaveGroupRequest::default()
            .with_group_id(group_id)
            .with_member_id(joined.member_id),
    );
    let mut buf = response_body(&broker, &leave).await;
    ResponseHeader::decode(&mut buf, LeaveGroupResponse::header_version(2)).unwrap();
    assert_eq!(
        LeaveGroupResponse::decode(&mut buf, 2).unwrap().error_code,
        0
    );
}

#[tokio::test]
async fn create_topics_binds_picomq_schema_and_validates_produce() {
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
    use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse, ProduceResponse};
    use picomq_schema::SchemaFormat;

    let registry = picomq_schema::Registry::new(object_store::memory::InMemory::new());
    let schema = bytes::Bytes::from_static(
        br#"{
        "title": "Person",
        "type": "object",
        "properties": {
            "value": {
                "type": "object",
                "properties": {
                    "name": { "type": "string" }
                },
                "required": ["name"]
            }
        }
    }"#,
    );
    registry
        .put("person", SchemaFormat::Json, schema)
        .await
        .unwrap();

    let broker = broker_with(Some(Arc::new(registry))).await;

    let create_req = encode_request(
        ApiKey::CreateTopics,
        5,
        1,
        &CreateTopicsRequest::default()
            .with_timeout_ms(5_000)
            .with_topics(vec![
                CreatableTopic::default()
                    .with_name(tn("orders"))
                    .with_num_partitions(1)
                    .with_replication_factor(1)
                    .with_configs(vec![
                        CreatableTopicConfig::default()
                            .with_name(StrBytes::from_static_str("pico.schema"))
                            .with_value(Some(StrBytes::from_static_str("person"))),
                        CreatableTopicConfig::default()
                            .with_name(StrBytes::from_static_str("pico.schema.validate"))
                            .with_value(Some(StrBytes::from_static_str("true"))),
                    ]),
            ]),
    );
    let mut buf = response_body(&broker, &create_req).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let created = CreateTopicsResponse::decode(&mut buf, 5).unwrap();
    assert_eq!(created.topics[0].error_code, 0);
    assert_eq!(
        broker
            .topics
            .get("/orders")
            .await
            .unwrap()
            .unwrap()
            .schema_name
            .as_deref(),
        Some("person")
    );

    let bad = encode_request(
        ApiKey::Produce,
        10,
        2,
        &ProduceRequest::default().with_acks(1).with_topic_data(vec![
            TopicProduceData::default()
                .with_name(tn("orders"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(kafka_batch(br#"{"name":1}"#))),
                ]),
        ]),
    );
    let mut buf = response_body(&broker, &bad).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let produced = ProduceResponse::decode(&mut buf, 10).unwrap();
    assert_eq!(produced.responses[0].partition_responses[0].error_code, 87);

    let ok = encode_request(
        ApiKey::Produce,
        10,
        3,
        &ProduceRequest::default().with_acks(1).with_topic_data(vec![
            TopicProduceData::default()
                .with_name(tn("orders"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(kafka_batch(br#"{"name":"alice"}"#))),
                ]),
        ]),
    );
    let mut buf = response_body(&broker, &ok).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let produced = ProduceResponse::decode(&mut buf, 10).unwrap();
    assert_eq!(produced.responses[0].partition_responses[0].error_code, 0);
}

#[tokio::test]
async fn partitioned_topics_create_produce_fetch_and_grow() {
    use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
    use kafka_protocol::messages::create_topics_request::CreatableTopic;
    use kafka_protocol::messages::{
        CreatePartitionsRequest, CreatePartitionsResponse, CreateTopicsRequest,
        CreateTopicsResponse, FetchResponse, ProduceResponse,
    };

    let broker = test_broker().await;

    let create = encode_request(
        ApiKey::CreateTopics,
        5,
        1,
        &CreateTopicsRequest::default()
            .with_timeout_ms(5000)
            .with_topics(vec![
                CreatableTopic::default()
                    .with_name(tn("parted"))
                    .with_num_partitions(3)
                    .with_replication_factor(1),
            ]),
    );
    let mut buf = response_body(&broker, &create).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let created = CreateTopicsResponse::decode(&mut buf, 5).unwrap();
    assert_eq!(created.topics[0].error_code, 0);
    assert_eq!(created.topics[0].num_partitions, 3);

    let produce = encode_request(
        ApiKey::Produce,
        10,
        2,
        &ProduceRequest::default().with_acks(1).with_topic_data(vec![
            TopicProduceData::default()
                .with_name(tn("parted"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(2)
                        .with_records(Some(kafka_batch(b"two"))),
                    PartitionProduceData::default()
                        .with_index(3)
                        .with_records(Some(kafka_batch(b"none"))),
                ]),
        ]),
    );
    let mut buf = response_body(&broker, &produce).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let produced = ProduceResponse::decode(&mut buf, 10).unwrap();
    let results = &produced.responses[0].partition_responses;
    assert_eq!((results[0].index, results[0].error_code), (2, 0));
    assert_eq!((results[1].index, results[1].error_code), (3, 3));

    let fetch = encode_request(
        ApiKey::Fetch,
        12,
        3,
        &FetchRequest::default()
            .with_max_wait_ms(0)
            .with_min_bytes(1)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(tn("parted"))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0),
                        FetchPartition::default()
                            .with_partition(2)
                            .with_fetch_offset(0),
                    ]),
            ]),
    );
    let mut buf = response_body(&broker, &fetch).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let fetched = FetchResponse::decode(&mut buf, 12).unwrap();
    let partitions = &fetched.responses[0].partitions;
    assert_eq!(
        (partitions[0].partition_index, partitions[0].high_watermark),
        (0, 0)
    );
    assert_eq!(
        (partitions[1].partition_index, partitions[1].high_watermark),
        (2, 1)
    );

    let grow = |count: i32| {
        encode_request(
            ApiKey::CreatePartitions,
            3,
            4,
            &CreatePartitionsRequest::default()
                .with_timeout_ms(5000)
                .with_topics(vec![
                    CreatePartitionsTopic::default()
                        .with_name(tn("parted"))
                        .with_count(count)
                        .with_assignments(None),
                ]),
        )
    };
    for (count, code) in [(5, 0), (4, 37), (5, 37)] {
        let mut buf = response_body(&broker, &grow(count)).await;
        ResponseHeader::decode(&mut buf, 1).unwrap();
        let grown = CreatePartitionsResponse::decode(&mut buf, 3).unwrap();
        assert_eq!(grown.results[0].error_code, code, "grow to {count}");
    }

    let topic = broker.topics.get("/parted").await.unwrap().unwrap();
    assert_eq!(topic.partitions, 5);
}

async fn create_topics(
    broker: &BrokerContext,
    topics: Vec<kafka_protocol::messages::create_topics_request::CreatableTopic>,
    validate_only: bool,
) -> kafka_protocol::messages::CreateTopicsResponse {
    use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse};

    let request = encode_request(
        ApiKey::CreateTopics,
        5,
        1,
        &CreateTopicsRequest::default()
            .with_timeout_ms(5000)
            .with_validate_only(validate_only)
            .with_topics(topics),
    );

    let mut buf = response_body(broker, &request).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    CreateTopicsResponse::decode(&mut buf, 5).unwrap()
}

async fn create_partitions(
    broker: &BrokerContext,
    topic: kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic,
    validate_only: bool,
) -> (i16, Option<String>) {
    use kafka_protocol::messages::{CreatePartitionsRequest, CreatePartitionsResponse};

    let request = encode_request(
        ApiKey::CreatePartitions,
        3,
        1,
        &CreatePartitionsRequest::default()
            .with_timeout_ms(5000)
            .with_validate_only(validate_only)
            .with_topics(vec![topic]),
    );

    let mut buf = response_body(broker, &request).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let result = CreatePartitionsResponse::decode(&mut buf, 3)
        .unwrap()
        .results
        .remove(0);
    (
        result.error_code,
        result.error_message.map(|message| message.to_string()),
    )
}

fn creatable(
    name: &str,
    partitions: i32,
    replication: i16,
    assignments: &[(i32, &[i32])],
) -> kafka_protocol::messages::create_topics_request::CreatableTopic {
    use kafka_protocol::messages::create_topics_request::{
        CreatableReplicaAssignment, CreatableTopic,
    };

    CreatableTopic::default()
        .with_name(tn(name))
        .with_num_partitions(partitions)
        .with_replication_factor(replication)
        .with_assignments(
            assignments
                .iter()
                .map(|(index, brokers)| {
                    CreatableReplicaAssignment::default()
                        .with_partition_index(*index)
                        .with_broker_ids(brokers.iter().map(|id| BrokerId(*id)).collect())
                })
                .collect(),
        )
}

#[tokio::test]
async fn create_topics_checks_partitions_replicas_and_assignments() {
    let broker = test_broker().await;

    let created = create_topics(
        &broker,
        vec![
            creatable("defaulted", -1, -1, &[]),
            creatable("zero", 0, 1, &[]),
            creatable("replicated", 1, 3, &[]),
            creatable("manual", -1, -1, &[(0, &[1]), (1, &[1])]),
            creatable("manual-factor", -1, 1, &[(0, &[1])]),
            creatable("manual-count", 2, -1, &[(0, &[1])]),
            creatable("gap", -1, -1, &[(1, &[1])]),
            creatable("copies", -1, -1, &[(0, &[1, 1])]),
            creatable("unregistered", -1, -1, &[(0, &[7])]),
        ],
        false,
    )
    .await;

    let outcomes: Vec<(String, i16, i32)> = created
        .topics
        .iter()
        .map(|topic| {
            (
                topic.name.to_string(),
                topic.error_code,
                topic.num_partitions,
            )
        })
        .collect();
    assert_eq!(
        outcomes,
        vec![
            ("defaulted".to_owned(), 0, 1),
            ("zero".to_owned(), 37, -1),
            ("replicated".to_owned(), 38, -1),
            ("manual".to_owned(), 0, 2),
            ("manual-factor".to_owned(), 38, -1),
            ("manual-count".to_owned(), 37, -1),
            ("gap".to_owned(), 39, -1),
            ("copies".to_owned(), 39, -1),
            ("unregistered".to_owned(), 39, -1),
        ]
    );
    assert!(
        created.topics[1..]
            .iter()
            .filter(|topic| topic.error_code != 0)
            .all(|topic| topic.error_message.is_some())
    );
    assert!(broker.topics.get("/zero").await.unwrap().is_none());
    assert_eq!(
        broker
            .topics
            .get("/manual")
            .await
            .unwrap()
            .unwrap()
            .partitions,
        2
    );
}

#[tokio::test]
async fn create_topics_validate_only_creates_nothing() {
    let broker = test_broker().await;
    create_topics(&broker, vec![creatable("existing", 1, 1, &[])], false).await;

    let validated = create_topics(
        &broker,
        vec![
            creatable("dry", 4, 1, &[]),
            creatable("existing", 1, 1, &[]),
        ],
        true,
    )
    .await;

    assert_eq!(validated.topics[0].error_code, 0);
    assert_eq!(validated.topics[0].num_partitions, 4);
    assert_eq!(validated.topics[1].error_code, 36);
    assert!(broker.topics.get("/dry").await.unwrap().is_none());
}

#[tokio::test]
async fn excessive_partition_requests_are_policy_violations() {
    use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;

    let broker = test_broker().await;

    let created = create_topics(
        &broker,
        vec![
            creatable("half", 6000, 1, &[]),
            creatable("other", 6000, 1, &[]),
        ],
        false,
    )
    .await;
    for topic in &created.topics {
        assert_eq!(topic.error_code, 44);
        assert_eq!(
            topic.error_message.as_deref(),
            Some("Excessively large number of partitions per request.")
        );
    }
    assert!(broker.topics.get("/half").await.unwrap().is_none());

    create_topics(&broker, vec![creatable("small", 1, 1, &[])], false).await;
    let (code, _) = create_partitions(
        &broker,
        CreatePartitionsTopic::default()
            .with_name(tn("small"))
            .with_count(10_002)
            .with_assignments(None),
        false,
    )
    .await;
    assert_eq!(code, 44);
}

#[tokio::test]
async fn create_partitions_validates_assignments_and_dry_runs() {
    use kafka_protocol::messages::create_partitions_request::{
        CreatePartitionsAssignment, CreatePartitionsTopic,
    };

    let broker = test_broker().await;
    create_topics(&broker, vec![creatable("grown", 1, 1, &[])], false).await;

    let assigned = |brokers: &[&[i32]]| {
        Some(
            brokers
                .iter()
                .map(|ids| {
                    CreatePartitionsAssignment::default()
                        .with_broker_ids(ids.iter().map(|id| BrokerId(*id)).collect())
                })
                .collect(),
        )
    };
    let grow = |count: i32, assignments| {
        CreatePartitionsTopic::default()
            .with_name(tn("grown"))
            .with_count(count)
            .with_assignments(assignments)
    };

    assert_eq!(create_partitions(&broker, grow(3, None), true).await.0, 0);
    assert_eq!(
        broker
            .topics
            .get("/grown")
            .await
            .unwrap()
            .unwrap()
            .partitions,
        1
    );

    let (code, message) = create_partitions(&broker, grow(3, assigned(&[&[1]])), false).await;
    assert_eq!(code, 39);
    assert!(message.unwrap().contains("Attempted to add 2"));

    let (code, _) = create_partitions(&broker, grow(2, assigned(&[&[7]])), false).await;
    assert_eq!(code, 39);

    let (code, _) = create_partitions(&broker, grow(3, assigned(&[&[1], &[1]])), false).await;
    assert_eq!(code, 0);
    assert_eq!(
        broker
            .topics
            .get("/grown")
            .await
            .unwrap()
            .unwrap()
            .partitions,
        3
    );
}

#[tokio::test]
async fn offsets_and_commits_address_partitions_past_zero() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
    use kafka_protocol::messages::{
        GroupId, ListOffsetsRequest, ListOffsetsResponse, OffsetCommitRequest,
        OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    };
    use kafka_protocol::protocol::HeaderVersion;

    let broker = test_broker().await;
    create_topics(&broker, vec![creatable("spread", 3, 1, &[])], false).await;

    let produce = encode_request(
        ApiKey::Produce,
        10,
        2,
        &ProduceRequest::default().with_acks(1).with_topic_data(vec![
            TopicProduceData::default()
                .with_name(tn("spread"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(2)
                        .with_records(Some(kafka_batch(b"first"))),
                ]),
        ]),
    );
    response_body(&broker, &produce).await;
    response_body(&broker, &produce).await;

    let list = encode_request(
        ApiKey::ListOffsets,
        7,
        3,
        &ListOffsetsRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(tn("spread"))
                    .with_partitions(
                        [0, 2, 3]
                            .into_iter()
                            .map(|index| {
                                ListOffsetsPartition::default()
                                    .with_partition_index(index)
                                    .with_timestamp(-1)
                            })
                            .collect(),
                    ),
            ]),
    );
    let mut buf = response_body(&broker, &list).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let listed = ListOffsetsResponse::decode(&mut buf, 7).unwrap();
    let latest: Vec<(i32, i16, i64)> = listed.topics[0]
        .partitions
        .iter()
        .map(|p| (p.partition_index, p.error_code, p.offset))
        .collect();
    assert_eq!(latest, vec![(0, 0, 0), (2, 0, 2), (3, 3, -1)]);

    let commit = encode_request(
        ApiKey::OffsetCommit,
        7,
        4,
        &OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("spread-readers")))
            .with_generation_id_or_member_epoch(-1)
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(tn("spread"))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(2)
                            .with_committed_offset(1),
                    ]),
            ]),
    );
    let mut buf = response_body(&broker, &commit).await;
    ResponseHeader::decode(&mut buf, OffsetCommitResponse::header_version(7)).unwrap();
    let committed = OffsetCommitResponse::decode(&mut buf, 7).unwrap();
    assert_eq!(committed.topics[0].partitions[0].error_code, 0);

    let fetch = encode_request(
        ApiKey::OffsetFetch,
        7,
        5,
        &OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("spread-readers")))
            .with_topics(Some(vec![
                OffsetFetchRequestTopic::default()
                    .with_name(tn("spread"))
                    .with_partition_indexes(vec![0, 2]),
            ])),
    );
    let mut buf = response_body(&broker, &fetch).await;
    ResponseHeader::decode(&mut buf, OffsetFetchResponse::header_version(7)).unwrap();
    let fetched = OffsetFetchResponse::decode(&mut buf, 7).unwrap();
    let positions: Vec<(i32, i64)> = fetched.topics[0]
        .partitions
        .iter()
        .map(|p| (p.partition_index, p.committed_offset))
        .collect();
    assert_eq!(positions, vec![(0, -1), (2, 1)]);
}

async fn describe_configs(
    broker: &BrokerContext,
    resource_type: i8,
    name: &str,
    keys: Option<&[&str]>,
) -> kafka_protocol::messages::describe_configs_response::DescribeConfigsResult {
    use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
    use kafka_protocol::messages::{DescribeConfigsRequest, DescribeConfigsResponse};

    let request = DescribeConfigsRequest::default().with_resources(vec![
        DescribeConfigsResource::default()
            .with_resource_type(resource_type)
            .with_resource_name(StrBytes::from(name.to_owned()))
            .with_configuration_keys(keys.map(|keys| {
                keys.iter()
                    .map(|key| StrBytes::from((*key).to_owned()))
                    .collect()
            })),
    ]);
    let response: DescribeConfigsResponse =
        call(broker, ApiKey::DescribeConfigs, 4, &request).await;
    response.results.into_iter().next().unwrap()
}

async fn alter_config(
    broker: &BrokerContext,
    topic: &str,
    name: &str,
    operation: i8,
    value: Option<&str>,
    validate_only: bool,
) -> i16 {
    use kafka_protocol::messages::incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig,
    };
    use kafka_protocol::messages::{
        IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse,
    };

    let request = IncrementalAlterConfigsRequest::default()
        .with_validate_only(validate_only)
        .with_resources(vec![
            AlterConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from(topic.to_owned()))
                .with_configs(vec![
                    AlterableConfig::default()
                        .with_name(StrBytes::from(name.to_owned()))
                        .with_config_operation(operation)
                        .with_value(value.map(|value| StrBytes::from(value.to_owned()))),
                ]),
        ]);
    let response: IncrementalAlterConfigsResponse =
        call(broker, ApiKey::IncrementalAlterConfigs, 1, &request).await;
    response.responses[0].error_code
}

#[tokio::test]
async fn topic_configs_describe_and_change_the_schema_binding() {
    let registry = picomq_schema::Registry::new(object_store::memory::InMemory::new());
    registry
        .put(
            "person",
            picomq_schema::SchemaFormat::Json,
            Bytes::from_static(br#"{"type": "object"}"#),
        )
        .await
        .unwrap();
    let broker = broker_with(Some(Arc::new(registry))).await;
    broker
        .topics
        .create(picomq_server::CreateCommand::new(
            "/orders",
            "application/octet-stream",
        ))
        .await
        .unwrap();
    let schema_of = || async {
        broker
            .topics
            .get("/orders")
            .await
            .unwrap()
            .unwrap()
            .schema_name
    };

    let described = describe_configs(&broker, 2, "orders", None).await;
    assert_eq!(described.error_code, 0);
    let names: Vec<_> = described.configs.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["cleanup.policy", "retention.ms", "retention.bytes"]);
    let read_only: Vec<_> = described.configs.iter().map(|c| c.read_only).collect();
    assert_eq!(read_only, [true, false, false]);

    assert_eq!(
        alter_config(&broker, "orders", "pico.schema", 0, Some("person"), true).await,
        0
    );
    assert_eq!(schema_of().await, None);

    assert_eq!(
        alter_config(&broker, "orders", "pico.schema", 0, Some("person"), false).await,
        0
    );
    assert_eq!(schema_of().await.as_deref(), Some("person"));

    let described = describe_configs(&broker, 2, "orders", Some(&["pico.schema"])).await;
    assert_eq!(described.configs.len(), 1);
    let schema = &described.configs[0];
    assert_eq!(schema.value.as_deref(), Some("person"));
    assert_eq!(schema.config_source, 1);
    assert!(!schema.read_only);

    assert_eq!(
        alter_config(&broker, "orders", "segment.bytes", 0, Some("1000"), false).await,
        40
    );
    assert_eq!(
        alter_config(
            &broker,
            "orders",
            "pico.schema.validate",
            0,
            Some("maybe"),
            false
        )
        .await,
        40
    );
    assert_eq!(
        alter_config(&broker, "missing", "pico.schema", 0, Some("person"), false).await,
        3
    );

    assert_eq!(
        alter_config(&broker, "orders", "pico.schema", 1, None, false).await,
        0
    );
    assert_eq!(schema_of().await, None);

    assert_eq!(
        describe_configs(&broker, 2, "missing", None)
            .await
            .error_code,
        3
    );
    let own = describe_configs(&broker, 4, "1", None).await;
    assert_eq!(own.error_code, 0);
    assert!(own.configs.is_empty());
    assert_eq!(describe_configs(&broker, 4, "2", None).await.error_code, 42);
}

#[tokio::test]
async fn delete_records_checks_offsets_and_times_out_before_upload() {
    use kafka_protocol::messages::delete_records_request::{
        DeleteRecordsPartition, DeleteRecordsTopic,
    };
    use kafka_protocol::messages::{DeleteRecordsRequest, DeleteRecordsResponse};

    let broker = test_broker().await;
    broker
        .topics
        .create(picomq_server::CreateCommand::new(
            "/logs",
            "application/octet-stream",
        ))
        .await
        .unwrap();

    let delete = |topic: &str, offset: i64| {
        DeleteRecordsRequest::default()
            .with_timeout_ms(200)
            .with_topics(vec![
                DeleteRecordsTopic::default()
                    .with_name(tn(topic))
                    .with_partitions(vec![
                        DeleteRecordsPartition::default()
                            .with_partition_index(0)
                            .with_offset(offset),
                    ]),
            ])
    };
    let result = |response: DeleteRecordsResponse| {
        let partition = &response.topics[0].partitions[0];
        (partition.error_code, partition.low_watermark)
    };

    let response = call(&broker, ApiKey::DeleteRecords, 2, &delete("logs", -1)).await;
    assert_eq!(result(response), (0, 0));

    let response = call(&broker, ApiKey::DeleteRecords, 2, &delete("logs", 5)).await;
    assert_eq!(result(response), (1, -1));

    let response = call(&broker, ApiKey::DeleteRecords, 2, &delete("missing", 0)).await;
    assert_eq!(result(response).0, 3);

    let produce = encode_request(
        ApiKey::Produce,
        10,
        2,
        &ProduceRequest::default().with_acks(1).with_topic_data(vec![
            TopicProduceData::default()
                .with_name(tn("logs"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(kafka_batch(b"one"))),
                ]),
        ]),
    );
    response_body(&broker, &produce).await;

    let response = call(&broker, ApiKey::DeleteRecords, 2, &delete("logs", -1)).await;
    assert_eq!(result(response), (7, -1));
}

#[tokio::test]
async fn offset_delete_and_delete_groups_respect_members() {
    use kafka_protocol::messages::offset_delete_request::{
        OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
    };
    use kafka_protocol::messages::{
        ConsumerProtocolSubscription, DeleteGroupsRequest, DeleteGroupsResponse, GroupId,
        OffsetDeleteRequest, OffsetDeleteResponse,
    };
    use picomq_server::{
        CommittedOffset, CreateCommand, JoinInput, JoinProtocol, Membership, OffsetCommit,
        Partition,
    };

    let broker = test_broker().await;
    let mut committed = Vec::new();
    for name in ["/events", "/other"] {
        let topic = broker
            .topics
            .create(CreateCommand::new(name, "application/octet-stream"))
            .await
            .unwrap()
            .meta
            .topic;
        committed.push(OffsetCommit {
            partition: Partition::new(topic.id, 0),
            value: CommittedOffset {
                position: 3,
                leader_epoch: None,
                metadata: None,
            },
        });
    }
    broker
        .groups
        .commit_offsets("workers", None, &committed)
        .await
        .unwrap();

    let mut subscription = BytesMut::new();
    subscription.extend_from_slice(&0i16.to_be_bytes());
    ConsumerProtocolSubscription::default()
        .with_topics(vec![StrBytes::from_static_str("events")])
        .encode(&mut subscription, 0)
        .unwrap();
    let member = broker
        .groups
        .join(JoinInput {
            group_id: "workers".to_owned(),
            member_id: String::new(),
            instance_id: None,
            client_id: "test".to_owned(),
            membership: Membership::Client {
                protocol_type: "consumer".to_owned(),
                protocols: vec![JoinProtocol {
                    name: "range".to_owned(),
                    metadata: subscription.freeze(),
                }],
            },
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 2_000,
            require_known_member_id: false,
        })
        .await;
    assert!(member.result.is_ok());

    let partition = |index| OffsetDeleteRequestPartition::default().with_partition_index(index);
    let request = OffsetDeleteRequest::default()
        .with_group_id(GroupId(StrBytes::from_static_str("workers")))
        .with_topics(vec![
            OffsetDeleteRequestTopic::default()
                .with_name(tn("events"))
                .with_partitions(vec![partition(0)]),
            OffsetDeleteRequestTopic::default()
                .with_name(tn("other"))
                .with_partitions(vec![partition(0), partition(4)]),
            OffsetDeleteRequestTopic::default()
                .with_name(tn("missing"))
                .with_partitions(vec![partition(0)]),
        ]);
    let deleted: OffsetDeleteResponse = call(&broker, ApiKey::OffsetDelete, 0, &request).await;
    assert_eq!(deleted.error_code, 0);
    let codes: Vec<Vec<i16>> = deleted
        .topics
        .iter()
        .map(|topic| topic.partitions.iter().map(|p| p.error_code).collect())
        .collect();
    assert_eq!(codes, [vec![86], vec![0, 3], vec![3]]);

    let remaining = broker.groups.fetch_offsets("workers", None).await.unwrap();
    assert_eq!(
        remaining.keys().copied().collect::<Vec<_>>(),
        [committed[0].partition]
    );

    let delete = |groups: &[&'static str]| {
        DeleteGroupsRequest::default().with_groups_names(
            groups
                .iter()
                .map(|group| GroupId(StrBytes::from_static_str(group)))
                .collect(),
        )
    };
    let response: DeleteGroupsResponse = call(
        &broker,
        ApiKey::DeleteGroups,
        2,
        &delete(&["workers", "ghost"]),
    )
    .await;
    let codes: Vec<i16> = response.results.iter().map(|r| r.error_code).collect();
    assert_eq!(codes, [68, 69]);

    broker
        .groups
        .leave("workers", &[(member.member_id.clone(), None)])
        .await;
    let response: DeleteGroupsResponse =
        call(&broker, ApiKey::DeleteGroups, 2, &delete(&["workers"])).await;
    assert_eq!(response.results[0].error_code, 0);
    assert!(
        broker
            .groups
            .fetch_offsets("workers", None)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn describe_cluster_lists_this_node() {
    use kafka_protocol::messages::{DescribeClusterRequest, DescribeClusterResponse};

    let broker = test_broker().await;
    let response: DescribeClusterResponse = call(
        &broker,
        ApiKey::DescribeCluster,
        2,
        &DescribeClusterRequest::default().with_include_fenced_brokers(true),
    )
    .await;

    assert_eq!(response.error_code, 0);
    assert_eq!(response.cluster_id.as_str(), "picomq");
    assert_eq!(response.controller_id.0, 1);
    assert_eq!(response.brokers.len(), 1);
    let node = &response.brokers[0];
    assert_eq!(node.broker_id.0, 1);
    assert_eq!(node.host.as_str(), "127.0.0.1");
    assert_eq!(node.port, 19092);
    assert!(!node.is_fenced);
}

#[tokio::test]
async fn leader_epochs_are_reported_and_fence_stale_requests() {
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    use kafka_protocol::messages::offset_for_leader_epoch_request::{
        OffsetForLeaderPartition, OffsetForLeaderTopic,
    };
    use kafka_protocol::messages::{
        FetchResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataResponse,
        OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, ProduceResponse,
    };

    let broker = test_broker().await;
    broker
        .topics
        .create(picomq_server::CreateCommand::new(
            "/events",
            "application/octet-stream",
        ))
        .await
        .unwrap();
    let produce = ProduceRequest::default().with_acks(1).with_topic_data(vec![
        TopicProduceData::default()
            .with_name(tn("events"))
            .with_partition_data(vec![
                PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(kafka_batch(b"one"))),
            ]),
    ]);
    let produced: ProduceResponse = call(&broker, ApiKey::Produce, 10, &produce).await;
    assert_eq!(produced.responses[0].partition_responses[0].error_code, 0);

    let topic = broker.topics.get("/events").await.unwrap().unwrap();
    let epoch = broker.topics.watermarks(&topic, 0).await.unwrap().epoch as i32;

    let metadata: MetadataResponse = call(
        &broker,
        ApiKey::Metadata,
        12,
        &MetadataRequest::default().with_topics(Some(vec![
            MetadataRequestTopic::default().with_name(Some(tn("events"))),
        ])),
    )
    .await;
    assert_eq!(metadata.topics[0].partitions[0].leader_epoch, epoch);

    let fetch = |current: i32| {
        FetchRequest::default()
            .with_max_wait_ms(0)
            .with_max_bytes(1 << 20)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(tn("events"))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_current_leader_epoch(current)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(1 << 20),
                    ]),
            ])
    };
    for (current, code) in [(epoch - 1, 74), (epoch + 1, 75), (-1, 0)] {
        let fetched: FetchResponse = call(&broker, ApiKey::Fetch, 12, &fetch(current)).await;
        assert_eq!(
            fetched.responses[0].partitions[0].error_code, code,
            "{current}"
        );
    }
    let fetched: FetchResponse = call(&broker, ApiKey::Fetch, 12, &fetch(epoch)).await;
    let records = fetched.responses[0].partitions[0].records.clone().unwrap();
    assert_eq!(&records[12..16], &epoch.to_be_bytes());

    let list = |timestamp: i64, current: i32| {
        ListOffsetsRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_topics(vec![
                ListOffsetsTopic::default()
                    .with_name(tn("events"))
                    .with_partitions(vec![
                        ListOffsetsPartition::default()
                            .with_partition_index(0)
                            .with_current_leader_epoch(current)
                            .with_timestamp(timestamp),
                    ]),
            ])
    };
    for (timestamp, current, expected) in [
        (-2, -1, (0, 0, epoch)),
        (-1, epoch, (0, 1, epoch)),
        (0, -1, (0, 0, epoch)),
        (-1, epoch - 1, (74, -1, -1)),
    ] {
        let listed: ListOffsetsResponse =
            call(&broker, ApiKey::ListOffsets, 7, &list(timestamp, current)).await;
        let partition = &listed.topics[0].partitions[0];
        assert_eq!(
            (
                partition.error_code,
                partition.offset,
                partition.leader_epoch
            ),
            expected,
            "{timestamp} at {current}"
        );
    }

    let end = |current: i32, wanted: i32| {
        OffsetForLeaderEpochRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_topics(vec![
                OffsetForLeaderTopic::default()
                    .with_topic(tn("events"))
                    .with_partitions(vec![
                        OffsetForLeaderPartition::default()
                            .with_partition(0)
                            .with_current_leader_epoch(current)
                            .with_leader_epoch(wanted),
                    ]),
            ])
    };
    for (current, wanted, expected) in [
        (epoch, epoch, (0, epoch, 1)),
        (-1, -1, (0, -1, -1)),
        (epoch + 1, epoch, (75, -1, -1)),
    ] {
        let ended: OffsetForLeaderEpochResponse = call(
            &broker,
            ApiKey::OffsetForLeaderEpoch,
            4,
            &end(current, wanted),
        )
        .await;
        let partition = &ended.topics[0].partitions[0];
        assert_eq!(
            (
                partition.error_code,
                partition.leader_epoch,
                partition.end_offset
            ),
            expected,
            "{wanted} at {current}"
        );
    }
}

#[tokio::test]
async fn create_topics_refuses_compaction() {
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
    use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse};

    let broker = broker_with(None).await;
    let create = |name: &'static str, policy: &'static str| {
        encode_request(
            ApiKey::CreateTopics,
            5,
            1,
            &CreateTopicsRequest::default()
                .with_timeout_ms(5_000)
                .with_topics(vec![
                    CreatableTopic::default()
                        .with_name(tn(name))
                        .with_num_partitions(1)
                        .with_replication_factor(1)
                        .with_configs(vec![
                            CreatableTopicConfig::default()
                                .with_name(StrBytes::from_static_str("cleanup.policy"))
                                .with_value(Some(StrBytes::from_static_str(policy))),
                        ]),
                ]),
        )
    };

    let mut buf = response_body(&broker, &create("state", "compact")).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let refused = CreateTopicsResponse::decode(&mut buf, 5).unwrap();
    assert_eq!(refused.topics[0].error_code, 40);
    assert!(broker.topics.get("/state").await.unwrap().is_none());

    let mut buf = response_body(&broker, &create("events", "delete")).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let created = CreateTopicsResponse::decode(&mut buf, 5).unwrap();
    assert_eq!(created.topics[0].error_code, 0);
}

#[tokio::test]
async fn topic_configs_set_describe_and_remove_retention() {
    let broker = test_broker().await;
    broker
        .topics
        .create(picomq_server::CreateCommand::new(
            "/events",
            "application/octet-stream",
        ))
        .await
        .unwrap();
    let retention = || async {
        broker
            .topics
            .get("/events")
            .await
            .unwrap()
            .unwrap()
            .retention
    };

    assert_eq!(
        alter_config(&broker, "events", "retention.ms", 0, Some("60000"), false).await,
        0
    );
    assert_eq!(
        alter_config(&broker, "events", "retention.bytes", 0, Some("4096"), false).await,
        0
    );
    assert_eq!(retention().await.ms, Some(60_000));
    assert_eq!(retention().await.bytes, Some(4096));

    let described = describe_configs(&broker, 2, "events", Some(&["retention.ms"])).await;
    let config = &described.configs[0];
    assert_eq!(config.value.as_deref(), Some("60000"));
    assert_eq!(config.config_source, 1);
    assert!(!config.read_only);

    assert_eq!(
        alter_config(&broker, "events", "retention.ms", 0, Some("0"), false).await,
        40
    );
    assert_eq!(
        alter_config(&broker, "events", "retention.ms", 1, None, false).await,
        0
    );
    assert_eq!(
        alter_config(&broker, "events", "retention.bytes", 0, Some("-1"), false).await,
        0
    );
    assert_eq!(retention().await, picomq_server::Retention::default());

    let described = describe_configs(&broker, 2, "events", Some(&["retention.ms"])).await;
    assert_eq!(described.configs[0].value.as_deref(), Some("-1"));
    assert_eq!(described.configs[0].config_source, 5);
}

#[tokio::test]
async fn create_topics_takes_retention() {
    use kafka_protocol::messages::create_topics_request::{CreatableTopic, CreatableTopicConfig};
    use kafka_protocol::messages::{CreateTopicsRequest, CreateTopicsResponse};

    let broker = test_broker().await;
    let create = |name: &'static str, retention: &'static str| {
        encode_request(
            ApiKey::CreateTopics,
            5,
            1,
            &CreateTopicsRequest::default()
                .with_timeout_ms(5_000)
                .with_topics(vec![
                    CreatableTopic::default()
                        .with_name(tn(name))
                        .with_num_partitions(1)
                        .with_replication_factor(1)
                        .with_configs(vec![
                            CreatableTopicConfig::default()
                                .with_name(StrBytes::from_static_str("retention.ms"))
                                .with_value(Some(StrBytes::from_static_str(retention))),
                        ]),
                ]),
        )
    };

    let mut buf = response_body(&broker, &create("kept", "3600000")).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    assert_eq!(
        CreateTopicsResponse::decode(&mut buf, 5).unwrap().topics[0].error_code,
        0
    );
    let topic = broker.topics.get("/kept").await.unwrap().unwrap();
    assert_eq!(topic.retention.ms, Some(3_600_000));

    let mut buf = response_body(&broker, &create("bad", "0")).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    assert_eq!(
        CreateTopicsResponse::decode(&mut buf, 5).unwrap().topics[0].error_code,
        40
    );
}
