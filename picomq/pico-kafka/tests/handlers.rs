use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, FetchRequest, MetadataRequest, ProduceRequest,
    RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{encode_request_header_into_buffer, Decodable, Encodable, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};
use picomq_kafka::{dispatch, BrokerContext, HandlerOutcome, KafkaListener, ListenerConfig};
use picomq_metadata::{CommandSink, LocalSink};
use picomq_server::{NodeConfig, PicoNode};
use s3stream::{MemoryObjectStorage, ObjectStorageTrait};
use tokio::net::TcpListener;

fn tn(name: &str) -> TopicName {
    TopicName(StrBytes::from(name.to_owned()))
}

async fn test_broker() -> BrokerContext {
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
        None,
    )
    .await
    .unwrap();
    BrokerContext::new(
        node.config().node_id,
        node.config().cluster_id.clone(),
        node.service(),
        node.ownership(),
        node.views(),
        node.metadata().clone(),
    )
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
        timestamp: 1,
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
    assert!(response
        .api_keys
        .iter()
        .any(|api| api.api_key == ApiKey::Metadata as i16));
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
                MetadataRequestTopic::default().with_name(Some(tn("events")))
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
                    .with_partition_data(vec![PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(kafka_batch(payload)))]),
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
            .with_topics(vec![FetchTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(0)])]),
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
            .with_topics(vec![FetchTopic::default()
                .with_topic_id(uuid::Uuid::new_v4())
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(0)])]),
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
            .with_topics(vec![FetchTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![FetchPartition::default()
                    .with_partition(0)
                    .with_fetch_offset(50)])]),
    );
    let mut buf = response_body(&broker, &oor_req).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let oor = FetchResponse::decode(&mut buf, 13).unwrap();
    assert_eq!(oor.responses[0].partitions[0].error_code, 1);
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
                MetadataRequestTopic::default().with_name(Some(tn("fire")))
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
        &ProduceRequest::default()
            .with_acks(0)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(tn("fire"))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(kafka_batch(b"fire-and-forget")))])]),
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

    // hex("probe") under the reserved namespace: no stream may exist.
    let stream = "/_sys/groups/70726f6265";
    assert!(broker
        .service
        .lookup_stream_id(stream)
        .await
        .unwrap()
        .is_none());

    let fetch = encode_request(
        ApiKey::OffsetFetch,
        7,
        31,
        &OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("probe")))
            .with_topics(Some(vec![OffsetFetchRequestTopic::default()
                .with_name(tn("events"))
                .with_partition_indexes(vec![0])])),
    );
    let mut buf = response_body(&broker, &fetch).await;
    ResponseHeader::decode(&mut buf, OffsetFetchResponse::header_version(7)).unwrap();
    let fetched = OffsetFetchResponse::decode(&mut buf, 7).unwrap();
    assert_eq!(fetched.error_code, 0);
    assert_eq!(fetched.topics[0].partitions[0].committed_offset, -1);
    assert!(broker
        .service
        .lookup_stream_id(stream)
        .await
        .unwrap()
        .is_none());
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
                .with_protocols(vec![JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str("range"))
                    .with_metadata(Bytes::from_static(b"subscription"))]),
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
                vec![SyncGroupRequestAssignment::default()
                    .with_member_id(member1.clone())
                    .with_assignment(Bytes::from_static(b"all"))],
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
                .with_protocols(vec![JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str("range"))
                    .with_metadata(Bytes::from_static(b"subscription"))]),
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
                .with_protocols(vec![JoinGroupRequestProtocol::default()
                    .with_name(StrBytes::from_static_str("range"))
                    .with_metadata(Bytes::from_static(b"subscription"))]),
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
            .with_assignments(vec![SyncGroupRequestAssignment::default()
                .with_member_id(joined.member_id.clone())
                .with_assignment(assignment.clone())]),
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
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(tn("events"))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(42)
                    .with_committed_leader_epoch(3)
                    .with_committed_metadata(Some(StrBytes::from_static_str(
                        "checkpoint",
                    )))])]),
    );
    let mut buf = response_body(&broker, &commit).await;
    ResponseHeader::decode(&mut buf, OffsetCommitResponse::header_version(7)).unwrap();
    let committed = OffsetCommitResponse::decode(&mut buf, 7).unwrap();
    assert_eq!(committed.topics[0].partitions[0].error_code, 0);

    let restarted = BrokerContext::new(
        broker.node_id,
        broker.cluster_id.clone(),
        broker.service.clone(),
        broker.ownership.clone(),
        broker.views.clone(),
        broker.metadata.clone(),
    );
    let fetch = encode_request(
        ApiKey::OffsetFetch,
        7,
        26,
        &OffsetFetchRequest::default()
            .with_group_id(group_id.clone())
            .with_topics(Some(vec![OffsetFetchRequestTopic::default()
                .with_name(tn("events"))
                .with_partition_indexes(vec![0])]))
            .with_require_stable(true),
    );
    let mut buf = response_body(&restarted, &fetch).await;
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

    let (sink, views) = LocalSink::new();
    let sink: Arc<dyn CommandSink> = Arc::new(sink);
    let object_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(20));
    let wal_storage: Arc<dyn ObjectStorageTrait> = Arc::new(MemoryObjectStorage::new(21));
    let node = PicoNode::start(
        NodeConfig {
            node_id: 1,
            node_epoch: 1,
            http_address: "http://127.0.0.1:4021".into(),
            protocol_addresses: std::collections::BTreeMap::from([(
                picomq_kafka::PROTOCOL_NAME.to_owned(),
                "127.0.0.1:19093".to_owned(),
            )]),
            ..Default::default()
        },
        sink,
        views,
        object_storage,
        wal_storage,
        Some(Arc::new(registry)),
    )
    .await
    .unwrap();
    let broker = BrokerContext::new(
        node.config().node_id,
        node.config().cluster_id.clone(),
        node.service(),
        node.ownership(),
        node.views(),
        node.metadata().clone(),
    );

    let create_req = encode_request(
        ApiKey::CreateTopics,
        5,
        1,
        &CreateTopicsRequest::default()
            .with_timeout_ms(5_000)
            .with_topics(vec![CreatableTopic::default()
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
                ])]),
    );
    let mut buf = response_body(&broker, &create_req).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let created = CreateTopicsResponse::decode(&mut buf, 5).unwrap();
    assert_eq!(created.topics[0].error_code, 0);
    assert_eq!(
        broker
            .service
            .head("/orders")
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
        &ProduceRequest::default()
            .with_acks(1)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(tn("orders"))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(kafka_batch(br#"{"name":1}"#)))])]),
    );
    let mut buf = response_body(&broker, &bad).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let produced = ProduceResponse::decode(&mut buf, 10).unwrap();
    assert_eq!(produced.responses[0].partition_responses[0].error_code, 87);

    let ok = encode_request(
        ApiKey::Produce,
        10,
        3,
        &ProduceRequest::default()
            .with_acks(1)
            .with_topic_data(vec![TopicProduceData::default()
                .with_name(tn("orders"))
                .with_partition_data(vec![PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(kafka_batch(br#"{"name":"alice"}"#)))])]),
    );
    let mut buf = response_body(&broker, &ok).await;
    ResponseHeader::decode(&mut buf, 1).unwrap();
    let produced = ProduceResponse::decode(&mut buf, 10).unwrap();
    assert_eq!(produced.responses[0].partition_responses[0].error_code, 0);
}
