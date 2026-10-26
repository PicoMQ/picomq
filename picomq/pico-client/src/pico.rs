use async_trait::async_trait;
use bytes::Bytes;
use picomq_protocol::WireRequest;
use picomq_protocol::config::{ConfigUpdate, TopicConfig};
use picomq_protocol::groups::{
    AssignmentRequest, AssignmentResponse, CommitRequest, DeleteGroupRequest, DescribeRequest,
    FetchOffsetsRequest, GroupDescription, GroupListing, HeartbeatRequest, JoinRequest,
    JoinResponse, LeaveRequest, ListGroupsRequest, MemberFence, Offsets, OffsetsResponse,
};
use picomq_protocol::pico::{
    AppendRequest, AppendResponse, CloseRequest, CloseResponse, CreateRequest, CreateResponse,
    DEFAULT_CT, DeleteRequest, DeleteResponse, HeadRequest, HeadResponse, LIVE_LONG_POLL,
    ListRequest, Listing, ReadRequest, ReadResponse, SEQ_BEGINNING, TrimRequest, TrimResponse,
    decode_error,
};
use picomq_protocol::record::PicoRecord;
use picomq_protocol::schemas::Schema;
use reqwest::Response;

use crate::ClientConfig;
use crate::error::{ClientError, Result};
use crate::retry::RetryPolicy;
use crate::transport::{Transport, invalid_response};
use crate::types::{
    AppendAck, CreateOptions, Live, Protocol, ReadLimits, ReadPage, Record, StreamApi, StreamInfo,
    StreamListing,
};

pub use picomq_protocol::pico::Producer as ProducerRef;

#[derive(Debug, Clone)]
pub struct ProducerAck {
    pub applied: bool,
    pub duplicate: bool,
    pub ack: AppendAck,
}

#[derive(Clone)]
pub struct PicoClient {
    transport: Transport,
    retry: RetryPolicy,
}

impl PicoClient {
    pub fn new(endpoint: &str) -> Result<Self> {
        Self::with_config(endpoint, &ClientConfig::default())
    }

    pub fn with_config(endpoint: &str, config: &ClientConfig) -> Result<Self> {
        Ok(Self {
            transport: Transport::new(endpoint, crate::http_client(config)?),
            retry: config.retry.clone(),
        })
    }

    pub async fn append_as(
        &self,
        name: &str,
        partition: Option<u32>,
        records: &[Bytes],
        producer: &ProducerRef<'_>,
    ) -> Result<ProducerAck> {
        let records_wire = wire_records(records);
        let mut request = AppendRequest::new(name, &records_wire);
        request.partition = partition;
        request.producer = Some(*producer);
        let response = self.call(request.encode()).await?;
        let ack = AppendResponse::decode(response.headers());
        let next = seq_string(ack.next_seq);
        let applied = ack.start_seq.is_some();
        Ok(ProducerAck {
            applied,
            duplicate: !applied && !records.is_empty(),
            ack: AppendAck {
                start: ack
                    .start_seq
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| next.clone()),
                next,
                timestamp: ack.timestamp,
            },
        })
    }

    pub async fn trim(&self, name: &str, partition: Option<u32>, seq: u64) -> Result<String> {
        let request = TrimRequest {
            stream: name,
            partition,
            seq,
        };
        let response = self.call(request.encode()).await?;
        Ok(seq_string(
            TrimResponse::decode(response.headers()).start_seq,
        ))
    }

    pub async fn join_group(&self, request: &JoinRequest) -> Result<JoinResponse> {
        let response = self.call(request.encode()).await?;
        JoinResponse::decode(&response.bytes().await?).map_err(invalid_response)
    }

    pub async fn group_assignment(
        &self,
        group: &str,
        fence: &MemberFence,
    ) -> Result<AssignmentResponse> {
        let request = AssignmentRequest {
            group: group.to_owned(),
            fence: fence.clone(),
        };
        let response = self.call(request.encode()).await?;
        AssignmentResponse::decode(&response.bytes().await?).map_err(invalid_response)
    }

    pub async fn heartbeat(&self, group: &str, fence: &MemberFence) -> Result<()> {
        let request = HeartbeatRequest {
            group: group.to_owned(),
            fence: fence.clone(),
        };
        self.call(request.encode()).await.map(drop)
    }

    pub async fn leave_group(
        &self,
        group: &str,
        member_id: &str,
        instance_id: Option<&str>,
    ) -> Result<()> {
        let request = LeaveRequest {
            group: group.to_owned(),
            member_id: member_id.to_owned(),
            instance_id: instance_id.map(str::to_owned),
        };
        self.call(request.encode()).await.map(drop)
    }

    pub async fn commit_offsets(
        &self,
        group: &str,
        fence: Option<&MemberFence>,
        offsets: &Offsets,
    ) -> Result<()> {
        let request = CommitRequest {
            group: group.to_owned(),
            fence: fence.cloned(),
            offsets: offsets.clone(),
        };
        self.call(request.encode()).await.map(drop)
    }

    pub async fn fetch_offsets(&self, group: &str, streams: &[String]) -> Result<Offsets> {
        let request = FetchOffsetsRequest {
            group: group.to_owned(),
            streams: streams.to_vec(),
        };
        let response = self.call(request.encode()).await?;
        OffsetsResponse::decode(&response.bytes().await?)
            .map(|decoded| decoded.offsets)
            .map_err(invalid_response)
    }

    pub async fn describe_group(&self, group: &str) -> Result<GroupDescription> {
        let request = DescribeRequest {
            group: group.to_owned(),
        };
        let response = self.retry.run(|| self.call(request.encode())).await?;
        GroupDescription::decode(&response.bytes().await?).map_err(invalid_response)
    }

    pub async fn delete_group(&self, group: &str) -> Result<()> {
        let request = DeleteGroupRequest {
            group: group.to_owned(),
        };
        self.call(request.encode()).await.map(drop)
    }

    pub async fn list_groups(
        &self,
        prefix: &str,
        limit: u64,
        cursor: Option<&str>,
    ) -> Result<GroupListing> {
        let request = ListGroupsRequest {
            prefix,
            limit,
            cursor,
        };
        let response = self.retry.run(|| self.call(request.encode())).await?;
        GroupListing::decode(&response.bytes().await?).map_err(invalid_response)
    }

    async fn call(&self, wire: WireRequest) -> Result<Response> {
        self.transport.call(wire, decode_error).await
    }

    async fn head_once(&self, name: &str, partition: Option<u32>) -> Result<Option<StreamInfo>> {
        let request = HeadRequest {
            stream: name,
            partition,
        };
        let response = self.call(request.encode()).await?;
        let Some(head) = HeadResponse::decode(response.status().as_u16(), response.headers())
        else {
            return Ok(None);
        };
        Ok(Some(StreamInfo {
            name: name.to_owned(),
            content_type: head.content_type,
            partitions: head.partitions,
            start: head.start_seq.map(|seq| seq.to_string()),
            next: head.next_seq.map(|seq| seq.to_string()),
            closed: head.closed,
            ttl_seconds: head.ttl_seconds,
            expires_at: head.expires_at,
            retention_ms: head.retention_ms,
            retention_bytes: head.retention_bytes,
            schema: head.schema,
            kafka_topic: head.kafka_topic,
        }))
    }

    async fn read_once(
        &self,
        name: &str,
        partition: Option<u32>,
        from: &str,
        live: Live,
        limits: ReadLimits,
    ) -> Result<ReadPage> {
        let mut request = ReadRequest::new(name, from);
        request.partition = partition;
        request.count = limits.count;
        request.bytes = limits.bytes;
        request.live = (live == Live::LongPoll).then_some(LIVE_LONG_POLL);
        let response = self.call(request.encode()).await?;

        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await?;
        let read = ReadResponse::decode(status, &headers, &body).map_err(invalid_response)?;

        Ok(ReadPage {
            up_to_date: read.up_to_date || (read.no_content && live == Live::LongPoll),
            records: read
                .records
                .into_iter()
                .map(|record| Record {
                    position: record.seq.to_string(),
                    timestamp: Some(record.record.timestamp),
                    key: record.record.key,
                    headers: record.record.headers,
                    body: record.record.body,
                })
                .collect(),
            next: read
                .next_seq
                .map(|v| v.to_string())
                .unwrap_or_else(|| from.to_owned()),
            closed: read.closed,
        })
    }

    async fn list_once(
        &self,
        prefix: &str,
        limit: u64,
        cursor: Option<&str>,
    ) -> Result<StreamListing> {
        let request = ListRequest {
            prefix,
            limit,
            cursor,
        };
        let response = self.call(request.encode()).await?;
        let body = response.bytes().await?;
        let listing = Listing::decode(&body).map_err(invalid_response)?;

        Ok(StreamListing {
            streams: listing
                .streams
                .into_iter()
                .map(|entry| StreamInfo {
                    name: entry.name,
                    content_type: entry.content_type,
                    partitions: entry.partitions,
                    start: None,
                    next: None,
                    closed: entry.closed,
                    ttl_seconds: entry.ttl_seconds,
                    expires_at: entry.expires_at,
                    retention_ms: None,
                    retention_bytes: None,
                    schema: None,
                    kafka_topic: None,
                })
                .collect(),
            next: listing.next,
        })
    }
}

#[async_trait]
impl StreamApi for PicoClient {
    fn protocol(&self) -> Protocol {
        Protocol::Pico
    }

    fn beginning(&self) -> String {
        SEQ_BEGINNING.to_owned()
    }

    fn now(&self) -> Result<String> {
        Err(ClientError::unsupported(
            "the Pico protocol has no `now` token; read from the stream's next seq",
        ))
    }

    async fn create(&self, name: &str, options: &CreateOptions) -> Result<bool> {
        let content_type = options.content_type.as_deref().unwrap_or(DEFAULT_CT);
        let request = CreateRequest {
            ttl_seconds: options.ttl_seconds,
            expires_at: options.expires_at.as_deref(),
            closed: options.closed,
            schema: options.schema.as_deref(),
            schema_validate: options.schema_validate,
            partitions: options.partitions,
            retention_ms: options.retention_ms,
            retention_bytes: options.retention_bytes,
            ..CreateRequest::new(name, content_type)
        };
        let response = self.call(request.encode()).await?;
        Ok(CreateResponse::decode(response.status().as_u16(), response.headers()).created)
    }

    async fn head(&self, name: &str, partition: Option<u32>) -> Result<Option<StreamInfo>> {
        self.retry.run(|| self.head_once(name, partition)).await
    }

    async fn append(
        &self,
        name: &str,
        partition: Option<u32>,
        records: &[Bytes],
        _content_type: &str,
    ) -> Result<AppendAck> {
        let records_wire = wire_records(records);
        let mut request = AppendRequest::new(name, &records_wire);
        request.partition = partition;
        let response = self.call(request.encode()).await?;
        let ack = AppendResponse::decode(response.headers());
        let next = seq_string(ack.next_seq);
        Ok(AppendAck {
            start: ack
                .start_seq
                .map(|v| v.to_string())
                .unwrap_or_else(|| next.clone()),
            next,
            timestamp: ack.timestamp,
        })
    }

    async fn read(
        &self,
        name: &str,
        partition: Option<u32>,
        from: &str,
        live: Live,
        limits: ReadLimits,
    ) -> Result<ReadPage> {
        self.retry
            .run(|| self.read_once(name, partition, from, live, limits))
            .await
    }

    async fn list(&self, prefix: &str, limit: u64, cursor: Option<&str>) -> Result<StreamListing> {
        self.retry
            .run(|| self.list_once(prefix, limit, cursor))
            .await
    }

    async fn close(&self, name: &str) -> Result<String> {
        let response = self.call(CloseRequest { stream: name }.encode()).await?;
        Ok(seq_string(
            CloseResponse::decode(response.headers()).next_seq,
        ))
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        let response = self.call(DeleteRequest { stream: name }.encode()).await?;
        Ok(DeleteResponse::decode(response.status().as_u16()).found)
    }
    async fn config(&self, name: &str) -> Result<Option<TopicConfig>> {
        self.retry.run(|| self.transport.config(name)).await
    }

    async fn update_config(&self, update: &ConfigUpdate) -> Result<TopicConfig> {
        self.transport.update_config(update).await
    }

    async fn put_schema(&self, name: &str, content_type: Option<&str>, body: Bytes) -> Result<()> {
        self.transport.put_schema(name, content_type, body).await
    }

    async fn get_schema(&self, name: &str) -> Result<Option<Schema>> {
        self.retry.run(|| self.transport.get_schema(name)).await
    }

    async fn delete_schema(&self, name: &str) -> Result<bool> {
        self.transport.delete_schema(name).await
    }
}

fn wire_records(records: &[Bytes]) -> Vec<PicoRecord> {
    records
        .iter()
        .map(|body| PicoRecord::new(body.clone()))
        .collect()
}

fn seq_string(seq: Option<u64>) -> String {
    seq.map(|v| v.to_string())
        .unwrap_or_else(|| SEQ_BEGINNING.to_owned())
}
