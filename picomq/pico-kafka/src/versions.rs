use kafka_protocol::messages::ApiKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupportedApi {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub advertised_min_version: i16,
}

impl SupportedApi {
    const fn new(api_key: ApiKey, min_version: i16, max_version: i16) -> Self {
        Self {
            api_key: api_key as i16,
            min_version,
            max_version,
            advertised_min_version: min_version,
        }
    }

    const fn advertised_from(mut self, version: i16) -> Self {
        self.advertised_min_version = version;
        self
    }
}

const APIS: &[SupportedApi] = &[
    SupportedApi::new(ApiKey::Produce, 3, 10).advertised_from(0),
    SupportedApi::new(ApiKey::Fetch, 4, 16),
    SupportedApi::new(ApiKey::ListOffsets, 1, 7),
    SupportedApi::new(ApiKey::Metadata, 0, 12),
    SupportedApi::new(ApiKey::CreateTopics, 2, 7),
    SupportedApi::new(ApiKey::DeleteTopics, 1, 6),
    SupportedApi::new(ApiKey::CreatePartitions, 0, 3),
    SupportedApi::new(ApiKey::DeleteRecords, 0, 2),
    SupportedApi::new(ApiKey::DescribeConfigs, 1, 4),
    SupportedApi::new(ApiKey::IncrementalAlterConfigs, 0, 1),
    SupportedApi::new(ApiKey::DescribeCluster, 0, 2),
    SupportedApi::new(ApiKey::InitProducerId, 0, 4),
    SupportedApi::new(ApiKey::OffsetCommit, 2, 8),
    SupportedApi::new(ApiKey::OffsetFetch, 1, 7),
    SupportedApi::new(ApiKey::FindCoordinator, 0, 3),
    SupportedApi::new(ApiKey::JoinGroup, 0, 7),
    SupportedApi::new(ApiKey::Heartbeat, 0, 4),
    SupportedApi::new(ApiKey::LeaveGroup, 0, 5),
    SupportedApi::new(ApiKey::SyncGroup, 0, 5),
    SupportedApi::new(ApiKey::DescribeGroups, 0, 5),
    SupportedApi::new(ApiKey::ListGroups, 0, 5),
    SupportedApi::new(ApiKey::DeleteGroups, 0, 2),
    SupportedApi::new(ApiKey::OffsetDelete, 0, 0),
    SupportedApi::new(ApiKey::ApiVersions, 0, 4),
];

pub fn supported_apis() -> &'static [SupportedApi] {
    APIS
}

pub fn lookup_versions(api_key: i16) -> Option<(i16, i16)> {
    supported_apis()
        .iter()
        .find(|api| api.api_key == api_key)
        .map(|api| (api.min_version, api.max_version))
}

pub fn is_supported(api_key: i16, api_version: i16) -> bool {
    lookup_versions(api_key).is_some_and(|(min, max)| (min..=max).contains(&api_version))
}
