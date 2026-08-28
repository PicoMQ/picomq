#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupportedApi {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

/// Advertised API ranges. librdkafka needs the floors at Produce 3, Fetch 4,
/// and ListOffsets 1 or it falls back to the v0 message format.
pub fn supported_apis() -> &'static [SupportedApi] {
    use kafka_protocol::messages::ApiKey;
    const APIS: &[SupportedApi] = &[
        SupportedApi {
            api_key: ApiKey::Produce as i16,
            min_version: 3,
            max_version: 10,
        },
        SupportedApi {
            api_key: ApiKey::Fetch as i16,
            min_version: 4,
            max_version: 16,
        },
        SupportedApi {
            api_key: ApiKey::ListOffsets as i16,
            min_version: 1,
            max_version: 7,
        },
        SupportedApi {
            api_key: ApiKey::Metadata as i16,
            min_version: 10,
            max_version: 12,
        },
        SupportedApi {
            api_key: ApiKey::CreateTopics as i16,
            min_version: 2,
            max_version: 7,
        },
        SupportedApi {
            api_key: ApiKey::DeleteTopics as i16,
            min_version: 1,
            max_version: 6,
        },
        SupportedApi {
            api_key: ApiKey::InitProducerId as i16,
            min_version: 0,
            max_version: 4,
        },
        SupportedApi {
            api_key: ApiKey::OffsetCommit as i16,
            min_version: 2,
            max_version: 8,
        },
        SupportedApi {
            api_key: ApiKey::OffsetFetch as i16,
            min_version: 1,
            max_version: 7,
        },
        SupportedApi {
            api_key: ApiKey::FindCoordinator as i16,
            min_version: 0,
            max_version: 3,
        },
        SupportedApi {
            api_key: ApiKey::JoinGroup as i16,
            min_version: 0,
            max_version: 7,
        },
        SupportedApi {
            api_key: ApiKey::Heartbeat as i16,
            min_version: 0,
            max_version: 4,
        },
        SupportedApi {
            api_key: ApiKey::LeaveGroup as i16,
            min_version: 0,
            max_version: 5,
        },
        SupportedApi {
            api_key: ApiKey::SyncGroup as i16,
            min_version: 0,
            max_version: 5,
        },
        SupportedApi {
            api_key: ApiKey::DescribeGroups as i16,
            min_version: 0,
            max_version: 5,
        },
        SupportedApi {
            api_key: ApiKey::ListGroups as i16,
            min_version: 0,
            max_version: 5,
        },
        SupportedApi {
            api_key: ApiKey::ApiVersions as i16,
            min_version: 0,
            max_version: 4,
        },
    ];
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
