use std::str::FromStr;
use std::time::Duration;

use url::Url;

use crate::error::Error;

const LEVELS: [&str; 4] = ["on", "remote_apply", "remote_write", "local"];
const SECRETS: [&str; 2] = ["password", "sslpassword"];

#[derive(Debug, Clone)]
pub struct Config {
    pub cluster_id: String,
    pub node_id: u32,
    pub epoch: u64,
    pub batch_interval: Duration,
    pub max_bytes_in_batch: u64,
    pub max_inflight: usize,
    pub max_unflushed_bytes: u64,
    pub segment_bytes: u64,
    pub segments: u32,
    pub synchronous_commit: String,
    pub allow_unsafe: bool,
    connection: String,
    redacted: String,
}

impl Config {
    pub fn accepts(uri: &str) -> bool {
        uri.starts_with("postgres://") || uri.starts_with("postgresql://")
    }

    pub fn parse(uri: &str) -> Result<Self, Error> {
        if !Self::accepts(uri) {
            return Err(Error::Uri("expected a postgres:// url".to_owned()));
        }
        let mut url = Url::parse(uri).map_err(|error| Error::Uri(error.to_string()))?;
        let mut config = Self::defaults();
        let mut passthrough = Vec::new();
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "batchInterval" => {
                    config.batch_interval = Duration::from_millis(typed(&key, &value)?)
                }
                "maxBytesInBatch" => config.max_bytes_in_batch = typed(&key, &value)?,
                "maxInflight" => config.max_inflight = typed(&key, &value)?,
                "maxUnflushedBytes" => config.max_unflushed_bytes = typed(&key, &value)?,
                "segmentBytes" => config.segment_bytes = typed(&key, &value)?,
                "segments" => config.segments = typed(&key, &value)?,
                "synchronousCommit" => config.synchronous_commit = value.into_owned(),
                "allowUnsafe" => config.allow_unsafe = typed(&key, &value)?,
                _ => passthrough.push((key.into_owned(), value.into_owned())),
            }
        }
        let mut shown = url.clone();
        if shown.password().is_some() {
            let _ = shown.set_password(Some("***"));
        }
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        if !pairs.is_empty() {
            shown.set_query(None);
            shown
                .query_pairs_mut()
                .extend_pairs(pairs.iter().map(|(key, value)| {
                    if SECRETS.contains(&key.as_str()) {
                        (key.as_str(), "***")
                    } else {
                        (key.as_str(), value.as_str())
                    }
                }));
        }
        url.set_query(None);
        if !passthrough.is_empty() {
            url.query_pairs_mut().extend_pairs(&passthrough);
        }
        config.connection = url.into();
        config.redacted = shown.into();
        config.validate()?;
        Ok(config)
    }

    pub fn identity(mut self, cluster_id: impl Into<String>, node_id: u32, epoch: u64) -> Self {
        self.cluster_id = cluster_id.into();
        self.node_id = node_id;
        self.epoch = epoch;
        self
    }

    pub fn redacted(&self) -> &str {
        &self.redacted
    }

    pub fn capacity(&self) -> u64 {
        (u64::from(self.segments) - 1) * self.segment_bytes
    }

    pub(crate) fn connection(&self) -> &str {
        &self.connection
    }

    fn defaults() -> Self {
        Self {
            cluster_id: String::new(),
            node_id: 0,
            epoch: 0,
            batch_interval: Duration::from_millis(1),
            max_bytes_in_batch: 1024 * 1024,
            max_inflight: 4,
            max_unflushed_bytes: 128 * 1024 * 1024,
            segment_bytes: 64 * 1024 * 1024,
            segments: 8,
            synchronous_commit: "on".to_owned(),
            allow_unsafe: false,
            connection: String::new(),
            redacted: String::new(),
        }
    }

    fn validate(&self) -> Result<(), Error> {
        let invalid = |reason: String| Err(Error::Uri(reason));
        if !LEVELS.contains(&self.synchronous_commit.as_str()) {
            return invalid(format!(
                "synchronousCommit must be one of {LEVELS:?}, got {:?}",
                self.synchronous_commit
            ));
        }
        if self.max_inflight == 0 || self.max_bytes_in_batch == 0 {
            return invalid("maxInflight and maxBytesInBatch must be positive".to_owned());
        }
        if self.segments < 2 {
            return invalid("segments must be at least 2".to_owned());
        }
        if self.segment_bytes < self.max_bytes_in_batch || self.segment_bytes > u64::from(u32::MAX)
        {
            return invalid(format!(
                "segmentBytes must be between maxBytesInBatch and {}",
                u32::MAX
            ));
        }
        if self.capacity() <= self.max_unflushed_bytes {
            return invalid(format!(
                "ring capacity {} must exceed maxUnflushedBytes {}",
                self.capacity(),
                self.max_unflushed_bytes
            ));
        }
        Ok(())
    }
}

fn typed<T: FromStr>(key: &str, value: &str) -> Result<T, Error> {
    value
        .parse()
        .map_err(|_| Error::Uri(format!("{key} has an invalid value {value:?}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_wal_parameters_and_keeps_the_rest() {
        let config = Config::parse(
            "postgres://pico:secret@db:5432/pico?sslmode=disable&batchInterval=5&segments=4&maxUnflushedBytes=1048576",
        )
        .unwrap();
        assert_eq!(config.batch_interval, Duration::from_millis(5));
        assert_eq!(config.segments, 4);
        assert_eq!(
            config.connection(),
            "postgres://pico:secret@db:5432/pico?sslmode=disable"
        );
        assert!(config.redacted().contains("pico:***@db"));
        assert!(!config.redacted().contains("secret"));
    }

    #[test]
    fn masks_passwords_passed_as_parameters() {
        let config = Config::parse(
            "postgres://pico@db/pico?password=hunter2&sslpassword=keypass&sslmode=require&segments=4",
        )
        .unwrap();
        assert!(config.connection().contains("password=hunter2"));
        assert!(config.connection().contains("sslpassword=keypass"));
        assert!(!config.redacted().contains("hunter2"));
        assert!(!config.redacted().contains("keypass"));
        assert!(config.redacted().contains("sslmode=require"));
        assert!(config.redacted().contains("segments=4"));
    }

    #[test]
    fn rejects_other_schemes_and_bad_values() {
        assert!(Config::parse("0@s3://bucket").is_err());
        assert!(Config::parse("postgres://db/pico?segments=1").is_err());
        assert!(Config::parse("postgres://db/pico?synchronousCommit=off").is_err());
        assert!(Config::parse("postgres://db/pico?maxInflight=abc").is_err());
    }

    #[test]
    fn ring_must_outgrow_the_unflushed_cap() {
        assert!(
            Config::parse(
                "postgres://db/pico?segments=2&segmentBytes=1048576&maxUnflushedBytes=1048576"
            )
            .is_err()
        );
    }
}
