// Modified from Apache Iggy for PicoMQ.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use chrono::{DateTime, Utc};
use picomq_connector_sdk::Error;

use crate::OutputFormat;

const TOPIC_SEGMENT_PLACEHOLDER: &str = "{topic_segment[";

pub(crate) struct PathContext<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub first_timestamp_millis: u64,
}

pub(crate) fn render_s3_key(
    prefix: Option<&str>,
    template: &str,
    ctx: &PathContext<'_>,
    offset_start: u64,
    offset_end: u64,
    format: OutputFormat,
) -> Result<String, Error> {
    let rendered = render_template(template, ctx)?;

    let filename = format!(
        "{:05}-{:020}-{:020}.{}",
        ctx.partition,
        offset_start,
        offset_end,
        format.file_extension()
    );

    let key = match prefix {
        Some(p) => {
            let p = p.trim_matches('/');
            if p.is_empty() {
                format!("{rendered}/{filename}")
            } else {
                format!("{p}/{rendered}/{filename}")
            }
        }
        None => format!("{rendered}/{filename}"),
    };

    Ok(key)
}

fn render_template(template: &str, ctx: &PathContext<'_>) -> Result<String, Error> {
    let dt = timestamp_to_datetime(ctx.first_timestamp_millis)?;
    let date = dt.format("%Y-%m-%d").to_string();
    let hour = dt.format("%H").to_string();

    let with_segments = render_topic_segments(template, ctx.topic)?;
    Ok(with_segments
        .replace("{topic}", &sanitize_key_segment(ctx.topic))
        .replace("{partition}", &ctx.partition.to_string())
        .replace("{date}", &date)
        .replace("{hour}", &hour)
        .replace("{timestamp}", &ctx.first_timestamp_millis.to_string()))
}

fn render_topic_segments(template: &str, topic: &str) -> Result<String, Error> {
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find(TOPIC_SEGMENT_PLACEHOLDER) {
        rendered.push_str(&rest[..start]);
        let after = &rest[start + TOPIC_SEGMENT_PLACEHOLDER.len()..];
        let Some(end) = after.find("]}") else {
            return Err(Error::InvalidConfigValue(format!(
                "Unterminated topic_segment placeholder in path_template '{template}'"
            )));
        };
        let index: isize = after[..end].parse().map_err(|_| {
            Error::InvalidConfigValue(format!(
                "Invalid topic_segment index '{}' in path_template '{template}'",
                &after[..end]
            ))
        })?;
        let segments: Vec<&str> = topic.split('.').collect();
        let position = if index < 0 {
            segments.len().checked_sub(index.unsigned_abs())
        } else {
            Some(index.unsigned_abs())
        };
        let Some(segment) = position.and_then(|position| segments.get(position)) else {
            return Err(Error::CannotStoreData(format!(
                "Topic '{topic}' has no segment {index} for path_template '{template}'"
            )));
        };
        rendered.push_str(&sanitize_key_segment(segment));
        rest = &after[end + 2..];
    }
    rendered.push_str(rest);
    Ok(rendered)
}

fn sanitize_key_segment(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn timestamp_to_datetime(millis: u64) -> Result<DateTime<Utc>, Error> {
    let millis = i64::try_from(millis).map_err(|_| {
        Error::CannotStoreData(format!(
            "Invalid message timestamp: {millis} millis is out of range"
        ))
    })?;
    DateTime::<Utc>::from_timestamp_millis(millis).ok_or_else(|| {
        Error::CannotStoreData(format!(
            "Invalid message timestamp: {millis} millis is out of range"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ctx() -> PathContext<'static> {
        PathContext {
            topic: "app_logs.api_requests",
            partition: 1,
            first_timestamp_millis: 1_710_597_600_000,
        }
    }

    #[test]
    fn render_default_template() {
        let ctx = test_ctx();
        let key = render_s3_key(
            Some("picomq/raw"),
            "{topic}/{date}/{hour}",
            &ctx,
            0,
            99,
            OutputFormat::JsonLines,
        )
        .unwrap();
        assert_eq!(
            key,
            "picomq/raw/app_logs.api_requests/2024-03-16/14/00001-00000000000000000000-00000000000000000099.jsonl"
        );
    }

    #[test]
    fn render_with_topic_segments() {
        let ctx = test_ctx();
        let key = render_s3_key(
            None,
            "{topic_segment[0]}/{topic_segment[-1]}/{partition}/{date}",
            &ctx,
            100,
            199,
            OutputFormat::JsonArray,
        )
        .unwrap();
        assert_eq!(
            key,
            "app_logs/api_requests/1/2024-03-16/00001-00000000000000000100-00000000000000000199.json"
        );
    }

    #[test]
    fn render_missing_topic_segment_fails() {
        let ctx = test_ctx();
        let result = render_s3_key(None, "{topic_segment[5]}", &ctx, 0, 0, OutputFormat::Raw);
        assert!(result.is_err());
    }

    #[test]
    fn render_no_prefix() {
        let ctx = test_ctx();
        let key = render_s3_key(None, "{topic}", &ctx, 0, 9, OutputFormat::Raw).unwrap();
        assert_eq!(
            key,
            "app_logs.api_requests/00001-00000000000000000000-00000000000000000009.bin"
        );
    }

    #[test]
    fn render_empty_prefix() {
        let ctx = test_ctx();
        let key = render_s3_key(
            Some(""),
            "{topic_segment[0]}",
            &ctx,
            0,
            0,
            OutputFormat::JsonLines,
        )
        .unwrap();
        assert_eq!(
            key,
            "app_logs/00001-00000000000000000000-00000000000000000000.jsonl"
        );
    }

    #[test]
    fn render_prefix_with_trailing_slash() {
        let ctx = test_ctx();
        let key = render_s3_key(
            Some("data/"),
            "{topic_segment[1]}",
            &ctx,
            5,
            10,
            OutputFormat::JsonLines,
        )
        .unwrap();
        assert_eq!(
            key,
            "data/api_requests/00001-00000000000000000005-00000000000000000010.jsonl"
        );
    }

    #[test]
    fn timestamp_deterministic_from_message() {
        let ctx = test_ctx();
        let key1 = render_s3_key(None, "{timestamp}", &ctx, 0, 0, OutputFormat::Raw).unwrap();
        let key2 = render_s3_key(None, "{timestamp}", &ctx, 0, 0, OutputFormat::Raw).unwrap();
        assert_eq!(key1, key2);
    }

    #[test]
    fn timestamp_to_datetime_zero() {
        let dt = timestamp_to_datetime(0).unwrap();
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "1970-01-01");
    }

    #[test]
    fn timestamp_to_datetime_known() {
        let dt = timestamp_to_datetime(1_710_597_600_000).unwrap();
        assert_eq!(dt.format("%Y-%m-%dT%H").to_string(), "2024-03-16T14");
    }

    #[test]
    fn sanitize_topic_names() {
        let ctx = PathContext {
            topic: "my//topic with spaces",
            partition: 0,
            first_timestamp_millis: 1_710_597_600_000,
        };
        let key = render_s3_key(None, "{topic}", &ctx, 0, 0, OutputFormat::Raw).unwrap();
        assert!(
            !key.contains("//"),
            "Sanitized key must not contain '//' from topic name: {key}"
        );
        assert!(
            !key.contains(' '),
            "Sanitized key must not contain spaces: {key}"
        );
    }

    #[test]
    fn lex_sort_correct_with_large_offsets() {
        let ctx = test_ctx();
        let key_small =
            render_s3_key(None, "{topic}", &ctx, 999_900, 999_999, OutputFormat::Raw).unwrap();
        let key_large = render_s3_key(
            None,
            "{topic}",
            &ctx,
            1_000_000,
            1_001_000,
            OutputFormat::Raw,
        )
        .unwrap();
        assert!(key_small < key_large, "Lexicographic sort must be correct");
    }
}
