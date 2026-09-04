use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::Error;

pub const DEFAULT_TOPIC_SEPARATOR: char = '.';

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationTemplate {
    raw: String,
    parts: Vec<Part>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Part {
    Literal(String),
    Topic,
    Segment(isize),
}

impl DestinationTemplate {
    pub fn literal(value: &str) -> Self {
        Self {
            raw: value.to_owned(),
            parts: vec![Part::Literal(value.to_owned())],
        }
    }

    pub fn is_static(&self) -> bool {
        self.parts
            .iter()
            .all(|part| matches!(part, Part::Literal(_)))
    }

    pub fn resolve(&self, topic: &str) -> Result<String, Error> {
        self.resolve_with_separator(topic, DEFAULT_TOPIC_SEPARATOR)
    }

    pub fn resolve_with_separator(&self, topic: &str, separator: char) -> Result<String, Error> {
        let mut resolved = String::with_capacity(self.raw.len() + topic.len());
        for part in &self.parts {
            match part {
                Part::Literal(literal) => resolved.push_str(literal),
                Part::Topic => resolved.push_str(topic),
                Part::Segment(index) => {
                    let segments: Vec<&str> = topic.split(separator).collect();
                    let position = if *index < 0 {
                        segments.len().checked_sub(index.unsigned_abs())
                    } else {
                        Some(index.unsigned_abs())
                    };
                    let segment = position.and_then(|position| segments.get(position));
                    let Some(segment) = segment else {
                        return Err(Error::InvalidRecordValue(format!(
                            "topic '{topic}' has no segment {index} for destination template '{}'",
                            self.raw
                        )));
                    };
                    resolved.push_str(segment);
                }
            }
        }
        Ok(resolved)
    }
}

impl FromStr for DestinationTemplate {
    type Err = Error;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let mut parts = Vec::new();
        let mut literal = String::new();
        let mut rest = raw;
        while let Some(open) = rest.find('{') {
            literal.push_str(&rest[..open]);
            let after_open = &rest[open + 1..];
            let Some(close) = after_open.find('}') else {
                return Err(Error::InvalidConfigValue(format!(
                    "unterminated placeholder in destination template '{raw}'"
                )));
            };
            let placeholder = &after_open[..close];
            let part = parse_placeholder(placeholder).ok_or_else(|| {
                Error::InvalidConfigValue(format!(
                    "unknown placeholder '{{{placeholder}}}' in destination template '{raw}'"
                ))
            })?;
            if !literal.is_empty() {
                parts.push(Part::Literal(std::mem::take(&mut literal)));
            }
            parts.push(part);
            rest = &after_open[close + 1..];
        }
        literal.push_str(rest);
        if !literal.is_empty() {
            parts.push(Part::Literal(literal));
        }
        Ok(Self {
            raw: raw.to_owned(),
            parts,
        })
    }
}

fn parse_placeholder(placeholder: &str) -> Option<Part> {
    if placeholder == "topic" {
        return Some(Part::Topic);
    }
    let index = placeholder
        .strip_prefix("topic_segment[")?
        .strip_suffix(']')?;
    index.parse::<isize>().ok().map(Part::Segment)
}

impl fmt::Display for DestinationTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.raw)
    }
}

impl Serialize for DestinationTemplate {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for DestinationTemplate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: &str) -> DestinationTemplate {
        raw.parse().unwrap()
    }

    #[test]
    fn given_literal_template_should_be_static() {
        let template = parse("orders");
        assert!(template.is_static());
        assert_eq!(template.resolve("anything").unwrap(), "orders");
    }

    #[test]
    fn given_topic_placeholder_should_substitute_whole_topic() {
        let template = parse("t_{topic}");
        assert!(!template.is_static());
        assert_eq!(template.resolve("orders.eu").unwrap(), "t_orders.eu");
    }

    #[test]
    fn given_segment_placeholders_should_substitute_by_index() {
        let template = parse("{topic_segment[0]}_{topic_segment[-1]}");
        assert_eq!(
            template.resolve("orders.eu.user42").unwrap(),
            "orders_user42"
        );
    }

    #[test]
    fn given_out_of_range_segment_should_fail() {
        let template = parse("{topic_segment[3]}");
        assert!(template.resolve("a.b").is_err());
        let template = parse("{topic_segment[-3]}");
        assert!(template.resolve("a.b").is_err());
    }

    #[test]
    fn given_custom_separator_should_split_on_it() {
        let template = parse("{topic_segment[1]}");
        assert_eq!(template.resolve_with_separator("a-b-c", '-').unwrap(), "b");
    }

    #[test]
    fn given_bad_placeholder_should_fail_to_parse() {
        assert!("{stream}".parse::<DestinationTemplate>().is_err());
        assert!("{topic".parse::<DestinationTemplate>().is_err());
        assert!("{topic_segment[x]}".parse::<DestinationTemplate>().is_err());
    }

    #[test]
    fn given_toml_string_should_deserialize() {
        #[derive(Deserialize)]
        struct Config {
            table: DestinationTemplate,
        }
        let config: Config = toml::from_str(r#"table = "events_{topic_segment[1]}""#).unwrap();
        assert_eq!(config.table.resolve("app.orders").unwrap(), "events_orders");
    }
}
