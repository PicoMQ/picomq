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

use super::{Transform, TransformType};
use crate::{DecodedMessage, Error, Payload, TopicMetadata};
use regex::Regex;
use serde::{Deserialize, Serialize};
use simd_json::OwnedValue;
use simd_json::prelude::{TypedArrayValue, TypedObjectValue, TypedScalarValue, ValueAsScalar};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyPattern<T = String> {
    Exact(String),
    StartsWith(String),
    EndsWith(String),
    Contains(String),
    Regex(T),
}

impl KeyPattern<String> {
    pub fn compile(self) -> Result<KeyPattern<Regex>, Error> {
        Ok(match self {
            KeyPattern::Regex(pattern) => {
                KeyPattern::Regex(Regex::new(&pattern).map_err(|_| Error::InvalidConfig)?)
            }
            KeyPattern::Exact(s) => KeyPattern::Exact(s),
            KeyPattern::StartsWith(s) => KeyPattern::StartsWith(s),
            KeyPattern::EndsWith(s) => KeyPattern::EndsWith(s),
            KeyPattern::Contains(s) => KeyPattern::Contains(s),
        })
    }
}

impl KeyPattern<Regex> {
    pub fn matches(&self, k: &str) -> bool {
        match self {
            KeyPattern::Exact(s) => k == s,
            KeyPattern::StartsWith(s) => k.starts_with(s),
            KeyPattern::EndsWith(s) => k.ends_with(s),
            KeyPattern::Contains(s) => k.contains(s),
            KeyPattern::Regex(re) => re.is_match(k),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValuePattern<T = String> {
    Equals(OwnedValue),
    Contains(String),
    Regex(T),
    GreaterThan(f64),
    LessThan(f64),
    Between(f64, f64),
    IsNull,
    IsNotNull,
    IsString,
    IsNumber,
    IsBoolean,
    IsObject,
    IsArray,
}

impl ValuePattern<String> {
    pub fn compile(self) -> Result<ValuePattern<Regex>, Error> {
        use ValuePattern::*;
        Ok(match self {
            Regex(pattern) => Regex(regex::Regex::new(&pattern).map_err(|_| Error::InvalidConfig)?),
            Equals(v) => Equals(v),
            Contains(s) => Contains(s),
            GreaterThan(n) => GreaterThan(n),
            LessThan(n) => LessThan(n),
            Between(a, b) => Between(a, b),
            IsNull => IsNull,
            IsNotNull => IsNotNull,
            IsString => IsString,
            IsNumber => IsNumber,
            IsBoolean => IsBoolean,
            IsObject => IsObject,
            IsArray => IsArray,
        })
    }
}

impl ValuePattern<Regex> {
    pub fn matches(&self, v: &OwnedValue) -> bool {
        use ValuePattern::*;
        match self {
            Equals(x) => v == x,
            Contains(s) => v.as_str().is_some_and(|x| x.contains(s)),
            Regex(re) => v.as_str().is_some_and(|x| re.is_match(x)),
            GreaterThan(t) => v.as_f64().is_some_and(|n| n > *t),
            LessThan(t) => v.as_f64().is_some_and(|n| n < *t),
            Between(a, b) => v.as_f64().is_some_and(|n| n >= *a && n <= *b),
            IsNull => v.is_null(),
            IsNotNull => !v.is_null(),
            IsString => v.is_str(),
            IsNumber => v.is_number(),
            IsBoolean => v.is_bool(),
            IsObject => v.is_object(),
            IsArray => v.is_array(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterFieldsConfig {
    #[serde(default)]
    pub keep_fields: Vec<String>,
    #[serde(default)]
    pub patterns: Vec<FilterPattern>,
    #[serde(default = "default_include")]
    pub include_matching: bool,
}

fn default_include() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterPattern {
    #[serde(default)]
    pub key_pattern: Option<KeyPattern<String>>,
    #[serde(default)]
    pub value_pattern: Option<ValuePattern<String>>,
}

pub struct CompiledPattern {
    pub key_pattern: Option<KeyPattern<Regex>>,
    pub value_pattern: Option<ValuePattern<Regex>>,
}

pub struct FilterFields {
    pub include_matching: bool,
    pub keep_set: HashSet<String>,
    pub patterns: Vec<CompiledPattern>,
}

impl FilterFields {
    pub fn new(cfg: FilterFieldsConfig) -> Result<Self, Error> {
        let keep_set = cfg.keep_fields.into_iter().collect();

        let mut patterns = Vec::with_capacity(cfg.patterns.len());
        for p in cfg.patterns {
            patterns.push(CompiledPattern {
                key_pattern: p.key_pattern.map(|kp| kp.compile()).transpose()?,
                value_pattern: p.value_pattern.map(|vp| vp.compile()).transpose()?,
            });
        }

        Ok(Self {
            include_matching: cfg.include_matching,
            keep_set,
            patterns,
        })
    }

    #[inline]
    pub fn matches_patterns(&self, k: &str, v: &OwnedValue) -> bool {
        self.patterns.iter().any(|pat| {
            let key_ok = pat.key_pattern.as_ref().is_none_or(|kp| kp.matches(k));
            let value_ok = pat.value_pattern.as_ref().is_none_or(|vp| vp.matches(v));
            key_ok && value_ok
        })
    }
}

impl Transform for FilterFields {
    fn r#type(&self) -> TransformType {
        TransformType::FilterFields
    }

    fn transform(
        &self,
        metadata: &TopicMetadata,
        message: DecodedMessage,
    ) -> Result<Option<DecodedMessage>, Error> {
        if self.keep_set.is_empty() && self.patterns.is_empty() {
            return Ok(Some(message));
        }

        match &message.payload {
            Payload::Json(_) => self.transform_json(metadata, message),
            _ => Ok(Some(message)),
        }
    }
}
