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

use crate::{DecodedMessage, Payload, TopicMetadata};
use simd_json::OwnedValue;
use simd_json::prelude::{TypedScalarValue, ValueAsScalar};
use uuid;

pub fn create_test_message(json: &str) -> DecodedMessage {
    let mut payload = json.to_string().into_bytes();
    let value = simd_json::to_owned_value(&mut payload).unwrap();
    DecodedMessage {
        offset: None,
        timestamp: None,
        key: None,
        headers: None,
        payload: Payload::Json(value),
    }
}

pub fn create_raw_test_message(bytes: Vec<u8>) -> DecodedMessage {
    DecodedMessage {
        offset: None,
        timestamp: None,
        key: None,
        headers: None,
        payload: Payload::Raw(bytes),
    }
}

pub fn create_test_topic_metadata() -> TopicMetadata {
    TopicMetadata {
        topic: "test-topic".to_string(),
    }
}

pub fn extract_json_object(msg: &DecodedMessage) -> Option<&simd_json::owned::Object> {
    if let Payload::Json(OwnedValue::Object(map)) = &msg.payload {
        Some(map)
    } else {
        None
    }
}

pub fn assert_is_number(value: &OwnedValue, field_name: &str) {
    if !value.is_number() {
        panic!("{field_name} should be a number");
    }
}

pub fn assert_is_string(value: &OwnedValue, field_name: &str) {
    if !value.is_str() {
        panic!("{field_name} should be a string");
    }
}

pub fn assert_is_uuid(value: &OwnedValue, field_name: &str) {
    if !value.is_str() {
        panic!("{field_name} should be a string");
    }

    let string_value = value.as_str().unwrap();
    if uuid::Uuid::parse_str(string_value).is_err() {
        panic!("{field_name} is not a valid UUID");
    }
}
