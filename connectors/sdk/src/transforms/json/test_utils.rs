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
