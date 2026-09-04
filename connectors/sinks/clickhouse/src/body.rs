use picomq_connector_sdk::{ConsumedMessage, Error, Payload};
use tracing::error;

use crate::StringFormat;
use crate::schema::Column;

pub(crate) fn build_json_body(messages: &[ConsumedMessage]) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::with_capacity(messages.len() * 64);
    for msg in messages {
        match &msg.payload {
            Payload::Json(value) => {
                simd_json::to_writer(&mut buf, value).map_err(|e| {
                    error!(
                        "Failed to serialise JSON payload at offset {}: {e}",
                        msg.offset
                    );
                    Error::InvalidJsonPayload
                })?;
                buf.push(b'\n');
            }
            other => return Err(unsupported_payload("JSONEachRow", msg.offset, other)),
        }
    }
    Ok(buf)
}

pub(crate) fn build_row_binary_body(
    messages: &[ConsumedMessage],
    schema: &[Column],
) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::with_capacity(messages.len() * 128);
    for msg in messages {
        match &msg.payload {
            Payload::Json(value) => {
                crate::binary::serialize_row(value, schema, &mut buf)?;
            }
            other => return Err(unsupported_payload("RowBinary", msg.offset, other)),
        }
    }
    Ok(buf)
}

pub(crate) fn build_string_body(
    messages: &[ConsumedMessage],
    string_format: StringFormat,
) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::with_capacity(messages.len() * 64);
    for msg in messages {
        match &msg.payload {
            Payload::Text(s) => {
                buf.extend_from_slice(s.as_bytes());
                if string_format.requires_newline() && !s.ends_with('\n') {
                    buf.push(b'\n');
                }
            }
            other => return Err(unsupported_payload("String passthrough", msg.offset, other)),
        }
    }
    Ok(buf)
}

fn unsupported_payload(mode: &str, offset: u64, payload: &Payload) -> Error {
    let kind = match payload {
        Payload::Json(_) => "json",
        Payload::Raw(_) => "raw",
        Payload::Text(_) => "text",
        Payload::Proto(_) => "proto",
        Payload::FlatBuffer(_) => "flatbuffer",
        Payload::Avro(_) => "avro",
    };
    error!("{mode} mode: unsupported payload type '{kind}' at offset {offset}");
    Error::InvalidPayloadType
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StringFormat;
    use crate::schema::{ChType, Column};
    use picomq_connector_sdk::{ConsumedMessage, Payload};
    use simd_json::{OwnedValue, StaticNode};

    fn msg(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            offset: 0,
            timestamp: 0,
            key: None,
            headers: None,
            payload,
        }
    }

    fn col(name: &str, ch_type: ChType) -> Column {
        Column {
            name: name.into(),
            ch_type,
            has_default: false,
        }
    }

    fn json_null() -> OwnedValue {
        OwnedValue::Static(StaticNode::Null)
    }

    #[test]
    fn json_body_empty_input_returns_empty_buf() {
        assert!(build_json_body(&[]).unwrap().is_empty());
    }

    #[test]
    fn json_body_null_payload_produces_null_line() {
        let messages = vec![msg(Payload::Json(json_null()))];
        assert_eq!(build_json_body(&messages).unwrap(), b"null\n");
    }

    #[test]
    fn json_body_appends_one_line_per_message() {
        let messages = vec![
            msg(Payload::Json(json_null())),
            msg(Payload::Json(json_null())),
        ];
        assert_eq!(build_json_body(&messages).unwrap(), b"null\nnull\n");
    }

    #[test]
    fn given_non_json_payload_json_body_should_fail() {
        let messages = vec![msg(Payload::Text("hello".into()))];
        assert_eq!(
            build_json_body(&messages).unwrap_err(),
            Error::InvalidPayloadType
        );
    }

    #[test]
    fn given_mixed_payloads_json_body_should_fail_whole_batch() {
        let messages = vec![
            msg(Payload::Json(json_null())),
            msg(Payload::Text("rejected".into())),
            msg(Payload::Raw(vec![1, 2, 3])),
            msg(Payload::Json(json_null())),
        ];
        assert!(build_json_body(&messages).is_err());
    }

    #[test]
    fn string_body_empty_input_returns_empty_buf() {
        assert!(
            build_string_body(&[], StringFormat::Csv)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn string_body_csv_appends_newline_when_missing() {
        let messages = vec![msg(Payload::Text("a,b,c".into()))];
        assert_eq!(
            build_string_body(&messages, StringFormat::Csv).unwrap(),
            b"a,b,c\n"
        );
    }

    #[test]
    fn string_body_csv_does_not_double_newline() {
        let messages = vec![msg(Payload::Text("a,b,c\n".into()))];
        assert_eq!(
            build_string_body(&messages, StringFormat::Csv).unwrap(),
            b"a,b,c\n"
        );
    }

    #[test]
    fn string_body_tsv_appends_newline_when_missing() {
        let messages = vec![msg(Payload::Text("a\tb\tc".into()))];
        assert_eq!(
            build_string_body(&messages, StringFormat::Tsv).unwrap(),
            b"a\tb\tc\n"
        );
    }

    #[test]
    fn string_body_json_each_row_appends_newline_when_missing() {
        let messages = vec![msg(Payload::Text("{\"k\":1}".into()))];
        assert_eq!(
            build_string_body(&messages, StringFormat::JsonEachRow).unwrap(),
            b"{\"k\":1}\n"
        );
    }

    #[test]
    fn string_body_json_each_row_does_not_double_newline() {
        let messages = vec![msg(Payload::Text("{\"k\":1}\n".into()))];
        assert_eq!(
            build_string_body(&messages, StringFormat::JsonEachRow).unwrap(),
            b"{\"k\":1}\n"
        );
    }

    #[test]
    fn string_body_json_each_row_multi_message_newline_delimited() {
        let messages = vec![
            msg(Payload::Text("{\"a\":1}".into())),
            msg(Payload::Text("{\"b\":2}".into())),
        ];
        assert_eq!(
            build_string_body(&messages, StringFormat::JsonEachRow).unwrap(),
            b"{\"a\":1}\n{\"b\":2}\n"
        );
    }

    #[test]
    fn string_body_csv_multi_message_newline_delimited() {
        let messages = vec![
            msg(Payload::Text("a,b,c".into())),
            msg(Payload::Text("d,e,f".into())),
        ];
        assert_eq!(
            build_string_body(&messages, StringFormat::Csv).unwrap(),
            b"a,b,c\nd,e,f\n"
        );
    }

    #[test]
    fn string_body_tsv_multi_message_newline_delimited() {
        let messages = vec![
            msg(Payload::Text("a\tb\tc".into())),
            msg(Payload::Text("d\te\tf".into())),
        ];
        assert_eq!(
            build_string_body(&messages, StringFormat::Tsv).unwrap(),
            b"a\tb\tc\nd\te\tf\n"
        );
    }

    #[test]
    fn given_non_text_payload_string_body_should_fail() {
        let messages = vec![
            msg(Payload::Raw(vec![1, 2, 3])),
            msg(Payload::Json(json_null())),
        ];
        assert_eq!(
            build_string_body(&messages, StringFormat::Csv).unwrap_err(),
            Error::InvalidPayloadType
        );
    }

    #[test]
    fn row_binary_body_empty_input_returns_empty_buf() {
        assert!(build_row_binary_body(&[], &[]).unwrap().is_empty());
    }

    #[test]
    fn given_non_json_payload_row_binary_body_should_fail() {
        let messages = vec![
            msg(Payload::Text("hello".into())),
            msg(Payload::Raw(vec![0xFF])),
        ];
        assert_eq!(
            build_row_binary_body(&messages, &[col("x", ChType::String)]).unwrap_err(),
            Error::InvalidPayloadType
        );
    }

    #[test]
    fn row_binary_body_json_payload_writes_bytes() {
        let mut obj = simd_json::owned::Object::with_capacity(1);
        obj.insert("name".to_string(), OwnedValue::String("alice".into()));
        let messages = vec![msg(Payload::Json(OwnedValue::Object(Box::new(obj))))];
        let schema = vec![col("name", ChType::String)];
        let body = build_row_binary_body(&messages, &schema).unwrap();
        assert_eq!(body, b"\x00\x05alice");
    }

    #[test]
    fn row_binary_body_multiple_rows_concatenated() {
        let mut obj1 = simd_json::owned::Object::with_capacity(1);
        obj1.insert("n".to_string(), OwnedValue::String("x".into()));
        let mut obj2 = simd_json::owned::Object::with_capacity(1);
        obj2.insert("n".to_string(), OwnedValue::String("y".into()));
        let messages = vec![
            msg(Payload::Json(OwnedValue::Object(Box::new(obj1)))),
            msg(Payload::Json(OwnedValue::Object(Box::new(obj2)))),
        ];
        let schema = vec![col("n", ChType::String)];
        let body = build_row_binary_body(&messages, &schema).unwrap();
        assert_eq!(body, b"\x00\x01x\x00\x01y");
    }
}
