use chrono::{SecondsFormat, Utc};
use simd_json::OwnedValue;

use super::ComputedValue;

pub mod add_fields;
pub mod delete_fields;
pub mod filter_fields;
pub mod unwrap_envelope;
pub mod update_fields;

pub fn compute_value(kind: &ComputedValue) -> OwnedValue {
    let now = Utc::now();
    match kind {
        ComputedValue::DateTime => now.to_rfc3339_opts(SecondsFormat::Micros, true).into(),
        ComputedValue::TimestampNanos => now
            .timestamp_nanos_opt()
            .expect("Nanosecond timestamp overflow")
            .into(),
        ComputedValue::TimestampMicros => now.timestamp_micros().into(),
        ComputedValue::TimestampMillis => now.timestamp_millis().into(),
        ComputedValue::TimestampSeconds => now.timestamp().into(),
        ComputedValue::UuidV4 => uuid::Uuid::new_v4().to_string().into(),
        ComputedValue::UuidV7 => uuid::Uuid::now_v7().to_string().into(),
    }
}

#[cfg(test)]
pub mod test_utils;
