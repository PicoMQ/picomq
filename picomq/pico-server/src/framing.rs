//! Multi-record batch framing and content-type helpers.
//!
//! (byte-compatible: version byte + i32 count +
//! `server.ContentTypes`.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{ErrorKind, ServiceError};

const FRAME_VERSION: u8 = 1;

pub fn encode_frames(records: &[Bytes]) -> Bytes {
    let size = 1 + 4 + records.iter().map(|r| 4 + r.len()).sum::<usize>();
    let mut buf = BytesMut::with_capacity(size);
    buf.put_u8(FRAME_VERSION);
    buf.put_i32(records.len() as i32);
    for record in records {
        buf.put_i32(record.len() as i32);
        buf.put_slice(record);
    }
    buf.freeze()
}

pub fn decode_frames(payload: &[u8], expected_count: u32) -> Result<Vec<Bytes>, ServiceError> {
    let corrupt = |m: &str| ServiceError::with_message(ErrorKind::BadRequest, None, false, m);
    let mut buf = payload;
    if buf.remaining() < 5 {
        return Err(corrupt("batch frame truncated"));
    }
    let version = buf.get_u8();
    if version != FRAME_VERSION {
        return Err(corrupt(&format!("unknown batch frame version {version}")));
    }
    let count = buf.get_i32();
    if count < 0 || count as u32 != expected_count {
        return Err(corrupt(&format!(
            "batch frame count {count} does not match batch count {expected_count}"
        )));
    }
    let mut out = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if buf.remaining() < 4 {
            return Err(corrupt("batch frame truncated"));
        }
        let len = buf.get_i32();
        if len < 0 || buf.remaining() < len as usize {
            return Err(corrupt("batch frame truncated"));
        }
        out.push(Bytes::copy_from_slice(&buf[..len as usize]));
        buf.advance(len as usize);
    }
    if buf.has_remaining() {
        return Err(corrupt("trailing bytes after batch frames"));
    }
    Ok(out)
}

pub use picomq_protocol::mime::{is_json, mime_equals, mime_of};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_roundtrip() {
        let records = vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"bb"),
            Bytes::from_static(b""),
        ];
        let framed = encode_frames(&records);
        assert_eq!(decode_frames(&framed, 3).unwrap(), records);
    }

    #[test]
    fn frames_reject_count_mismatch_version_and_trailing() {
        let framed = encode_frames(&[Bytes::from_static(b"x")]);
        assert!(decode_frames(&framed, 2).is_err());

        let mut bad_version = framed.to_vec();
        bad_version[0] = 9;
        assert!(decode_frames(&bad_version, 1).is_err());

        let mut trailing = framed.to_vec();
        trailing.push(0);
        assert!(decode_frames(&trailing, 1).is_err());
    }
}
