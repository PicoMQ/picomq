use bytes::Bytes;
use s3stream_codec::{
    StreamRecordBatch, WAL_RECORD_HEADER_SIZE, WalRecordHeader, decode_record, frame_record,
};

use crate::error::Error;

const HEADER: u64 = WAL_RECORD_HEADER_SIZE as u64;

pub(crate) struct Framed {
    pub(crate) offset: u64,
    pub(crate) size: u32,
    pub(crate) record: StreamRecordBatch,
}

pub(crate) fn size(record: &StreamRecordBatch) -> u64 {
    record.encoded().len() as u64 + HEADER
}

pub(crate) fn encode<'a>(
    start: u64,
    records: impl IntoIterator<Item = &'a StreamRecordBatch>,
) -> Vec<u8> {
    let mut body = Vec::new();
    let mut offset = start;
    for record in records {
        let framed = frame_record(offset, &record.encoded());
        offset += framed.len() as u64;
        body.extend_from_slice(&framed);
    }
    body
}

pub(crate) fn decode(start: u64, mut body: Bytes) -> Result<Vec<Framed>, Error> {
    let mut offset = start;
    let mut framed = Vec::new();
    while !body.is_empty() {
        let header = WalRecordHeader::unmarshal(&body)?;
        if header.body_offset != offset + HEADER {
            return Err(Error::Corrupt(format!(
                "record at {offset} claims body offset {}",
                header.body_offset
            )));
        }
        let size = WAL_RECORD_HEADER_SIZE as u32 + header.body_length;
        let record = decode_record(&mut body)?;
        framed.push(Framed {
            offset,
            size,
            record,
        });
        offset += u64::from(size);
    }
    Ok(framed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(base_offset: u64, payload: &'static [u8]) -> StreamRecordBatch {
        StreamRecordBatch::new(7, 1, base_offset, 1, Bytes::from_static(payload))
    }

    #[test]
    fn round_trips_offsets_and_sizes() {
        let records = [record(0, b"alpha"), record(1, b"beta")];
        let body = encode(100, &records);
        let framed = decode(100, Bytes::from(body)).unwrap();
        assert_eq!(framed.len(), 2);
        assert_eq!(framed[0].offset, 100);
        assert_eq!(u64::from(framed[0].size), size(&records[0]));
        assert_eq!(framed[1].offset, 100 + size(&records[0]));
        assert_eq!(framed[1].record.payload().as_ref(), b"beta");
    }

    #[test]
    fn rejects_a_body_decoded_at_the_wrong_offset() {
        let body = encode(100, &[record(0, b"alpha")]);
        assert!(matches!(
            decode(200, Bytes::from(body)),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn rejects_flipped_bytes() {
        let mut body = encode(0, &[record(0, b"alpha")]);
        let last = body.len() - 1;
        body[last] ^= 0xff;
        assert!(matches!(decode(0, Bytes::from(body)), Err(Error::Codec(_))));
    }
}
