use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::KafkaError;

const FRAME_HEADER_LEN: usize = 4;

pub async fn read_frame(
    reader: &mut (impl AsyncRead + Unpin),
    max_bytes: usize,
) -> Result<BytesMut, KafkaError> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    reader.read_exact(&mut header).await?;
    let size = i32::from_be_bytes(header);
    if size < 0 {
        return Err(KafkaError::InvalidFrameSize(size));
    }
    let size = size as usize;
    if size > max_bytes {
        return Err(KafkaError::FrameTooLarge {
            size,
            max: max_bytes,
        });
    }
    let mut body = BytesMut::zeroed(size);
    reader.read_exact(&mut body).await?;
    Ok(body)
}

pub async fn write_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    body: &[u8],
) -> Result<(), KafkaError> {
    let size = i32::try_from(body.len()).map_err(|_| KafkaError::InvalidFrameSize(-1))?;
    let mut frame = BytesMut::with_capacity(FRAME_HEADER_LEN + body.len());
    frame.put_i32(size);
    frame.extend_from_slice(body);
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn round_trip_frame() {
        let (mut client, mut server) = duplex(64);
        let payload = b"hello";
        tokio::spawn(async move {
            write_frame(&mut client, payload).await.unwrap();
        });
        let read = read_frame(&mut server, 1024).await.unwrap();
        assert_eq!(&read[..], payload);
    }
}
