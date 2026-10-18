use bytes::{Buf, BufMut, BytesMut};

use crate::error::{ErrorKind, ServiceError};

pub(crate) fn put_str(buf: &mut BytesMut, value: &str) {
    buf.put_i32(value.len() as i32);
    buf.put_slice(value.as_bytes());
}

pub(crate) fn put_opt_str(buf: &mut BytesMut, value: Option<&str>) {
    buf.put_u8(value.is_some() as u8);
    if let Some(value) = value {
        put_str(buf, value);
    }
}

pub(crate) fn put_opt_i64(buf: &mut BytesMut, value: Option<i64>) {
    buf.put_u8(value.is_some() as u8);
    buf.put_i64(value.unwrap_or(0));
}

pub(crate) fn get_u8(buf: &mut &[u8]) -> Result<u8, ServiceError> {
    ensure(buf, 1)?;
    Ok(buf.get_u8())
}

pub(crate) fn get_i16(buf: &mut &[u8]) -> Result<i16, ServiceError> {
    ensure(buf, 2)?;
    Ok(buf.get_i16())
}

pub(crate) fn get_i32(buf: &mut &[u8]) -> Result<i32, ServiceError> {
    ensure(buf, 4)?;
    Ok(buf.get_i32())
}

pub(crate) fn get_u32(buf: &mut &[u8]) -> Result<u32, ServiceError> {
    ensure(buf, 4)?;
    Ok(buf.get_u32())
}

pub(crate) fn get_i64(buf: &mut &[u8]) -> Result<i64, ServiceError> {
    ensure(buf, 8)?;
    Ok(buf.get_i64())
}

pub(crate) fn get_bytes<const N: usize>(buf: &mut &[u8]) -> Result<[u8; N], ServiceError> {
    ensure(buf, N)?;
    let mut out = [0u8; N];
    out.copy_from_slice(&buf[..N]);
    buf.advance(N);
    Ok(out)
}

pub(crate) fn get_str(buf: &mut &[u8]) -> Result<String, ServiceError> {
    let len = get_i32(buf)?;
    if len < 0 {
        return Err(corrupt("negative length"));
    }
    ensure(buf, len as usize)?;

    let value =
        String::from_utf8(buf[..len as usize].to_vec()).map_err(|_| corrupt("invalid UTF-8"))?;
    buf.advance(len as usize);

    Ok(value)
}

pub(crate) fn get_opt_str(buf: &mut &[u8]) -> Result<Option<String>, ServiceError> {
    match get_u8(buf)? {
        0 => Ok(None),
        _ => Ok(Some(get_str(buf)?)),
    }
}

pub(crate) fn get_opt_i64(buf: &mut &[u8]) -> Result<Option<i64>, ServiceError> {
    let present = get_u8(buf)? == 1;
    let value = get_i64(buf)?;

    Ok(present.then_some(value))
}

pub(crate) fn expect_version(
    buf: &mut &[u8],
    expected: u8,
    what: &str,
) -> Result<(), ServiceError> {
    let version = get_u8(buf)?;
    if version != expected {
        return Err(corrupt(&format!("unknown {what} version {version}")));
    }

    Ok(())
}

pub(crate) fn expect_end(buf: &[u8], what: &str) -> Result<(), ServiceError> {
    if buf.has_remaining() {
        return Err(corrupt(&format!("trailing bytes after {what}")));
    }

    Ok(())
}

fn ensure(buf: &[u8], n: usize) -> Result<(), ServiceError> {
    if buf.remaining() < n {
        return Err(corrupt("metadata record truncated"));
    }

    Ok(())
}

fn corrupt(message: &str) -> ServiceError {
    ServiceError::with_message(ErrorKind::BadRequest, None, false, message)
}
