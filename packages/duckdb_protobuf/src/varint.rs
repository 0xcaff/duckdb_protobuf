use thiserror::Error;

const MAX_VARINT_ENCODED_LEN: usize = 10;
const MAX_VARINT32_ENCODED_LEN: usize = 5;

pub trait DecodeVarint {
    const MAX_ENCODED_LEN: usize;
    const LAST_BYTE_MAX_VALUE: u8;

    fn from_u64(value: u64) -> Self;
}

impl DecodeVarint for u64 {
    const MAX_ENCODED_LEN: usize = MAX_VARINT_ENCODED_LEN;
    const LAST_BYTE_MAX_VALUE: u8 = 0x01;

    fn from_u64(value: u64) -> Self {
        value
    }
}

impl DecodeVarint for u32 {
    const MAX_ENCODED_LEN: usize = MAX_VARINT32_ENCODED_LEN;
    const LAST_BYTE_MAX_VALUE: u8 = 0x0f;

    fn from_u64(value: u64) -> Self {
        value as u32
    }
}

#[derive(Error, Debug)]
#[error("varint doesn't fit into provided type")]
pub struct IncorrectVarintError;

/// Decode a varint, and return decoded value and decoded byte count.
#[inline]
fn decode_varint_full<D: DecodeVarint>(
    rem: &[u8],
) -> Result<Option<(D, usize)>, IncorrectVarintError> {
    let mut r: u64 = 0;
    for (i, &b) in rem.iter().enumerate() {
        if i == D::MAX_ENCODED_LEN - 1 {
            if b > D::LAST_BYTE_MAX_VALUE {
                return Err(IncorrectVarintError);
            }
            let r = r | ((b as u64) << (i as u64 * 7));
            return Ok(Some((D::from_u64(r), i + 1)));
        }

        r = r | (((b & 0x7f) as u64) << (i as u64 * 7));
        if b < 0x80 {
            return Ok(Some((D::from_u64(r), i + 1)));
        }
    }
    Ok(None)
}

#[inline]
pub fn decode_varint<D: DecodeVarint>(
    buf: &[u8],
) -> Result<Option<(D, usize)>, IncorrectVarintError> {
    if buf.len() >= 1 && buf[0] < 0x80 {
        // The the most common case.
        let ret = buf[0] as u64;
        let consume = 1;
        Ok(Some((D::from_u64(ret), consume)))
    } else if buf.len() >= 2 && buf[1] < 0x80 {
        // Handle the case of two bytes too.
        let ret = (buf[0] & 0x7f) as u64 | (buf[1] as u64) << 7;
        let consume = 2;
        Ok(Some((D::from_u64(ret), consume)))
    } else {
        // Read from array when buf at at least 10 bytes,
        // max len for varint.
        decode_varint_full(buf)
    }
}
