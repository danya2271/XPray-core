use std::fmt;

pub const MAX_ADDRESS: usize = 2048;
pub const MAX_MESSAGE: usize = 2048;
pub const MAX_PADDING: usize = 4096;
pub const TCP_REQUEST: u64 = 0x401;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Incomplete,
    Invalid(&'static str),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Incomplete => f.write_str("incomplete frame"),
            Self::Invalid(reason) => f.write_str(reason),
        }
    }
}

impl std::error::Error for Error {}

pub fn read_varint(input: &[u8]) -> Result<(u64, usize), Error> {
    let first = *input.first().ok_or(Error::Incomplete)?;
    let len = 1usize << (first >> 6);
    if input.len() < len {
        return Err(Error::Incomplete);
    }
    let mut value = (first & 0x3f) as u64;
    for b in &input[1..len] {
        value = (value << 8) | *b as u64;
    }
    Ok((value, len))
}

pub fn varint_len(value: u64) -> Result<usize, Error> {
    match value {
        0..=63 => Ok(1),
        64..=16383 => Ok(2),
        16384..=1073741823 => Ok(4),
        1073741824..=4611686018427387903 => Ok(8),
        _ => Err(Error::Invalid("QUIC varint overflow")),
    }
}

pub fn write_varint(value: u64, output: &mut Vec<u8>) -> Result<(), Error> {
    let len = varint_len(value)?;
    let mut bytes = value.to_be_bytes();
    bytes[8 - len] |= match len {
        1 => 0,
        2 => 0x40,
        4 => 0x80,
        _ => 0xc0,
    };
    output.extend_from_slice(&bytes[8 - len..]);
    Ok(())
}

fn field(input: &[u8], limit: usize, nonempty: bool) -> Result<(&[u8], usize), Error> {
    let (len, prefix) = read_varint(input)?;
    if len > limit as u64 || (nonempty && len == 0) {
        return Err(Error::Invalid("invalid field length"));
    }
    let end = prefix + len as usize;
    Ok((input.get(prefix..end).ok_or(Error::Incomplete)?, end))
}

pub fn parse_tcp_request(input: &[u8]) -> Result<(&[u8], usize), Error> {
    let (kind, prefix) = read_varint(input)?;
    if kind != TCP_REQUEST {
        return Err(Error::Invalid("not a TCP request"));
    }
    let (address, address_end) = field(&input[prefix..], MAX_ADDRESS, true)?;
    let (_, padding_end) = field(&input[prefix + address_end..], MAX_PADDING, false)?;
    Ok((address, prefix + address_end + padding_end))
}

pub fn encode_tcp_request(address: &[u8], padding: &[u8]) -> Result<Vec<u8>, Error> {
    if address.is_empty() || address.len() > MAX_ADDRESS || padding.len() > MAX_PADDING {
        return Err(Error::Invalid("invalid TCP request lengths"));
    }
    let mut output = Vec::with_capacity(18 + address.len() + padding.len());
    write_varint(TCP_REQUEST, &mut output)?;
    write_varint(address.len() as u64, &mut output)?;
    output.extend_from_slice(address);
    write_varint(padding.len() as u64, &mut output)?;
    output.extend_from_slice(padding);
    Ok(output)
}

#[derive(Debug, PartialEq, Eq)]
pub struct TcpResponse<'a> {
    pub accepted: bool,
    pub message: &'a [u8],
    pub consumed: usize,
}

pub fn parse_tcp_response(input: &[u8]) -> Result<TcpResponse<'_>, Error> {
    let status = *input.first().ok_or(Error::Incomplete)?;
    if status > 1 {
        return Err(Error::Invalid("invalid TCP response status"));
    }
    let (message, message_end) = field(&input[1..], MAX_MESSAGE, false)?;
    let (_, padding_end) = field(&input[1 + message_end..], MAX_PADDING, false)?;
    Ok(TcpResponse {
        accepted: status == 0,
        message,
        consumed: 1 + message_end + padding_end,
    })
}

pub fn encode_tcp_response(
    accepted: bool,
    message: &[u8],
    padding: &[u8],
) -> Result<Vec<u8>, Error> {
    if message.len() > MAX_MESSAGE || padding.len() > MAX_PADDING {
        return Err(Error::Invalid("invalid TCP response lengths"));
    }
    let mut output = vec![if accepted { 0 } else { 1 }];
    write_varint(message.len() as u64, &mut output)?;
    output.extend_from_slice(message);
    write_varint(padding.len() as u64, &mut output)?;
    output.extend_from_slice(padding);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_boundaries_and_nonminimal_encoding() {
        for n in [
            0,
            63,
            64,
            16383,
            16384,
            1073741823,
            1073741824,
            (1 << 62) - 1,
        ] {
            let mut out = Vec::new();
            write_varint(n, &mut out).unwrap();
            assert_eq!(read_varint(&out), Ok((n, out.len())));
            for end in 0..out.len() {
                assert_eq!(read_varint(&out[..end]), Err(Error::Incomplete));
            }
        }
        assert_eq!(read_varint(&[0x40, 1]), Ok((1, 2)));
        assert!(varint_len(1 << 62).is_err());
    }

    #[test]
    fn headers_parse_at_every_read_boundary_and_preserve_payload() {
        let request = encode_tcp_request(b"[::1]:443", b"padding").unwrap();
        assert_eq!(&request[..2], &[0x44, 0x01]);
        for end in 0..request.len() {
            assert_eq!(parse_tcp_request(&request[..end]), Err(Error::Incomplete));
        }
        let mut framed = request.clone();
        framed.extend_from_slice(b"payload");
        assert_eq!(
            parse_tcp_request(&framed),
            Ok((&b"[::1]:443"[..], request.len()))
        );
        let response = encode_tcp_response(true, b"", b"padding").unwrap();
        for end in 0..response.len() {
            assert_eq!(parse_tcp_response(&response[..end]), Err(Error::Incomplete));
        }
        assert!(parse_tcp_response(&response).unwrap().accepted);
    }

    #[test]
    fn rejects_oversized_lengths_without_waiting_for_the_field() {
        let mut request = vec![0x44, 0x01];
        write_varint((MAX_ADDRESS + 1) as u64, &mut request).unwrap();
        assert!(matches!(
            parse_tcp_request(&request),
            Err(Error::Invalid(_))
        ));
        assert!(encode_tcp_request(b"", b"").is_err());
        assert!(encode_tcp_response(true, b"", &vec![0; MAX_PADDING + 1]).is_err());
        assert!(parse_tcp_response(&[2]).is_err());
    }
}
