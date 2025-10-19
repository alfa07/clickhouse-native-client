//! Buffer utilities for synchronous varint and string encoding/decoding
//!
//! These utilities work on in-memory byte slices and are used for parsing
//! compressed block data and constructing query packets.

use crate::{
    Error,
    Result,
};
use bytes::{
    Buf,
    BufMut,
    BytesMut,
};

/// Read a varint-encoded u64 from a byte slice
///
/// This is the synchronous version used for parsing in-memory buffers.
/// For async I/O, use `WireFormat::read_varint64` instead.
pub fn read_varint(buffer: &mut &[u8]) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;

    loop {
        if buffer.is_empty() {
            return Err(Error::Protocol(
                "Unexpected end of buffer reading varint".to_string(),
            ));
        }

        let byte = buffer[0];
        buffer.advance(1);

        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift >= 64 {
            return Err(Error::Protocol("Varint overflow".to_string()));
        }
    }

    Ok(result)
}

/// Write a varint-encoded u64 to a byte buffer
///
/// This is the synchronous version used for constructing in-memory buffers.
/// For async I/O, use `WireFormat::write_varint64` instead.
pub fn write_varint(buffer: &mut BytesMut, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buffer.put_u8(byte);

        if value == 0 {
            break;
        }
    }
}

/// Read a length-prefixed string from a byte slice
///
/// This is the synchronous version used for parsing in-memory buffers.
/// For async I/O, use `WireFormat::read_string` instead.
pub fn read_string(buffer: &mut &[u8]) -> Result<String> {
    let len = read_varint(buffer)? as usize;

    if buffer.len() < len {
        return Err(Error::Protocol(format!(
            "Not enough data for string: need {}, have {}",
            len,
            buffer.len()
        )));
    }

    let string_data = &buffer[..len];
    let s = String::from_utf8(string_data.to_vec()).map_err(|e| {
        Error::Protocol(format!("Invalid UTF-8 in string: {}", e))
    })?;

    buffer.advance(len);
    Ok(s)
}

/// Write a length-prefixed string to a byte buffer
///
/// This is the synchronous version used for constructing in-memory buffers.
/// For async I/O, use `WireFormat::write_string` instead.
pub fn write_string(buffer: &mut BytesMut, s: &str) {
    write_varint(buffer, s.len() as u64);
    buffer.put_slice(s.as_bytes());
}

/// Write a varint to a raw Vec<u8> (convenience for tests)
pub fn write_varint_to_vec(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_roundtrip() {
        let test_cases =
            vec![0u64, 1, 127, 128, 255, 256, 65535, 65536, u64::MAX];

        for value in test_cases {
            let mut buf = BytesMut::new();
            write_varint(&mut buf, value);

            let mut slice = &buf[..];
            let decoded = read_varint(&mut slice).unwrap();

            assert_eq!(
                value, decoded,
                "Varint roundtrip failed for {}",
                value
            );
            assert!(slice.is_empty(), "Buffer should be fully consumed");
        }
    }

    #[test]
    fn test_string_roundtrip() {
        let test_strings =
            vec!["", "hello", "мир", "🦀", "test\nwith\nnewlines"];

        for s in test_strings {
            let mut buf = BytesMut::new();
            write_string(&mut buf, s);

            let mut slice = &buf[..];
            let decoded = read_string(&mut slice).unwrap();

            assert_eq!(s, decoded, "String roundtrip failed for '{}'", s);
            assert!(slice.is_empty(), "Buffer should be fully consumed");
        }
    }

    #[test]
    fn test_varint_overflow() {
        // Create an invalid varint that would overflow
        let mut buf = BytesMut::new();
        for _ in 0..10 {
            buf.put_u8(0xFF); // All continuation bits set
        }

        let mut slice = &buf[..];
        let result = read_varint(&mut slice);
        assert!(result.is_err());
    }

    #[test]
    fn test_string_truncated() {
        let mut buf = BytesMut::new();
        write_varint(&mut buf, 100); // Say we have 100 bytes
        buf.put_slice(b"only10"); // But only provide 6

        let mut slice = &buf[..];
        let result = read_string(&mut slice);
        assert!(result.is_err());
    }

    #[test]
    fn test_varint_to_vec() {
        let mut buf = Vec::new();
        write_varint_to_vec(&mut buf, 300);

        let mut slice = &buf[..];
        let decoded = read_varint(&mut slice).unwrap();
        assert_eq!(decoded, 300);
    }
}
