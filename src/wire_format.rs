use crate::{
    Error,
    Result,
};
use tokio::io::{
    AsyncRead,
    AsyncReadExt,
    AsyncWrite,
    AsyncWriteExt,
};

/// Wire format utilities for ClickHouse protocol
pub struct WireFormat;

impl WireFormat {
    /// Read a varint-encoded u64
    pub async fn read_varint64<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0;

        loop {
            let byte = reader.read_u8().await?;
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

    /// Write a varint-encoded u64
    pub async fn write_varint64<W: AsyncWrite + Unpin>(
        writer: &mut W,
        mut value: u64,
    ) -> Result<()> {
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;

            if value != 0 {
                byte |= 0x80;
            }

            writer.write_u8(byte).await?;

            if value == 0 {
                break;
            }
        }

        Ok(())
    }

    /// Read a fixed-size value (little-endian)
    pub async fn read_fixed<R: AsyncRead + Unpin + Send, T: FixedSize>(
        reader: &mut R,
    ) -> Result<T> {
        T::read_from(reader).await
    }

    /// Write a fixed-size value (little-endian)
    pub async fn write_fixed<W: AsyncWrite + Unpin + Send, T: FixedSize>(
        writer: &mut W,
        value: T,
    ) -> Result<()> {
        value.write_to(writer).await
    }

    /// Read a length-prefixed string
    pub async fn read_string<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<String> {
        let len = Self::read_varint64(reader).await? as usize;

        if len > 0x00FFFFFF {
            return Err(Error::Protocol(format!(
                "String length too large: {}",
                len
            )));
        }

        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;

        String::from_utf8(buf)
            .map_err(|e| Error::Protocol(format!("Invalid UTF-8: {}", e)))
    }

    /// Write a length-prefixed string
    pub async fn write_string<W: AsyncWrite + Unpin>(
        writer: &mut W,
        value: &str,
    ) -> Result<()> {
        Self::write_varint64(writer, value.len() as u64).await?;
        writer.write_all(value.as_bytes()).await?;
        Ok(())
    }

    /// Read raw bytes of specified length
    pub async fn read_bytes<R: AsyncRead + Unpin>(
        reader: &mut R,
        len: usize,
    ) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;
        Ok(buf)
    }

    /// Write raw bytes
    pub async fn write_bytes<W: AsyncWrite + Unpin>(
        writer: &mut W,
        bytes: &[u8],
    ) -> Result<()> {
        writer.write_all(bytes).await?;
        Ok(())
    }

    /// Skip a string without reading it into memory
    pub async fn skip_string<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<()> {
        let len = Self::read_varint64(reader).await? as usize;

        if len > 0x00FFFFFF {
            return Err(Error::Protocol(format!(
                "String length too large: {}",
                len
            )));
        }

        // Skip bytes
        let mut remaining = len;
        let mut buf = [0u8; 8192];
        while remaining > 0 {
            let to_read = remaining.min(buf.len());
            reader.read_exact(&mut buf[..to_read]).await?;
            remaining -= to_read;
        }

        Ok(())
    }

    /// Write a quoted string for query parameters (1:1 port of C++
    /// WriteQuotedString)
    ///
    /// Format: varint(length) + quoted_string
    /// Special chars escaped: \0, \b, \t, \n, ', \
    ///
    /// Escaping rules:
    /// - \0 → \x00
    /// - \b → \x08
    /// - \t → \\t
    /// - \n → \\n
    /// - '  → \x27
    /// - \  → \\\
    pub async fn write_quoted_string<W: AsyncWrite + Unpin>(
        writer: &mut W,
        value: &str,
    ) -> Result<()> {
        const QUOTED_CHARS: &[u8] = b"\0\x08\t\n'\\";

        // Check if we need escaping (fast path)
        let bytes = value.as_bytes();
        let first_special =
            bytes.iter().position(|&b| QUOTED_CHARS.contains(&b));

        if first_special.is_none() {
            // Fast path: no special characters
            Self::write_varint64(writer, (value.len() + 2) as u64).await?;
            writer.write_all(b"'").await?;
            writer.write_all(bytes).await?;
            writer.write_all(b"'").await?;
            return Ok(());
        }

        // Count special characters for length calculation
        let quoted_count =
            bytes.iter().filter(|&&b| QUOTED_CHARS.contains(&b)).count();

        // Write length: original + 2 quotes + 3 bytes per special char
        let total_len = value.len() + 2 + 3 * quoted_count;
        Self::write_varint64(writer, total_len as u64).await?;

        // Write opening quote
        writer.write_all(b"'").await?;

        // Write string with escaping
        let mut start = 0;
        for (i, &byte) in bytes.iter().enumerate() {
            if QUOTED_CHARS.contains(&byte) {
                // Write chunk before special char
                if i > start {
                    writer.write_all(&bytes[start..i]).await?;
                }

                // Write escape sequence
                writer.write_all(b"\\").await?;
                match byte {
                    b'\0' => writer.write_all(b"x00").await?,
                    b'\x08' => writer.write_all(b"x08").await?,
                    b'\t' => writer.write_all(b"\\t").await?,
                    b'\n' => writer.write_all(b"\\n").await?,
                    b'\'' => writer.write_all(b"x27").await?,
                    b'\\' => writer.write_all(b"\\\\").await?,
                    _ => unreachable!(),
                }

                start = i + 1;
            }
        }

        // Write final chunk
        if start < bytes.len() {
            writer.write_all(&bytes[start..]).await?;
        }

        // Write closing quote
        writer.write_all(b"'").await?;

        Ok(())
    }
}

/// Trait for types that can be read/written as fixed-size values
#[async_trait::async_trait]
pub trait FixedSize: Sized + Send {
    async fn read_from<R: AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> Result<Self>;
    async fn write_to<W: AsyncWrite + Unpin + Send>(
        self,
        writer: &mut W,
    ) -> Result<()>;
}

// Implement FixedSize for primitive types
macro_rules! impl_fixed_size {
    ($type:ty, $read:ident, $write:ident) => {
        #[async_trait::async_trait]
        impl FixedSize for $type {
            async fn read_from<R: AsyncRead + Unpin + Send>(
                reader: &mut R,
            ) -> Result<Self> {
                Ok(reader.$read().await?)
            }

            async fn write_to<W: AsyncWrite + Unpin + Send>(
                self,
                writer: &mut W,
            ) -> Result<()> {
                Ok(writer.$write(self).await?)
            }
        }
    };
}

impl_fixed_size!(u8, read_u8, write_u8);
impl_fixed_size!(u16, read_u16_le, write_u16_le);
impl_fixed_size!(u32, read_u32_le, write_u32_le);
impl_fixed_size!(u64, read_u64_le, write_u64_le);
impl_fixed_size!(i8, read_i8, write_i8);
impl_fixed_size!(i16, read_i16_le, write_i16_le);
impl_fixed_size!(i32, read_i32_le, write_i32_le);
impl_fixed_size!(i64, read_i64_le, write_i64_le);
impl_fixed_size!(f32, read_f32_le, write_f32_le);
impl_fixed_size!(f64, read_f64_le, write_f64_le);

// i128/u128 implementation
#[async_trait::async_trait]
impl FixedSize for i128 {
    async fn read_from<R: AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> Result<Self> {
        Ok(reader.read_i128_le().await?)
    }

    async fn write_to<W: AsyncWrite + Unpin + Send>(
        self,
        writer: &mut W,
    ) -> Result<()> {
        Ok(writer.write_i128_le(self).await?)
    }
}

#[async_trait::async_trait]
impl FixedSize for u128 {
    async fn read_from<R: AsyncRead + Unpin + Send>(
        reader: &mut R,
    ) -> Result<Self> {
        Ok(reader.read_u128_le().await?)
    }

    async fn write_to<W: AsyncWrite + Unpin + Send>(
        self,
        writer: &mut W,
    ) -> Result<()> {
        Ok(writer.write_u128_le(self).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_varint64_encoding() {
        let test_cases =
            vec![0u64, 1, 127, 128, 255, 256, 65535, 65536, u64::MAX];

        for value in test_cases {
            let mut buf = Vec::new();
            WireFormat::write_varint64(&mut buf, value).await.unwrap();

            let mut reader = &buf[..];
            let decoded =
                WireFormat::read_varint64(&mut reader).await.unwrap();

            assert_eq!(value, decoded, "Varint encoding failed for {}", value);
        }
    }

    #[tokio::test]
    async fn test_string_encoding() {
        let test_strings = vec!["", "hello", "мир", "🦀"];

        for s in test_strings {
            let mut buf = Vec::new();
            WireFormat::write_string(&mut buf, s).await.unwrap();

            let mut reader = &buf[..];
            let decoded = WireFormat::read_string(&mut reader).await.unwrap();

            assert_eq!(s, decoded, "String encoding failed for '{}'", s);
        }
    }

    #[tokio::test]
    async fn test_fixed_u32() {
        let value = 0x12345678u32;
        let mut buf = Vec::new();
        WireFormat::write_fixed(&mut buf, value).await.unwrap();

        assert_eq!(buf, vec![0x78, 0x56, 0x34, 0x12]); // Little-endian

        let mut reader = &buf[..];
        let decoded: u32 = WireFormat::read_fixed(&mut reader).await.unwrap();

        assert_eq!(value, decoded);
    }

    #[tokio::test]
    async fn test_fixed_i64() {
        let value = -12345i64;
        let mut buf = Vec::new();
        WireFormat::write_fixed(&mut buf, value).await.unwrap();

        let mut reader = &buf[..];
        let decoded: i64 = WireFormat::read_fixed(&mut reader).await.unwrap();

        assert_eq!(value, decoded);
    }

    #[tokio::test]
    async fn test_fixed_float() {
        let value = 3.14159f32;
        let mut buf = Vec::new();
        WireFormat::write_fixed(&mut buf, value).await.unwrap();

        let mut reader = &buf[..];
        let decoded: f32 = WireFormat::read_fixed(&mut reader).await.unwrap();

        assert!((value - decoded).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_bytes() {
        let data = vec![1u8, 2, 3, 4, 5];
        let mut buf = Vec::new();
        WireFormat::write_bytes(&mut buf, &data).await.unwrap();

        let mut reader = &buf[..];
        let decoded =
            WireFormat::read_bytes(&mut reader, data.len()).await.unwrap();

        assert_eq!(data, decoded);
    }

    #[tokio::test]
    async fn test_write_quoted_string_no_escaping() {
        let mut buf = Vec::new();
        WireFormat::write_quoted_string(&mut buf, "hello").await.unwrap();

        // Length: 7 (5 + 2 quotes)
        // Content: 'hello'
        let mut expected = Vec::new();
        WireFormat::write_varint64(&mut expected, 7).await.unwrap();
        expected.extend_from_slice(b"'hello'");

        assert_eq!(buf, expected);
    }

    #[tokio::test]
    async fn test_write_quoted_string_with_tab() {
        let mut buf = Vec::new();
        WireFormat::write_quoted_string(&mut buf, "a\tb").await.unwrap();

        // Length: original(3) + 2(quotes) + 3(one special char) = 8
        // Content: 'a\\tb'
        let mut expected = Vec::new();
        WireFormat::write_varint64(&mut expected, 8).await.unwrap();
        expected.extend_from_slice(b"'a\\\\tb'");

        assert_eq!(buf, expected);
    }

    #[tokio::test]
    async fn test_write_quoted_string_with_null() {
        let mut buf = Vec::new();
        WireFormat::write_quoted_string(&mut buf, "a\0b").await.unwrap();

        // Length: 3 + 2 + 3 = 8
        // Content: 'a\x00b'
        let mut expected = Vec::new();
        WireFormat::write_varint64(&mut expected, 8).await.unwrap();
        expected.extend_from_slice(b"'a\\x00b'");

        assert_eq!(buf, expected);
    }

    #[tokio::test]
    async fn test_write_quoted_string_all_special_chars() {
        let test_str = "\0\x08\t\n'\\";
        let mut buf = Vec::new();
        WireFormat::write_quoted_string(&mut buf, test_str).await.unwrap();

        // 6 chars, each becomes 4 bytes: 6 + 2 + 3*6 = 26
        let mut expected = Vec::new();
        WireFormat::write_varint64(&mut expected, 26).await.unwrap();
        // \0 → \x00, \b → \x08, \t → \\t, \n → \\n, ' → \x27, \ → \\\
        expected.extend_from_slice(b"'\\x00\\x08\\\\t\\\\n\\x27\\\\\\'");

        assert_eq!(buf, expected);
    }

    #[tokio::test]
    async fn test_write_quoted_string_single_quote() {
        let mut buf = Vec::new();
        WireFormat::write_quoted_string(&mut buf, "a'b").await.unwrap();

        // Length: 3 + 2 + 3 = 8
        // Content: 'a\x27b'
        let mut expected = Vec::new();
        WireFormat::write_varint64(&mut expected, 8).await.unwrap();
        expected.extend_from_slice(b"'a\\x27b'");

        assert_eq!(buf, expected);
    }

    #[tokio::test]
    async fn test_write_quoted_string_backslash() {
        let mut buf = Vec::new();
        WireFormat::write_quoted_string(&mut buf, "a\\b").await.unwrap();

        // Length: 3 + 2 + 3 = 8
        // Content: 'a\\\b' (backslash becomes \\\ which is 3 backslashes)
        let mut expected = Vec::new();
        WireFormat::write_varint64(&mut expected, 8).await.unwrap();
        expected.extend_from_slice(b"'a\\\\\\b'");

        assert_eq!(buf, expected);
    }

    #[tokio::test]
    async fn test_write_quoted_string_utf8() {
        let mut buf = Vec::new();
        WireFormat::write_quoted_string(&mut buf, "utf8Русский")
            .await
            .unwrap();

        // UTF-8 doesn't need escaping unless it contains special chars
        let content = "utf8Русский";
        let expected_len = content.len() + 2;
        let mut expected = Vec::new();
        WireFormat::write_varint64(&mut expected, expected_len as u64)
            .await
            .unwrap();
        expected.push(b'\'');
        expected.extend_from_slice(content.as_bytes());
        expected.push(b'\'');

        assert_eq!(buf, expected);
    }
}
