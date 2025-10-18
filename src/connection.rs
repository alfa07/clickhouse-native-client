use crate::wire_format::WireFormat;
use crate::{Error, Result};
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;

/// Default buffer sizes for reading and writing
const DEFAULT_READ_BUFFER_SIZE: usize = 8192;
const DEFAULT_WRITE_BUFFER_SIZE: usize = 8192;

/// Async connection wrapper for TCP socket
/// This is the async I/O boundary - all socket operations are async
pub struct Connection {
    reader: BufReader<tokio::io::ReadHalf<TcpStream>>,
    writer: BufWriter<tokio::io::WriteHalf<TcpStream>>,
}

impl Connection {
    /// Create a new connection from a TCP stream
    pub fn new(stream: TcpStream) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);

        Self {
            reader: BufReader::with_capacity(DEFAULT_READ_BUFFER_SIZE, read_half),
            writer: BufWriter::with_capacity(DEFAULT_WRITE_BUFFER_SIZE, write_half),
        }
    }

    /// Connect to a ClickHouse server
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr).await.map_err(|e| {
            Error::Connection(format!("Failed to connect to {}: {}", addr, e))
        })?;

        // Enable TCP_NODELAY for lower latency
        stream
            .set_nodelay(true)
            .map_err(|e| Error::Connection(format!("Failed to set TCP_NODELAY: {}", e)))?;

        Ok(Self::new(stream))
    }

    /// Read a varint-encoded u64
    pub async fn read_varint(&mut self) -> Result<u64> {
        WireFormat::read_varint64(&mut self.reader).await
    }

    /// Write a varint-encoded u64
    pub async fn write_varint(&mut self, value: u64) -> Result<()> {
        WireFormat::write_varint64(&mut self.writer, value).await
    }

    /// Read a fixed-size value
    pub async fn read_u8(&mut self) -> Result<u8> {
        Ok(self.reader.read_u8().await?)
    }

    pub async fn read_u16(&mut self) -> Result<u16> {
        Ok(self.reader.read_u16_le().await?)
    }

    pub async fn read_u32(&mut self) -> Result<u32> {
        Ok(self.reader.read_u32_le().await?)
    }

    pub async fn read_u64(&mut self) -> Result<u64> {
        Ok(self.reader.read_u64_le().await?)
    }

    pub async fn read_i8(&mut self) -> Result<i8> {
        Ok(self.reader.read_i8().await?)
    }

    pub async fn read_i16(&mut self) -> Result<i16> {
        Ok(self.reader.read_i16_le().await?)
    }

    pub async fn read_i32(&mut self) -> Result<i32> {
        Ok(self.reader.read_i32_le().await?)
    }

    pub async fn read_i64(&mut self) -> Result<i64> {
        Ok(self.reader.read_i64_le().await?)
    }

    /// Write fixed-size values
    pub async fn write_u8(&mut self, value: u8) -> Result<()> {
        Ok(self.writer.write_u8(value).await?)
    }

    pub async fn write_u16(&mut self, value: u16) -> Result<()> {
        Ok(self.writer.write_u16_le(value).await?)
    }

    pub async fn write_u32(&mut self, value: u32) -> Result<()> {
        Ok(self.writer.write_u32_le(value).await?)
    }

    pub async fn write_u64(&mut self, value: u64) -> Result<()> {
        Ok(self.writer.write_u64_le(value).await?)
    }

    pub async fn write_i8(&mut self, value: i8) -> Result<()> {
        Ok(self.writer.write_i8(value).await?)
    }

    pub async fn write_i16(&mut self, value: i16) -> Result<()> {
        Ok(self.writer.write_i16_le(value).await?)
    }

    pub async fn write_i32(&mut self, value: i32) -> Result<()> {
        Ok(self.writer.write_i32_le(value).await?)
    }

    pub async fn write_i64(&mut self, value: i64) -> Result<()> {
        Ok(self.writer.write_i64_le(value).await?)
    }

    /// Read a length-prefixed string
    pub async fn read_string(&mut self) -> Result<String> {
        WireFormat::read_string(&mut self.reader).await
    }

    /// Write a length-prefixed string
    pub async fn write_string(&mut self, s: &str) -> Result<()> {
        WireFormat::write_string(&mut self.writer, s).await
    }

    /// Read exact number of bytes into a buffer
    pub async fn read_bytes(&mut self, len: usize) -> Result<Bytes> {
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf).await?;
        Ok(Bytes::from(buf))
    }

    /// Read bytes into an existing buffer
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.reader.read_exact(buf).await?;
        Ok(())
    }

    /// Write bytes
    pub async fn write_bytes(&mut self, data: &[u8]) -> Result<()> {
        Ok(self.writer.write_all(data).await?)
    }

    /// Flush the write buffer
    pub async fn flush(&mut self) -> Result<()> {
        Ok(self.writer.flush().await?)
    }

    /// Read a complete packet (length-prefixed data)
    /// Returns the packet data without the length prefix
    pub async fn read_packet(&mut self) -> Result<Bytes> {
        let len = self.read_varint().await? as usize;

        if len == 0 {
            return Ok(Bytes::new());
        }

        if len > 0x40000000 {
            // 1GB limit
            return Err(Error::Protocol(format!("Packet too large: {}", len)));
        }

        self.read_bytes(len).await
    }

    /// Write a packet with length prefix
    pub async fn write_packet(&mut self, data: &[u8]) -> Result<()> {
        self.write_varint(data.len() as u64).await?;
        self.write_bytes(data).await?;
        Ok(())
    }

    /// Get access to the underlying reader (for advanced use)
    pub fn reader_mut(&mut self) -> &mut BufReader<tokio::io::ReadHalf<TcpStream>> {
        &mut self.reader
    }

    /// Get access to the underlying writer (for advanced use)
    pub fn writer_mut(&mut self) -> &mut BufWriter<tokio::io::WriteHalf<TcpStream>> {
        &mut self.writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests would require a running ClickHouse server or mock
    // For now, we'll just test constants and basic structure

    #[test]
    fn test_buffer_sizes() {
        assert_eq!(DEFAULT_READ_BUFFER_SIZE, 8192);
        assert_eq!(DEFAULT_WRITE_BUFFER_SIZE, 8192);
    }

    // Integration tests with actual server would go in tests/ directory
}
