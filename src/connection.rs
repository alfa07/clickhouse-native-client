use crate::{
    wire_format::WireFormat,
    Error,
    Result,
};
use bytes::Bytes;
use std::time::Duration;
use tokio::{
    io::{
        AsyncRead,
        AsyncReadExt,
        AsyncWrite,
        AsyncWriteExt,
        BufReader,
        BufWriter,
    },
    net::TcpStream,
};

#[cfg(feature = "tls")]
use rustls::ServerName;
#[cfg(feature = "tls")]
use std::sync::Arc;
#[cfg(feature = "tls")]
use tokio_rustls::TlsConnector;

/// Default buffer sizes for reading and writing
const DEFAULT_READ_BUFFER_SIZE: usize = 8192;
const DEFAULT_WRITE_BUFFER_SIZE: usize = 8192;

/// Connection timeout and TCP options
#[derive(Clone, Debug)]
pub struct ConnectionOptions {
    /// Connection timeout (default: 5 seconds)
    pub connect_timeout: Duration,
    /// Receive timeout (0 = no timeout)
    pub recv_timeout: Duration,
    /// Send timeout (0 = no timeout)
    pub send_timeout: Duration,
    /// Enable TCP keepalive
    pub tcp_keepalive: bool,
    /// TCP keepalive idle time (default: 60 seconds)
    pub tcp_keepalive_idle: Duration,
    /// TCP keepalive interval (default: 5 seconds)
    pub tcp_keepalive_interval: Duration,
    /// TCP keepalive probe count (default: 3)
    pub tcp_keepalive_count: u32,
    /// Enable TCP_NODELAY (disable Nagle's algorithm)
    pub tcp_nodelay: bool,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            recv_timeout: Duration::ZERO,
            send_timeout: Duration::ZERO,
            tcp_keepalive: false,
            tcp_keepalive_idle: Duration::from_secs(60),
            tcp_keepalive_interval: Duration::from_secs(5),
            tcp_keepalive_count: 3,
            tcp_nodelay: true,
        }
    }
}

impl ConnectionOptions {
    /// Create new connection options
    pub fn new() -> Self {
        Self::default()
    }

    /// Set connection timeout
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set receive timeout
    pub fn recv_timeout(mut self, timeout: Duration) -> Self {
        self.recv_timeout = timeout;
        self
    }

    /// Set send timeout
    pub fn send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = timeout;
        self
    }

    /// Enable TCP keepalive
    pub fn tcp_keepalive(mut self, enabled: bool) -> Self {
        self.tcp_keepalive = enabled;
        self
    }

    /// Set TCP keepalive idle time
    pub fn tcp_keepalive_idle(mut self, duration: Duration) -> Self {
        self.tcp_keepalive_idle = duration;
        self
    }

    /// Set TCP keepalive interval
    pub fn tcp_keepalive_interval(mut self, duration: Duration) -> Self {
        self.tcp_keepalive_interval = duration;
        self
    }

    /// Set TCP keepalive probe count
    pub fn tcp_keepalive_count(mut self, count: u32) -> Self {
        self.tcp_keepalive_count = count;
        self
    }

    /// Enable/disable TCP_NODELAY
    pub fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.tcp_nodelay = enabled;
        self
    }
}

/// Async connection wrapper for TCP/TLS socket
/// This is the async I/O boundary - all socket operations are async
pub struct Connection {
    reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
}

impl Connection {
    /// Create a new connection from a TCP stream
    pub fn new(stream: TcpStream) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);

        Self {
            reader: BufReader::with_capacity(
                DEFAULT_READ_BUFFER_SIZE,
                Box::new(read_half) as Box<dyn AsyncRead + Unpin + Send>,
            ),
            writer: BufWriter::with_capacity(
                DEFAULT_WRITE_BUFFER_SIZE,
                Box::new(write_half) as Box<dyn AsyncWrite + Unpin + Send>,
            ),
        }
    }

    /// Create a new connection from a TLS stream
    #[cfg(feature = "tls")]
    pub fn new_tls(
        stream: tokio_rustls::client::TlsStream<TcpStream>,
    ) -> Self {
        let (read_half, write_half) = tokio::io::split(stream);

        Self {
            reader: BufReader::with_capacity(
                DEFAULT_READ_BUFFER_SIZE,
                Box::new(read_half) as Box<dyn AsyncRead + Unpin + Send>,
            ),
            writer: BufWriter::with_capacity(
                DEFAULT_WRITE_BUFFER_SIZE,
                Box::new(write_half) as Box<dyn AsyncWrite + Unpin + Send>,
            ),
        }
    }

    /// Connect to a ClickHouse server with default options
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        Self::connect_with_options(host, port, &ConnectionOptions::default())
            .await
    }

    /// Connect to a ClickHouse server with custom options
    pub async fn connect_with_options(
        host: &str,
        port: u16,
        options: &ConnectionOptions,
    ) -> Result<Self> {
        let addr = format!("{}:{}", host, port);

        // Apply connection timeout
        let stream = if options.connect_timeout > Duration::ZERO {
            tokio::time::timeout(
                options.connect_timeout,
                TcpStream::connect(&addr),
            )
            .await
            .map_err(|_| {
                Error::Connection(format!(
                    "Connection timeout after {:?} to {}",
                    options.connect_timeout, addr
                ))
            })?
            .map_err(|e| {
                Error::Connection(format!(
                    "Failed to connect to {}: {}",
                    addr, e
                ))
            })?
        } else {
            TcpStream::connect(&addr).await.map_err(|e| {
                Error::Connection(format!(
                    "Failed to connect to {}: {}",
                    addr, e
                ))
            })?
        };

        // Apply TCP_NODELAY
        if options.tcp_nodelay {
            stream.set_nodelay(true).map_err(|e| {
                Error::Connection(format!("Failed to set TCP_NODELAY: {}", e))
            })?;
        }

        // Apply TCP keepalive
        #[cfg(unix)]
        if options.tcp_keepalive {
            use socket2::{
                Socket,
                TcpKeepalive,
            };
            use std::os::unix::io::{
                AsRawFd,
                FromRawFd,
            };

            let socket = unsafe { Socket::from_raw_fd(stream.as_raw_fd()) };

            let mut keepalive =
                TcpKeepalive::new().with_time(options.tcp_keepalive_idle);

            #[cfg(any(target_os = "linux", target_os = "macos"))]
            {
                keepalive =
                    keepalive.with_interval(options.tcp_keepalive_interval);
            }

            // Note: with_retries is not available in socket2 0.5.x
            // TCP_KEEPCNT can be set via raw socket options if needed
            // For now, we rely on system defaults for keepalive retry count

            socket.set_tcp_keepalive(&keepalive).map_err(|e| {
                Error::Connection(format!(
                    "Failed to set TCP keepalive: {}",
                    e
                ))
            })?;

            // Prevent socket from being dropped
            std::mem::forget(socket);
        }

        #[cfg(windows)]
        if options.tcp_keepalive {
            use socket2::{
                Socket,
                TcpKeepalive,
            };
            use std::os::windows::io::{
                AsRawSocket,
                FromRawSocket,
            };

            let socket =
                unsafe { Socket::from_raw_socket(stream.as_raw_socket()) };

            let keepalive = TcpKeepalive::new()
                .with_time(options.tcp_keepalive_idle)
                .with_interval(options.tcp_keepalive_interval);

            socket.set_tcp_keepalive(&keepalive).map_err(|e| {
                Error::Connection(format!(
                    "Failed to set TCP keepalive: {}",
                    e
                ))
            })?;

            // Prevent socket from being dropped
            std::mem::forget(socket);
        }

        Ok(Self::new(stream))
    }

    /// Connect to a ClickHouse server with TLS
    #[cfg(feature = "tls")]
    pub async fn connect_with_tls(
        host: &str,
        port: u16,
        options: &ConnectionOptions,
        ssl_config: Arc<rustls::ClientConfig>,
        server_name: Option<&str>,
    ) -> Result<Self> {
        let addr = format!("{}:{}", host, port);

        // Establish TCP connection first
        let stream = if options.connect_timeout > Duration::ZERO {
            tokio::time::timeout(
                options.connect_timeout,
                TcpStream::connect(&addr),
            )
            .await
            .map_err(|_| {
                Error::Connection(format!(
                    "Connection timeout after {:?} to {}",
                    options.connect_timeout, addr
                ))
            })?
            .map_err(|e| {
                Error::Connection(format!(
                    "Failed to connect to {}: {}",
                    addr, e
                ))
            })?
        } else {
            TcpStream::connect(&addr).await.map_err(|e| {
                Error::Connection(format!(
                    "Failed to connect to {}: {}",
                    addr, e
                ))
            })?
        };

        // Apply TCP_NODELAY
        if options.tcp_nodelay {
            stream.set_nodelay(true).map_err(|e| {
                Error::Connection(format!("Failed to set TCP_NODELAY: {}", e))
            })?;
        }

        // Apply TCP keepalive (same as non-TLS connection)
        #[cfg(unix)]
        if options.tcp_keepalive {
            use socket2::{
                Socket,
                TcpKeepalive,
            };
            use std::os::unix::io::{
                AsRawFd,
                FromRawFd,
            };

            let socket = unsafe { Socket::from_raw_fd(stream.as_raw_fd()) };

            let mut keepalive =
                TcpKeepalive::new().with_time(options.tcp_keepalive_idle);

            #[cfg(any(target_os = "linux", target_os = "macos"))]
            {
                keepalive =
                    keepalive.with_interval(options.tcp_keepalive_interval);
            }

            // Note: with_retries is not available in socket2 0.5.x
            // TCP_KEEPCNT can be set via raw socket options if needed
            // For now, we rely on system defaults for keepalive retry count

            socket.set_tcp_keepalive(&keepalive).map_err(|e| {
                Error::Connection(format!(
                    "Failed to set TCP keepalive: {}",
                    e
                ))
            })?;

            // Prevent socket from being dropped
            std::mem::forget(socket);
        }

        #[cfg(windows)]
        if options.tcp_keepalive {
            use socket2::{
                Socket,
                TcpKeepalive,
            };
            use std::os::windows::io::{
                AsRawSocket,
                FromRawSocket,
            };

            let socket =
                unsafe { Socket::from_raw_socket(stream.as_raw_socket()) };

            let keepalive = TcpKeepalive::new()
                .with_time(options.tcp_keepalive_idle)
                .with_interval(options.tcp_keepalive_interval);

            socket.set_tcp_keepalive(&keepalive).map_err(|e| {
                Error::Connection(format!(
                    "Failed to set TCP keepalive: {}",
                    e
                ))
            })?;

            // Prevent socket from being dropped
            std::mem::forget(socket);
        }

        // Perform TLS handshake
        let connector = TlsConnector::from(ssl_config);
        let server_name_to_use = server_name.unwrap_or(host);

        let domain =
            ServerName::try_from(server_name_to_use).map_err(|e| {
                Error::Connection(format!(
                    "Invalid server name '{}': {}",
                    server_name_to_use, e
                ))
            })?;

        let tls_stream =
            connector.connect(domain, stream).await.map_err(|e| {
                Error::Connection(format!("TLS handshake failed: {}", e))
            })?;

        Ok(Self::new_tls(tls_stream))
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

    pub async fn write_u128(&mut self, value: u128) -> Result<()> {
        Ok(self.writer.write_u128_le(value).await?)
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
