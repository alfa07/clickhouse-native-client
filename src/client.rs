use crate::block::Block;
use crate::connection::Connection;
use crate::io::{BlockReader, BlockWriter};
use crate::protocol::{ClientCode, CompressionMethod, ServerCode};
use crate::query::{ClientInfo, Progress, Query, ServerInfo};
use crate::{Error, Result};

/// Client options
#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// Server host
    pub host: String,
    /// Server port
    pub port: u16,
    /// Database name
    pub database: String,
    /// Username
    pub user: String,
    /// Password
    pub password: String,
    /// Compression method
    pub compression: Option<CompressionMethod>,
    /// Client information
    pub client_info: ClientInfo,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 9000,
            database: "default".to_string(),
            user: "default".to_string(),
            password: String::new(),
            compression: Some(CompressionMethod::LZ4),
            client_info: ClientInfo::default(),
        }
    }
}

impl ClientOptions {
    /// Create new client options with host and port
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            ..Default::default()
        }
    }

    /// Set the database
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    /// Set the username
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    /// Set the password
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self
    }

    /// Set compression method
    pub fn compression(mut self, method: Option<CompressionMethod>) -> Self {
        self.compression = method;
        self
    }
}

/// ClickHouse client
pub struct Client {
    conn: Connection,
    server_info: ServerInfo,
    block_reader: BlockReader,
    block_writer: BlockWriter,
    options: ClientOptions,
}

impl Client {
    /// Connect to ClickHouse server
    pub async fn connect(options: ClientOptions) -> Result<Self> {
        let mut conn = Connection::connect(&options.host, options.port).await?;

        // Send hello
        Self::send_hello(&mut conn, &options).await?;

        // Receive hello
        let server_info = Self::receive_hello(&mut conn).await?;

        // Send addendum (quota key) if server supports it
        // DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM = 54458
        if server_info.revision >= 54458 {
            eprintln!("[DEBUG] Sending quota key addendum (empty string)...");
            conn.write_string("").await?;
            conn.flush().await?;
            eprintln!("[DEBUG] Addendum sent");
        }

        // Create block reader/writer with compression
        let mut block_reader = BlockReader::new(server_info.revision);
        let mut block_writer = BlockWriter::new(server_info.revision);

        // Enable compression on both reader and writer
        if let Some(compression) = options.compression {
            block_reader = block_reader.with_compression(compression);
            block_writer = block_writer.with_compression(compression);
        }

        Ok(Self {
            conn,
            server_info,
            block_reader,
            block_writer,
            options,
        })
    }

    /// Send hello packet
    async fn send_hello(conn: &mut Connection, options: &ClientOptions) -> Result<()> {
        eprintln!("[DEBUG] Sending client hello...");
        // Write client hello code
        conn.write_varint(ClientCode::Hello as u64).await?;
        eprintln!("[DEBUG] Sent hello code");

        // Write client name and version
        conn.write_string(&options.client_info.client_name).await?;
        eprintln!("[DEBUG] Sent client name: {}", options.client_info.client_name);
        conn.write_varint(options.client_info.client_version_major)
            .await?;
        conn.write_varint(options.client_info.client_version_minor)
            .await?;
        conn.write_varint(options.client_info.client_revision)
            .await?;
        eprintln!("[DEBUG] Sent version: {}.{}.{}",
            options.client_info.client_version_major,
            options.client_info.client_version_minor,
            options.client_info.client_revision);

        // Write database, user, password
        conn.write_string(&options.database).await?;
        conn.write_string(&options.user).await?;
        conn.write_string(&options.password).await?;
        eprintln!("[DEBUG] Sent credentials");

        conn.flush().await?;
        eprintln!("[DEBUG] Flushed");
        Ok(())
    }

    /// Receive hello packet from server
    async fn receive_hello(conn: &mut Connection) -> Result<ServerInfo> {
        eprintln!("[DEBUG] Reading server hello...");
        let packet_type = conn.read_varint().await?;
        eprintln!("[DEBUG] Got packet type: {}", packet_type);

        if packet_type != ServerCode::Hello as u64 {
            if packet_type == ServerCode::Exception as u64 {
                eprintln!("[DEBUG] Server sent exception!");
                return Err(Error::Protocol("Server returned exception during handshake".to_string()));
            }
            eprintln!("[DEBUG] Unexpected packet type: {}", packet_type);
            return Err(Error::Protocol(format!(
                "Expected Hello packet, got {}",
                packet_type
            )));
        }

        // Read server info
        eprintln!("[DEBUG] Reading server info...");
        let name = conn.read_string().await?;
        eprintln!("[DEBUG] Server name: {}", name);
        let version_major = conn.read_varint().await?;
        let version_minor = conn.read_varint().await?;
        let revision = conn.read_varint().await?;
        eprintln!("[DEBUG] Server version: {}.{}, revision: {}",version_major, version_minor, revision);

        let timezone = if revision >= 54058 {
            eprintln!("[DEBUG] Reading timezone...");
            conn.read_string().await?
        } else {
            String::new()
        };

        let display_name = if revision >= 54372 {
            eprintln!("[DEBUG] Reading display name...");
            conn.read_string().await?
        } else {
            String::new()
        };

        let version_patch = if revision >= 54401 {
            eprintln!("[DEBUG] Reading version patch...");
            conn.read_varint().await?
        } else {
            0
        };

        eprintln!("[DEBUG] Server hello complete!");
        Ok(ServerInfo {
            name,
            version_major,
            version_minor,
            version_patch,
            revision,
            timezone,
            display_name,
        })
    }

    /// Execute a query and return results
    pub async fn query(&mut self, query: impl Into<Query>) -> Result<QueryResult> {
        let query = query.into();

        // Send query
        self.send_query(&query).await?;

        // Receive results
        let mut blocks = Vec::new();
        let mut progress_info = Progress::default();

        loop {
            let packet_type = self.conn.read_varint().await?;
            eprintln!("[DEBUG] Query response packet: {}", packet_type);

            match packet_type {
                code if code == ServerCode::Data as u64 => {
                    eprintln!("[DEBUG] Received data packet");
                    // Skip temp table name if protocol supports it (matches C++ ReceiveData)
                    if self.server_info.revision >= 50264 { // DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES
                        let _temp_table = self.conn.read_string().await?;
                    }
                    let block = self.block_reader.read_block(&mut self.conn).await?;
                    if !block.is_empty() {
                        blocks.push(block);
                    }
                }
                code if code == ServerCode::Progress as u64 => {
                    eprintln!("[DEBUG] Received progress packet");
                    progress_info = self.read_progress().await?;
                }
                code if code == ServerCode::EndOfStream as u64 => {
                    eprintln!("[DEBUG] Received end of stream");
                    break;
                }
                code if code == ServerCode::ProfileInfo as u64 => {
                    eprintln!("[DEBUG] Received profile info packet (ignoring)");
                    // ProfileInfo contains: rows, blocks, bytes, elapsed, rows_before_limit, calculated_rows_before_limit
                    let _rows = self.conn.read_varint().await?;
                    let _blocks = self.conn.read_varint().await?;
                    let _bytes = self.conn.read_varint().await?;
                    let _applied_limit = self.conn.read_u8().await?;
                    let _rows_before_limit = self.conn.read_varint().await?;
                    let _calculated_rows_before_limit = self.conn.read_u8().await?;
                }
                code if code == ServerCode::Log as u64 => {
                    eprintln!("[DEBUG] Received log packet (ignoring)");
                    // Skip string first (log tag)
                    let _log_tag = self.conn.read_string().await?;
                    // Read and discard the log block (sent uncompressed)
                    let uncompressed_reader = BlockReader::new(self.server_info.revision);
                    let _block = uncompressed_reader.read_block(&mut self.conn).await?;
                }
                code if code == ServerCode::ProfileEvents as u64 => {
                    eprintln!("[DEBUG] Received profile events packet (ignoring)");
                    // Skip string first (matches C++ implementation)
                    let _table_name = self.conn.read_string().await?;
                    // Read and discard ProfileEvents block (sent uncompressed)
                    let uncompressed_reader = BlockReader::new(self.server_info.revision);
                    let _block = uncompressed_reader.read_block(&mut self.conn).await?;
                }
                code if code == ServerCode::TableColumns as u64 => {
                    eprintln!("[DEBUG] Received table columns packet (ignoring)");
                    // Skip external table name
                    let _table_name = self.conn.read_string().await?;
                    // Skip columns metadata string
                    let _columns_metadata = self.conn.read_string().await?;
                }
                code if code == ServerCode::Exception as u64 => {
                    eprintln!("[DEBUG] Server returned exception during query, reading details...");
                    let exception = self.read_exception().await?;
                    eprintln!("[DEBUG] Exception: code={}, name={}, msg={}",
                        exception.code, exception.name, exception.display_text);
                    return Err(Error::Protocol(format!(
                        "ClickHouse exception: {} ({}): {}",
                        exception.name, exception.code, exception.display_text
                    )));
                }
                other => {
                    eprintln!("[DEBUG] Unexpected packet type: {}", other);
                    return Err(Error::Protocol(format!("Unexpected packet type: {}", other)));
                }
            }
        }

        Ok(QueryResult {
            blocks,
            progress: progress_info,
        })
    }

    /// Send a query packet
    async fn send_query(&mut self, query: &Query) -> Result<()> {
        eprintln!("[DEBUG] Sending query: {}", query.text());
        // Write query code
        self.conn.write_varint(ClientCode::Query as u64).await?;

        // Write query ID
        self.conn.write_string(query.id()).await?;
        eprintln!("[DEBUG] Sent query ID");

        // Client info
        let revision = self.server_info.revision;
        if revision >= 54032 {
            eprintln!("[DEBUG] Writing client info...");
            let info = &self.options.client_info;

            // Write client info fields in the correct order
            self.conn.write_u8(1).await?; // query_kind = 1 (initial query)
            self.conn.write_string(&info.initial_user).await?;
            self.conn.write_string(&info.initial_query_id).await?;
            self.conn.write_string("127.0.0.1:0").await?; // initial_address (client address:port)

            if revision >= 54449 {
                self.conn.write_i64(0).await?; // initial_query_start_time
            }

            self.conn.write_u8(info.interface_type).await?; // interface type (1 = TCP)
            self.conn.write_string(&info.os_user).await?;
            self.conn.write_string(&info.client_hostname).await?;
            self.conn.write_string(&info.client_name).await?;
            self.conn.write_varint(info.client_version_major).await?;
            self.conn.write_varint(info.client_version_minor).await?;
            self.conn.write_varint(info.client_revision).await?;

            if revision >= 54060 {
                self.conn.write_string(&info.quota_key).await?;
            }
            if revision >= 54448 {
                self.conn.write_varint(0).await?; // distributed_depth
            }
            if revision >= 54401 {
                self.conn.write_varint(info.client_version_patch).await?;
            }
            if revision >= 54442 {
                self.conn.write_u8(0).await?; // no OpenTelemetry
            }
            if revision >= 54453 {
                self.conn.write_varint(0).await?; // collaborate_with_initiator
                self.conn.write_varint(0).await?; // count_participating_replicas
                self.conn.write_varint(0).await?; // number_of_current_replica
            }

            eprintln!("[DEBUG] Client info sent");
        }

        // Settings
        if revision >= 54429 {
            eprintln!("[DEBUG] Writing settings...");
            for (key, value) in query.settings() {
                self.conn.write_string(key).await?;
                self.conn.write_varint(0).await?; // flags = 0 (no special flags)
                self.conn.write_string(value).await?;
            }
        }
        // Empty string to mark end of settings
        self.conn.write_string("").await?;
        eprintln!("[DEBUG] Settings sent");

        // Interserver secret (for servers >= 54441)
        if revision >= 54441 {
            self.conn.write_string("").await?; // empty interserver secret
        }

        // Query stage, compression, text
        eprintln!("[DEBUG] Writing query stage and text...");
        self.conn.write_varint(2).await?; // Stage = Complete
        // Enable compression if we have it configured
        let compression_enabled = if self.options.compression.is_some() { 1u64 } else { 0u64 };
        self.conn.write_varint(compression_enabled).await?;
        self.conn.write_string(query.text()).await?;

        // Query parameters (for servers >= 54459)
        if revision >= 54459 {
            for (key, value) in query.parameters() {
                self.conn.write_string(key).await?;
                self.conn.write_varint(2).await?; // Custom type
                self.conn.write_string(value).await?;
            }
            // Empty string to mark end of parameters
            self.conn.write_string("").await?;
        }

        // Send empty block to finalize query (as per C++ client)
        // This block must respect the compression setting we told the server
        eprintln!("[DEBUG] Sending empty block to finalize...");
        self.conn.write_varint(ClientCode::Data as u64).await?;
        let empty_block = Block::new();
        // Create writer that matches the compression setting
        let writer = if self.options.compression.is_some() {
            BlockWriter::new(self.server_info.revision)
                .with_compression(self.options.compression.unwrap())
        } else {
            BlockWriter::new(self.server_info.revision)
        };
        writer.write_block(&mut self.conn, &empty_block).await?;

        self.conn.flush().await?;
        eprintln!("[DEBUG] Query sent, waiting for response...");
        Ok(())
    }

    /// Read progress info
    async fn read_progress(&mut self) -> Result<Progress> {
        let rows = self.conn.read_varint().await?;
        let bytes = self.conn.read_varint().await?;
        let total_rows = self.conn.read_varint().await?;

        let (written_rows, written_bytes) = if self.server_info.revision >= 54405 {
            (
                self.conn.read_varint().await?,
                self.conn.read_varint().await?,
            )
        } else {
            (0, 0)
        };

        Ok(Progress {
            rows,
            bytes,
            total_rows,
            written_rows,
            written_bytes,
        })
    }

    /// Read exception from server
    fn read_exception<'a>(&'a mut self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::query::Exception>> + 'a>> {
        use crate::query::Exception;
        Box::pin(async move {
            eprintln!("[DEBUG] Reading exception code...");
            let code = self.conn.read_i32().await?;
            eprintln!("[DEBUG] Exception code: {}", code);
            eprintln!("[DEBUG] Reading exception name...");
            let name = self.conn.read_string().await?;
            eprintln!("[DEBUG] Exception name: {}", name);
            eprintln!("[DEBUG] Reading exception display_text...");
            let display_text = self.conn.read_string().await?;
            eprintln!("[DEBUG] Exception display_text length: {}", display_text.len());
            eprintln!("[DEBUG] Reading exception stack_trace...");
            let stack_trace = self.conn.read_string().await?;
            eprintln!("[DEBUG] Exception stack_trace length: {}", stack_trace.len());

            // Check for nested exception
            let has_nested = self.conn.read_u8().await?;
            let nested = if has_nested != 0 {
                Some(Box::new(self.read_exception().await?))
            } else {
                None
            };

            Ok(Exception {
                code,
                name,
                display_text,
                stack_trace,
                nested,
            })
        })
    }

    /// Insert data into a table
    ///
    /// This method constructs an INSERT query from the block's column names and sends the data.
    /// Example: `client.insert("my_database.my_table", block).await?`
    pub async fn insert(&mut self, table_name: &str, block: Block) -> Result<()> {
        // Build query with column names from block (matches C++ implementation)
        let col_names: Vec<String> = (0..block.column_count())
            .filter_map(|i| block.column_name(i))
            .map(|n| format!("`{}`", n.replace("`", "``"))) // Escape backticks
            .collect();

        if col_names.is_empty() {
            return Err(Error::Protocol("Block has no columns".to_string()));
        }

        let query_text = format!(
            "INSERT INTO {} ({}) VALUES",
            table_name,
            col_names.join(", ")
        );

        eprintln!("[DEBUG] Sending INSERT query: {}", query_text);
        let query = Query::new(query_text);

        // Send query
        self.send_query(&query).await?;

        // Wait for server to respond with Data packet (matches C++ Insert flow)
        eprintln!("[DEBUG] Waiting for server Data packet...");
        loop {
            let packet_type = self.conn.read_varint().await?;
            eprintln!("[DEBUG] INSERT wait response packet type: {}", packet_type);

            match packet_type {
                code if code == ServerCode::Data as u64 => {
                    eprintln!("[DEBUG] Received Data packet, ready to send data");
                    // CRITICAL: Must consume the Data packet's payload to keep stream aligned!
                    // Skip temp table name
                    if self.server_info.revision >= 50264 {
                        let _temp_table = self.conn.read_string().await?;
                    }
                    // Read the block (likely empty, but must consume it)
                    let _block = self.block_reader.read_block(&mut self.conn).await?;
                    eprintln!("[DEBUG] Consumed Data packet payload, stream aligned");
                    break;
                }
                code if code == ServerCode::Progress as u64 => {
                    eprintln!("[DEBUG] Received Progress packet");
                    let _ = self.read_progress().await?;
                }
                code if code == ServerCode::TableColumns as u64 => {
                    eprintln!("[DEBUG] Received TableColumns packet");
                    // Skip external table name
                    let _table_name = self.conn.read_string().await?;
                    // Skip columns metadata string
                    let _columns_metadata = self.conn.read_string().await?;
                }
                code if code == ServerCode::Exception as u64 => {
                    eprintln!("[DEBUG] Server returned exception before accepting data");
                    let exception = self.read_exception().await?;
                    return Err(Error::Protocol(format!(
                        "ClickHouse exception: {} (code {}): {}",
                        exception.name, exception.code, exception.display_text
                    )));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "Unexpected packet type while waiting for Data: {}",
                        other
                    )));
                }
            }
        }

        // Now send our data block
        eprintln!("[DEBUG] Sending data block with {} rows", block.row_count());
        self.conn.write_varint(ClientCode::Data as u64).await?;
        self.block_writer
            .write_block(&mut self.conn, &block)
            .await?;

        // Send empty block to signal end
        eprintln!("[DEBUG] Sending empty block to signal end");
        let empty_block = Block::new();
        self.conn.write_varint(ClientCode::Data as u64).await?;
        self.block_writer
            .write_block(&mut self.conn, &empty_block)
            .await?;

        // Wait for EndOfStream (matches C++ flow)
        eprintln!("[DEBUG] Waiting for EndOfStream...");
        loop {
            let packet_type = self.conn.read_varint().await?;
            eprintln!("[DEBUG] INSERT final response packet type: {}", packet_type);

            match packet_type {
                code if code == ServerCode::EndOfStream as u64 => {
                    eprintln!("[DEBUG] Received EndOfStream, insert complete");
                    break;
                }
                code if code == ServerCode::Data as u64 => {
                    eprintln!("[DEBUG] Received Data packet in INSERT response (skipping)");
                    // Skip temp table name if protocol supports it
                    if self.server_info.revision >= 50264 {
                        let _temp_table = self.conn.read_string().await?;
                    }
                    // Read and discard the block
                    let _block = self.block_reader.read_block(&mut self.conn).await?;
                }
                code if code == ServerCode::Progress as u64 => {
                    eprintln!("[DEBUG] Received Progress packet");
                    let _ = self.read_progress().await?;
                }
                code if code == ServerCode::ProfileEvents as u64 => {
                    eprintln!("[DEBUG] Received ProfileEvents packet (skipping)");
                    let _table_name = self.conn.read_string().await?;
                    let uncompressed_reader = BlockReader::new(self.server_info.revision);
                    let _block = uncompressed_reader.read_block(&mut self.conn).await?;
                }
                code if code == ServerCode::TableColumns as u64 => {
                    eprintln!("[DEBUG] Received TableColumns packet (skipping)");
                    let _table_name = self.conn.read_string().await?;
                    let _columns_metadata = self.conn.read_string().await?;
                }
                code if code == ServerCode::Exception as u64 => {
                    eprintln!("[DEBUG] Server returned exception after sending data");
                    let exception = self.read_exception().await?;
                    return Err(Error::Protocol(format!(
                        "ClickHouse exception: {} (code {}): {}",
                        exception.name, exception.code, exception.display_text
                    )));
                }
                _ => {
                    eprintln!("[DEBUG] WARNING: Ignoring unexpected packet type: {} - stream may be misaligned", packet_type);
                }
            }
        }

        Ok(())
    }

    /// Ping the server
    pub async fn ping(&mut self) -> Result<()> {
        eprintln!("[DEBUG] Sending ping...");
        self.conn.write_varint(ClientCode::Ping as u64).await?;
        self.conn.flush().await?;
        eprintln!("[DEBUG] Ping sent, waiting for pong...");

        let packet_type = self.conn.read_varint().await?;
        eprintln!("[DEBUG] Got response packet type: {}", packet_type);

        if packet_type == ServerCode::Pong as u64 {
            eprintln!("[DEBUG] Pong received!");
            Ok(())
        } else {
            eprintln!("[DEBUG] Unexpected packet: {}", packet_type);
            Err(Error::Protocol(format!(
                "Expected Pong, got {}",
                packet_type
            )))
        }
    }

    /// Get server info
    pub fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }
}

/// Query result
pub struct QueryResult {
    /// Result blocks
    pub blocks: Vec<Block>,
    /// Progress information
    pub progress: Progress,
}

impl QueryResult {
    /// Get all blocks
    pub fn blocks(&self) -> &[Block] {
        &self.blocks
    }

    /// Get progress info
    pub fn progress(&self) -> &Progress {
        &self.progress
    }

    /// Get total number of rows across all blocks
    pub fn total_rows(&self) -> usize {
        self.blocks.iter().map(|b| b.row_count()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_options_default() {
        let opts = ClientOptions::default();
        assert_eq!(opts.host, "localhost");
        assert_eq!(opts.port, 9000);
        assert_eq!(opts.database, "default");
    }

    #[test]
    fn test_client_options_builder() {
        let opts = ClientOptions::new("127.0.0.1", 9000)
            .database("test_db")
            .user("test_user")
            .password("test_pass");

        assert_eq!(opts.host, "127.0.0.1");
        assert_eq!(opts.database, "test_db");
        assert_eq!(opts.user, "test_user");
        assert_eq!(opts.password, "test_pass");
    }

    #[test]
    fn test_query_result() {
        let result = QueryResult {
            blocks: vec![],
            progress: Progress::default(),
        };

        assert_eq!(result.total_rows(), 0);
    }
}
