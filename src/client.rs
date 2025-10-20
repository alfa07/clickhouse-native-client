use crate::{
    block::Block,
    connection::{
        Connection,
        ConnectionOptions,
    },
    io::{
        BlockReader,
        BlockWriter,
    },
    protocol::{
        ClientCode,
        CompressionMethod,
        ServerCode,
    },
    query::{
        ClientInfo,
        Profile,
        Progress,
        Query,
        ServerInfo,
    },
    Error,
    Result,
};
use std::time::Duration;

#[cfg(feature = "tls")]
use crate::ssl::SSLOptions;

/// Endpoint configuration (host + port)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    /// Server host
    pub host: String,
    /// Server port
    pub port: u16,
}

impl Endpoint {
    /// Create a new endpoint
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self { host: host.into(), port }
    }
}

/// Client options
#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// Server host (used if endpoints is empty)
    pub host: String,
    /// Server port (used if endpoints is empty)
    pub port: u16,
    /// Multiple endpoints for failover (if empty, uses host+port)
    pub endpoints: Vec<Endpoint>,
    /// Database name
    pub database: String,
    /// Username
    pub user: String,
    /// Password
    pub password: String,
    /// Compression method
    pub compression: Option<CompressionMethod>,
    /// Maximum compression chunk size (default: 65535)
    pub max_compression_chunk_size: usize,
    /// Client information
    pub client_info: ClientInfo,
    /// Connection timeout and TCP options
    pub connection_options: ConnectionOptions,
    /// SSL/TLS options (requires 'tls' feature)
    #[cfg(feature = "tls")]
    pub ssl_options: Option<SSLOptions>,
    /// Number of send retries (default: 1, no retry)
    pub send_retries: u32,
    /// Timeout between retry attempts (default: 5 seconds)
    pub retry_timeout: Duration,
    /// Send ping before each query (default: false)
    pub ping_before_query: bool,
    /// Rethrow server exceptions (default: true)
    pub rethrow_exceptions: bool,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 9000,
            endpoints: Vec::new(),
            database: "default".to_string(),
            user: "default".to_string(),
            password: String::new(),
            compression: Some(CompressionMethod::Lz4),
            max_compression_chunk_size: 65535,
            client_info: ClientInfo::default(),
            connection_options: ConnectionOptions::default(),
            #[cfg(feature = "tls")]
            ssl_options: None,
            send_retries: 1,
            retry_timeout: Duration::from_secs(5),
            ping_before_query: false,
            rethrow_exceptions: true,
        }
    }
}

impl ClientOptions {
    /// Create new client options with host and port
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self { host: host.into(), port, ..Default::default() }
    }

    /// Set multiple endpoints for failover
    pub fn endpoints(mut self, endpoints: Vec<Endpoint>) -> Self {
        self.endpoints = endpoints;
        self
    }

    /// Add an endpoint for failover
    pub fn add_endpoint(mut self, host: impl Into<String>, port: u16) -> Self {
        self.endpoints.push(Endpoint::new(host, port));
        self
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

    /// Set maximum compression chunk size
    pub fn max_compression_chunk_size(mut self, size: usize) -> Self {
        self.max_compression_chunk_size = size;
        self
    }

    /// Set connection options (timeouts, TCP settings)
    pub fn connection_options(mut self, options: ConnectionOptions) -> Self {
        self.connection_options = options;
        self
    }

    /// Set number of send retries
    pub fn send_retries(mut self, retries: u32) -> Self {
        self.send_retries = retries;
        self
    }

    /// Set retry timeout
    pub fn retry_timeout(mut self, timeout: Duration) -> Self {
        self.retry_timeout = timeout;
        self
    }

    /// Enable/disable ping before query
    pub fn ping_before_query(mut self, enabled: bool) -> Self {
        self.ping_before_query = enabled;
        self
    }

    /// Enable/disable exception rethrowing
    pub fn rethrow_exceptions(mut self, enabled: bool) -> Self {
        self.rethrow_exceptions = enabled;
        self
    }

    /// Set SSL/TLS options (requires 'tls' feature)
    #[cfg(feature = "tls")]
    pub fn ssl_options(mut self, options: SSLOptions) -> Self {
        self.ssl_options = Some(options);
        self
    }

    /// Get all endpoints (including host+port if endpoints is empty)
    pub(crate) fn get_endpoints(&self) -> Vec<Endpoint> {
        if self.endpoints.is_empty() {
            vec![Endpoint::new(&self.host, self.port)]
        } else {
            self.endpoints.clone()
        }
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
    /// Connect to ClickHouse server with retry and endpoint failover
    pub async fn connect(options: ClientOptions) -> Result<Self> {
        let endpoints = options.get_endpoints();
        let mut last_error = None;

        // Try each endpoint with retries
        for endpoint in &endpoints {
            for attempt in 0..options.send_retries {
                match Self::try_connect(
                    &endpoint.host,
                    endpoint.port,
                    &options,
                )
                .await
                {
                    Ok(client) => return Ok(client),
                    Err(e) => {
                        last_error = Some(e);

                        // Wait before retry (except for last attempt)
                        if attempt + 1 < options.send_retries {
                            tokio::time::sleep(options.retry_timeout).await;
                        }
                    }
                }
            }
        }

        // All endpoints and retries failed
        Err(last_error.unwrap_or_else(|| {
            Error::Connection("No endpoints available".to_string())
        }))
    }

    /// Try to connect to a specific endpoint
    async fn try_connect(
        host: &str,
        port: u16,
        options: &ClientOptions,
    ) -> Result<Self> {
        // Connect with or without TLS based on options
        let mut conn = {
            #[cfg(feature = "tls")]
            {
                if let Some(ref ssl_opts) = options.ssl_options {
                    // Build SSL client config
                    let ssl_config = ssl_opts.build_client_config()?;

                    // Use server name from SSL options if provided, otherwise
                    // use host
                    let server_name = ssl_opts
                        .server_name
                        .as_deref()
                        .or(if ssl_opts.use_sni { Some(host) } else { None });

                    Connection::connect_with_tls(
                        host,
                        port,
                        &options.connection_options,
                        ssl_config,
                        server_name,
                    )
                    .await?
                } else {
                    Connection::connect_with_options(
                        host,
                        port,
                        &options.connection_options,
                    )
                    .await?
                }
            }
            #[cfg(not(feature = "tls"))]
            {
                Connection::connect_with_options(
                    host,
                    port,
                    &options.connection_options,
                )
                .await?
            }
        };

        // Send hello
        Self::send_hello(&mut conn, options).await?;

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
            options: options.clone(),
        })
    }

    /// Send hello packet
    async fn send_hello(
        conn: &mut Connection,
        options: &ClientOptions,
    ) -> Result<()> {
        eprintln!("[DEBUG] Sending client hello...");
        // Write client hello code
        conn.write_varint(ClientCode::Hello as u64).await?;
        eprintln!("[DEBUG] Sent hello code");

        // Write client name and version
        conn.write_string(&options.client_info.client_name).await?;
        eprintln!(
            "[DEBUG] Sent client name: {}",
            options.client_info.client_name
        );
        conn.write_varint(options.client_info.client_version_major).await?;
        conn.write_varint(options.client_info.client_version_minor).await?;
        conn.write_varint(options.client_info.client_revision).await?;
        eprintln!(
            "[DEBUG] Sent version: {}.{}.{}",
            options.client_info.client_version_major,
            options.client_info.client_version_minor,
            options.client_info.client_revision
        );

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
                return Err(Error::Protocol(
                    "Server returned exception during handshake".to_string(),
                ));
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
        eprintln!(
            "[DEBUG] Server version: {}.{}, revision: {}",
            version_major, version_minor, revision
        );

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

    /// Execute a DDL/DML query without returning data
    ///
    /// Use this for queries that don't return result sets:
    /// - CREATE/DROP TABLE, DATABASE
    /// - ALTER TABLE
    /// - TRUNCATE
    /// - Other DDL/DML operations
    ///
    /// For SELECT queries, use `query()` instead.
    /// For query tracing, use `execute_with_id()`.
    ///
    /// # Example
    /// ```no_run
    /// # use clickhouse_client::{Client, ClientOptions};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut client = Client::connect(ClientOptions::default()).await?;
    /// client.execute("CREATE TABLE test (id UInt32) ENGINE = Memory").await?;
    /// client.execute("DROP TABLE test").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(&mut self, query: impl Into<Query>) -> Result<()> {
        self.execute_with_id(query, "").await
    }

    /// Execute a DDL/DML query with a specific query ID
    ///
    /// The query ID is useful for query tracing and debugging.
    ///
    /// # Example
    /// ```no_run
    /// # use clickhouse_client::{Client, ClientOptions};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut client = Client::connect(ClientOptions::default()).await?;
    /// client.execute_with_id("CREATE TABLE test (id UInt32) ENGINE = Memory", "create-123").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_with_id(&mut self, query: impl Into<Query>, query_id: &str) -> Result<()> {
        let mut query = query.into();
        if !query_id.is_empty() {
            query = Query::new(query.text()).with_query_id(query_id);
        }
        self.send_query(&query).await?;

        // Read responses until EndOfStream, but don't collect blocks
        loop {
            let packet_type = self.conn.read_varint().await?;

            match packet_type {
                code if code == ServerCode::Data as u64 => {
                    // Skip data blocks (shouldn't happen for DDL, but handle gracefully)
                    if self.server_info.revision >= 50264 {
                        let _temp_table = self.conn.read_string().await?;
                    }
                    let _block = self.block_reader.read_block(&mut self.conn).await?;
                }
                code if code == ServerCode::Progress as u64 => {
                    let progress = self.read_progress().await?;

                    // Invoke progress callback if present
                    if let Some(callback) = query.get_on_progress() {
                        callback(&progress);
                    }
                }
                code if code == ServerCode::EndOfStream as u64 => {
                    break;
                }
                code if code == ServerCode::Exception as u64 => {
                    let exception = self.read_exception().await?;

                    // Invoke exception callback if present
                    if let Some(callback) = query.get_on_exception() {
                        callback(&exception);
                    }

                    return Err(Error::Protocol(format!(
                        "ClickHouse exception: {} (code {}): {}",
                        exception.name, exception.code, exception.display_text
                    )));
                }
                code if code == ServerCode::ProfileInfo as u64 => {
                    // Read profile info
                    let rows = self.conn.read_varint().await?;
                    let blocks = self.conn.read_varint().await?;
                    let bytes = self.conn.read_varint().await?;
                    let applied_limit = self.conn.read_u8().await?;
                    let rows_before_limit = self.conn.read_varint().await?;
                    let calculated = self.conn.read_u8().await?;

                    let profile = Profile {
                        rows,
                        blocks,
                        bytes,
                        applied_limit: applied_limit != 0,
                        rows_before_limit,
                        calculated_rows_before_limit: calculated != 0,
                    };

                    // Invoke profile callback if present
                    if let Some(callback) = query.get_on_profile() {
                        callback(&profile);
                    }
                }
                code if code == ServerCode::Log as u64 => {
                    let _log_tag = self.conn.read_string().await?;
                    // Log blocks are sent uncompressed
                    let uncompressed_reader =
                        BlockReader::new(self.server_info.revision);
                    let block =
                        uncompressed_reader.read_block(&mut self.conn).await?;

                    // Invoke server log callback if present
                    if let Some(callback) = query.get_on_server_log() {
                        callback(&block);
                    }
                }
                code if code == ServerCode::ProfileEvents as u64 => {
                    let _table_name = self.conn.read_string().await?;
                    // ProfileEvents blocks are sent uncompressed
                    let uncompressed_reader =
                        BlockReader::new(self.server_info.revision);
                    let block =
                        uncompressed_reader.read_block(&mut self.conn).await?;

                    // Invoke profile events callback if present
                    if let Some(callback) = query.get_on_profile_events() {
                        callback(&block);
                    }
                }
                code if code == ServerCode::TableColumns as u64 => {
                    let _table_name = self.conn.read_string().await?;
                    let _columns_metadata = self.conn.read_string().await?;
                }
                _ => {
                    return Err(Error::Protocol(format!(
                        "Unexpected packet type during execute: {}",
                        packet_type
                    )));
                }
            }
        }

        Ok(())
    }

    /// Execute a query and return results
    ///
    /// For INSERT operations, use `insert()` instead.
    /// For DDL/DML without results, use `execute()` instead.
    /// For query tracing, use `query_with_id()`.
    pub async fn query(
        &mut self,
        query: impl Into<Query>,
    ) -> Result<QueryResult> {
        self.query_with_id(query, "").await
    }

    /// Execute a query with a specific query ID and return results
    ///
    /// The query ID is useful for query tracing and debugging.
    ///
    /// # Example
    /// ```no_run
    /// # use clickhouse_client::{Client, ClientOptions};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut client = Client::connect(ClientOptions::default()).await?;
    /// let result = client.query_with_id("SELECT 1", "select-123").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_with_id(
        &mut self,
        query: impl Into<Query>,
        query_id: &str,
    ) -> Result<QueryResult> {
        let mut query = query.into();
        if !query_id.is_empty() {
            query = Query::new(query.text()).with_query_id(query_id);
        }

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
                    // Skip temp table name if protocol supports it (matches
                    // C++ ReceiveData)
                    if self.server_info.revision >= 50264 {
                        // DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES
                        let _temp_table = self.conn.read_string().await?;
                    }
                    let block =
                        self.block_reader.read_block(&mut self.conn).await?;

                    // Invoke data callback if present
                    if let Some(callback) = query.get_on_data_cancelable() {
                        let should_continue = callback(&block);
                        if !should_continue {
                            eprintln!(
                                "[DEBUG] Query cancelled by data callback"
                            );
                            break;
                        }
                    } else if let Some(callback) = query.get_on_data() {
                        callback(&block);
                    }

                    if !block.is_empty() {
                        blocks.push(block);
                    }
                }
                code if code == ServerCode::Progress as u64 => {
                    eprintln!("[DEBUG] Received progress packet");
                    progress_info = self.read_progress().await?;

                    // Invoke progress callback if present
                    if let Some(callback) = query.get_on_progress() {
                        callback(&progress_info);
                    }
                }
                code if code == ServerCode::EndOfStream as u64 => {
                    eprintln!("[DEBUG] Received end of stream");
                    break;
                }
                code if code == ServerCode::ProfileInfo as u64 => {
                    eprintln!("[DEBUG] Received profile info packet");
                    // Read ProfileInfo fields directly
                    let rows = self.conn.read_varint().await?;
                    let blocks = self.conn.read_varint().await?;
                    let bytes = self.conn.read_varint().await?;
                    let applied_limit = self.conn.read_u8().await? != 0;
                    let rows_before_limit = self.conn.read_varint().await?;
                    let calculated_rows_before_limit =
                        self.conn.read_u8().await? != 0;

                    let profile = crate::query::Profile {
                        rows,
                        blocks,
                        bytes,
                        rows_before_limit,
                        applied_limit,
                        calculated_rows_before_limit,
                    };

                    // Invoke profile callback if present
                    if let Some(callback) = query.get_on_profile() {
                        callback(&profile);
                    }
                }
                code if code == ServerCode::Log as u64 => {
                    eprintln!("[DEBUG] Received log packet");
                    // Skip string first (log tag)
                    let _log_tag = self.conn.read_string().await?;
                    // Read the log block (sent uncompressed)
                    let uncompressed_reader =
                        BlockReader::new(self.server_info.revision);
                    let block =
                        uncompressed_reader.read_block(&mut self.conn).await?;

                    // Invoke server log callback if present
                    if let Some(callback) = query.get_on_server_log() {
                        callback(&block);
                    }
                }
                code if code == ServerCode::ProfileEvents as u64 => {
                    eprintln!("[DEBUG] Received profile events packet");
                    // Skip string first (matches C++ implementation)
                    let _table_name = self.conn.read_string().await?;
                    // Read ProfileEvents block (sent uncompressed)
                    let uncompressed_reader =
                        BlockReader::new(self.server_info.revision);
                    let block =
                        uncompressed_reader.read_block(&mut self.conn).await?;

                    // Invoke profile events callback if present
                    if let Some(callback) = query.get_on_profile_events() {
                        callback(&block);
                    }
                }
                code if code == ServerCode::TableColumns as u64 => {
                    eprintln!(
                        "[DEBUG] Received table columns packet (ignoring)"
                    );
                    // Skip external table name
                    let _table_name = self.conn.read_string().await?;
                    // Skip columns metadata string
                    let _columns_metadata = self.conn.read_string().await?;
                }
                code if code == ServerCode::Exception as u64 => {
                    eprintln!("[DEBUG] Server returned exception during query, reading details...");
                    let exception = self.read_exception().await?;
                    eprintln!(
                        "[DEBUG] Exception: code={}, name={}, msg={}",
                        exception.code, exception.name, exception.display_text
                    );

                    // Invoke exception callback if present
                    if let Some(callback) = query.get_on_exception() {
                        callback(&exception);
                    }

                    return Err(Error::Protocol(format!(
                        "ClickHouse exception: {} ({}): {}",
                        exception.name, exception.code, exception.display_text
                    )));
                }
                other => {
                    eprintln!("[DEBUG] Unexpected packet type: {}", other);
                    return Err(Error::Protocol(format!(
                        "Unexpected packet type: {}",
                        other
                    )));
                }
            }
        }

        Ok(QueryResult { blocks, progress: progress_info })
    }

    /// Execute a SELECT query with external tables for JOIN operations
    ///
    /// External tables allow passing temporary in-memory data to queries for JOINs
    /// without creating actual tables in ClickHouse.
    ///
    /// # Example
    /// ```no_run
    /// # use clickhouse_client::{Client, ClientOptions, Block, ExternalTable};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut client = Client::connect(ClientOptions::default()).await?;
    /// // Create a block with temporary data
    /// let mut block = Block::new();
    /// // ... populate block with data ...
    ///
    /// // Create external table
    /// let ext_table = ExternalTable::new("temp_table", block);
    ///
    /// // Use in query with JOIN
    /// let query = "SELECT * FROM my_table JOIN temp_table ON my_table.id = temp_table.id";
    /// let result = client.query_with_external_data(query, &[ext_table]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_with_external_data(
        &mut self,
        query: impl Into<Query>,
        external_tables: &[crate::ExternalTable],
    ) -> Result<QueryResult> {
        self.query_with_external_data_and_id(query, "", external_tables).await
    }

    /// Execute a SELECT query with external tables and a specific query ID
    ///
    /// Combines external table support with query ID tracing.
    ///
    /// # Example
    /// ```no_run
    /// # use clickhouse_client::{Client, ClientOptions, Block, ExternalTable};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut client = Client::connect(ClientOptions::default()).await?;
    /// # let mut block = Block::new();
    /// let ext_table = ExternalTable::new("temp_table", block);
    /// let result = client.query_with_external_data_and_id(
    ///     "SELECT * FROM my_table JOIN temp_table ON my_table.id = temp_table.id",
    ///     "query-123",
    ///     &[ext_table]
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_with_external_data_and_id(
        &mut self,
        query: impl Into<Query>,
        query_id: &str,
        external_tables: &[crate::ExternalTable],
    ) -> Result<QueryResult> {
        let mut query = query.into();
        if !query_id.is_empty() {
            query = Query::new(query.text()).with_query_id(query_id);
        }

        // Send query WITHOUT finalization (we'll finalize after external tables)
        self.send_query_internal(&query, false).await?;

        // Send external tables data (before finalization)
        self.send_external_tables(external_tables).await?;

        // Now finalize the query with empty block
        self.finalize_query().await?;

        // Receive results (same as regular query)
        let mut blocks = Vec::new();
        let mut progress_info = Progress::default();

        loop {
            let packet_type = self.conn.read_varint().await?;
            eprintln!("[DEBUG] Query response packet: {}", packet_type);

            match packet_type {
                code if code == ServerCode::Data as u64 => {
                    eprintln!("[DEBUG] Received data packet");
                    // Skip temp table name if protocol supports it
                    if self.server_info.revision >= 50264 {
                        let _temp_table = self.conn.read_string().await?;
                    }
                    let block =
                        self.block_reader.read_block(&mut self.conn).await?;

                    // Invoke data callback if present
                    if let Some(callback) = query.get_on_data_cancelable() {
                        let should_continue = callback(&block);
                        if !should_continue {
                            eprintln!(
                                "[DEBUG] Query cancelled by data callback"
                            );
                            break;
                        }
                    } else if let Some(callback) = query.get_on_data() {
                        callback(&block);
                    }

                    if !block.is_empty() {
                        blocks.push(block);
                    }
                }
                code if code == ServerCode::Progress as u64 => {
                    eprintln!("[DEBUG] Received progress packet");
                    progress_info = self.read_progress().await?;

                    // Invoke progress callback if present
                    if let Some(callback) = query.get_on_progress() {
                        callback(&progress_info);
                    }
                }
                code if code == ServerCode::EndOfStream as u64 => {
                    eprintln!("[DEBUG] Received end of stream");
                    break;
                }
                code if code == ServerCode::ProfileInfo as u64 => {
                    eprintln!("[DEBUG] Received profile info packet");
                    let rows = self.conn.read_varint().await?;
                    let blocks = self.conn.read_varint().await?;
                    let bytes = self.conn.read_varint().await?;
                    let applied_limit = self.conn.read_u8().await?;
                    let rows_before_limit = self.conn.read_varint().await?;
                    let calculated = self.conn.read_u8().await?;

                    let profile = Profile {
                        rows,
                        blocks,
                        bytes,
                        applied_limit: applied_limit != 0,
                        rows_before_limit,
                        calculated_rows_before_limit: calculated != 0,
                    };

                    // Invoke profile callback if present
                    if let Some(callback) = query.get_on_profile() {
                        callback(&profile);
                    }
                }
                code if code == ServerCode::Log as u64 => {
                    eprintln!("[DEBUG] Received log packet");
                    let _log_tag = self.conn.read_string().await?;
                    // Log blocks are sent uncompressed
                    let uncompressed_reader =
                        BlockReader::new(self.server_info.revision);
                    let block =
                        uncompressed_reader.read_block(&mut self.conn).await?;

                    // Invoke server log callback if present
                    if let Some(callback) = query.get_on_server_log() {
                        callback(&block);
                    }
                }
                code if code == ServerCode::ProfileEvents as u64 => {
                    eprintln!("[DEBUG] Received profile events packet");
                    let _table_name = self.conn.read_string().await?;
                    // ProfileEvents blocks are sent uncompressed
                    let uncompressed_reader =
                        BlockReader::new(self.server_info.revision);
                    let block =
                        uncompressed_reader.read_block(&mut self.conn).await?;

                    // Invoke profile events callback if present
                    if let Some(callback) = query.get_on_profile_events() {
                        callback(&block);
                    }
                }
                code if code == ServerCode::TableColumns as u64 => {
                    eprintln!("[DEBUG] Received table columns packet (ignoring)");
                    // Skip external table name
                    let _table_name = self.conn.read_string().await?;
                    // Skip columns metadata string
                    let _columns_metadata = self.conn.read_string().await?;
                }
                code if code == ServerCode::Exception as u64 => {
                    let exception = self.read_exception().await?;
                    eprintln!(
                        "[DEBUG] Received exception: {} - {}",
                        exception.name, exception.display_text
                    );

                    // Invoke exception callback if present
                    if let Some(callback) = query.get_on_exception() {
                        callback(&exception);
                    }

                    return Err(Error::Protocol(format!(
                        "ClickHouse exception: {} (code {}): {}",
                        exception.name, exception.code, exception.display_text
                    )));
                }
                other => {
                    return Err(Error::Protocol(format!(
                        "Unexpected packet type during query: {}",
                        other
                    )));
                }
            }
        }

        Ok(QueryResult { blocks, progress: progress_info })
    }

    /// Send a query packet (always finalized)
    async fn send_query(&mut self, query: &Query) -> Result<()> {
        self.send_query_internal(query, true).await
    }

    /// Send a query packet (internal with finalization control)
    async fn send_query_internal(&mut self, query: &Query, finalize: bool) -> Result<()> {
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
                // OpenTelemetry tracing context
                if let Some(ctx) = query.tracing_context() {
                    self.conn.write_u8(1).await?; // have OpenTelemetry
                                                  // Write trace_id (128-bit)
                    self.conn.write_u128(ctx.trace_id).await?;
                    // Write span_id (64-bit)
                    self.conn.write_u64(ctx.span_id).await?;
                    // Write tracestate
                    self.conn.write_string(&ctx.tracestate).await?;
                    // Write trace_flags
                    self.conn.write_u8(ctx.trace_flags).await?;
                } else {
                    self.conn.write_u8(0).await?; // no OpenTelemetry
                }
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
            for (key, field) in query.settings() {
                self.conn.write_string(key).await?;
                self.conn.write_varint(field.flags).await?;
                self.conn.write_string(&field.value).await?;
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
        let compression_enabled =
            if self.options.compression.is_some() { 1u64 } else { 0u64 };
        self.conn.write_varint(compression_enabled).await?;
        self.conn.write_string(query.text()).await?;

        // Query parameters (for servers >= 54459)
        if revision >= 54459 {
            for (key, value) in query.parameters() {
                self.conn.write_string(key).await?;
                self.conn.write_varint(2).await?; // Custom type
                self.conn.write_quoted_string(value).await?;
            }
            // Empty string to mark end of parameters
            self.conn.write_string("").await?;
        }

        // Conditionally finalize based on parameter
        if finalize {
            self.finalize_query().await?;
        }

        Ok(())
    }

    /// Finalize query by sending empty block marker
    ///
    /// Must be called after send_query_internal() to complete the query protocol.
    /// For most queries, use send_query() which handles this automatically.
    /// Only split for special cases like external tables.
    async fn finalize_query(&mut self) -> Result<()> {
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
        eprintln!("[DEBUG] Query finalized");
        Ok(())
    }

    /// Send external tables data
    ///
    /// External tables are sent as Data packets after the initial query packet.
    /// Each table is sent with its name and block data.
    /// Empty blocks are skipped to keep the connection in a consistent state.
    async fn send_external_tables(&mut self, external_tables: &[crate::ExternalTable]) -> Result<()> {
        for table in external_tables {
            // Skip empty blocks to keep connection consistent
            if table.data.row_count() == 0 {
                continue;
            }

            eprintln!("[DEBUG] Sending external table: {}", table.name);

            // Send Data packet type
            self.conn.write_varint(ClientCode::Data as u64).await?;

            // Send table name (this serves as the temp table name for this Data packet)
            self.conn.write_string(&table.name).await?;

            // Send block data WITHOUT temp table name prefix (we already wrote it above)
            self.block_writer.write_block_with_temp_table(&mut self.conn, &table.data, false).await?;
        }

        self.conn.flush().await?;
        Ok(())
    }

    /// Read progress info
    async fn read_progress(&mut self) -> Result<Progress> {
        let rows = self.conn.read_varint().await?;
        let bytes = self.conn.read_varint().await?;
        let total_rows = self.conn.read_varint().await?;

        let (written_rows, written_bytes) = if self.server_info.revision
            >= 54405
        {
            (self.conn.read_varint().await?, self.conn.read_varint().await?)
        } else {
            (0, 0)
        };

        Ok(Progress { rows, bytes, total_rows, written_rows, written_bytes })
    }

    /// Read exception from server
    fn read_exception<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<crate::query::Exception>>
                + 'a,
        >,
    > {
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
            eprintln!(
                "[DEBUG] Exception display_text length: {}",
                display_text.len()
            );
            eprintln!("[DEBUG] Reading exception stack_trace...");
            let stack_trace = self.conn.read_string().await?;
            eprintln!(
                "[DEBUG] Exception stack_trace length: {}",
                stack_trace.len()
            );

            // Check for nested exception
            let has_nested = self.conn.read_u8().await?;
            let nested = if has_nested != 0 {
                Some(Box::new(self.read_exception().await?))
            } else {
                None
            };

            Ok(Exception { code, name, display_text, stack_trace, nested })
        })
    }

    /// Insert data into a table
    ///
    /// This method constructs an INSERT query from the block's column names
    /// and sends the data. Example: `client.insert("my_database.my_table",
    /// block).await?`
    ///
    /// For query tracing, use `insert_with_id()` to specify a query ID.
    pub async fn insert(
        &mut self,
        table_name: &str,
        block: Block,
    ) -> Result<()> {
        self.insert_with_id(table_name, "", block).await
    }

    /// Insert data into a table with a specific query ID
    ///
    /// The query ID is useful for:
    /// - Query tracing and debugging
    /// - Correlating queries with logs
    /// - OpenTelemetry integration
    ///
    /// # Example
    /// ```no_run
    /// # use clickhouse_client::{Client, ClientOptions, Block};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut client = Client::connect(ClientOptions::default()).await?;
    /// # let block = Block::new();
    /// client.insert_with_id("my_table", "trace-id-12345", block).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn insert_with_id(
        &mut self,
        table_name: &str,
        query_id: &str,
        block: Block,
    ) -> Result<()> {
        // Build query with column names from block (matches C++
        // implementation)
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
        let query = Query::new(query_text).with_query_id(query_id);

        // Send query
        self.send_query(&query).await?;

        // Wait for server to respond with Data packet (matches C++ Insert
        // flow)
        eprintln!("[DEBUG] Waiting for server Data packet...");
        loop {
            let packet_type = self.conn.read_varint().await?;
            eprintln!(
                "[DEBUG] INSERT wait response packet type: {}",
                packet_type
            );

            match packet_type {
                code if code == ServerCode::Data as u64 => {
                    eprintln!(
                        "[DEBUG] Received Data packet, ready to send data"
                    );
                    // CRITICAL: Must consume the Data packet's payload to keep
                    // stream aligned! Skip temp table name
                    if self.server_info.revision >= 50264 {
                        let _temp_table = self.conn.read_string().await?;
                    }
                    // Read the block (likely empty, but must consume it)
                    let _block =
                        self.block_reader.read_block(&mut self.conn).await?;
                    eprintln!(
                        "[DEBUG] Consumed Data packet payload, stream aligned"
                    );
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
        eprintln!(
            "[DEBUG] Sending data block with {} rows",
            block.row_count()
        );
        self.conn.write_varint(ClientCode::Data as u64).await?;
        self.block_writer.write_block(&mut self.conn, &block).await?;

        // Send empty block to signal end
        eprintln!("[DEBUG] Sending empty block to signal end");
        let empty_block = Block::new();
        self.conn.write_varint(ClientCode::Data as u64).await?;
        self.block_writer.write_block(&mut self.conn, &empty_block).await?;

        // Wait for EndOfStream (matches C++ flow)
        eprintln!("[DEBUG] Waiting for EndOfStream...");
        loop {
            let packet_type = self.conn.read_varint().await?;
            eprintln!(
                "[DEBUG] INSERT final response packet type: {}",
                packet_type
            );

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
                    let _block =
                        self.block_reader.read_block(&mut self.conn).await?;
                }
                code if code == ServerCode::Progress as u64 => {
                    eprintln!("[DEBUG] Received Progress packet");
                    let _ = self.read_progress().await?;
                }
                code if code == ServerCode::ProfileEvents as u64 => {
                    eprintln!(
                        "[DEBUG] Received ProfileEvents packet (skipping)"
                    );
                    let _table_name = self.conn.read_string().await?;
                    let uncompressed_reader =
                        BlockReader::new(self.server_info.revision);
                    let _block =
                        uncompressed_reader.read_block(&mut self.conn).await?;
                }
                code if code == ServerCode::TableColumns as u64 => {
                    eprintln!(
                        "[DEBUG] Received TableColumns packet (skipping)"
                    );
                    let _table_name = self.conn.read_string().await?;
                    let _columns_metadata = self.conn.read_string().await?;
                }
                code if code == ServerCode::Exception as u64 => {
                    eprintln!(
                        "[DEBUG] Server returned exception after sending data"
                    );
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
            Err(Error::Protocol(format!("Expected Pong, got {}", packet_type)))
        }
    }

    /// Cancel the current query
    ///
    /// Sends a cancel packet to the server to stop any currently running
    /// query. Note: This is most useful when called with a cancelable
    /// callback, or when you need to cancel a long-running query from
    /// outside the query execution flow.
    pub async fn cancel(&mut self) -> Result<()> {
        eprintln!("[DEBUG] Sending cancel...");
        self.conn.write_varint(ClientCode::Cancel as u64).await?;
        self.conn.flush().await?;
        eprintln!("[DEBUG] Cancel sent");
        Ok(())
    }

    /// Get server info
    ///
    /// Returns information about the connected ClickHouse server including
    /// name, version, revision, timezone, and display name.
    ///
    /// # Example
    /// ```no_run
    /// # use clickhouse_client::{Client, ClientOptions};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect(ClientOptions::default()).await?;
    /// let info = client.server_info();
    /// println!("Server: {} v{}.{}.{}",
    ///     info.name,
    ///     info.version_major,
    ///     info.version_minor,
    ///     info.version_patch
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn server_info(&self) -> &ServerInfo {
        &self.server_info
    }

    /// Get server version as a tuple (major, minor, patch)
    ///
    /// # Example
    /// ```no_run
    /// # use clickhouse_client::{Client, ClientOptions};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect(ClientOptions::default()).await?;
    /// let (major, minor, patch) = client.server_version();
    /// println!("Server version: {}.{}.{}", major, minor, patch);
    /// # Ok(())
    /// # }
    /// ```
    pub fn server_version(&self) -> (u64, u64, u64) {
        (
            self.server_info.version_major,
            self.server_info.version_minor,
            self.server_info.version_patch,
        )
    }

    /// Get server revision number
    ///
    /// The revision number is used for protocol feature negotiation.
    ///
    /// # Example
    /// ```no_run
    /// # use clickhouse_client::{Client, ClientOptions};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = Client::connect(ClientOptions::default()).await?;
    /// let revision = client.server_revision();
    /// println!("Server revision: {}", revision);
    /// # Ok(())
    /// # }
    /// ```
    pub fn server_revision(&self) -> u64 {
        self.server_info.revision
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
        let result =
            QueryResult { blocks: vec![], progress: Progress::default() };

        assert_eq!(result.total_rows(), 0);
    }
}
