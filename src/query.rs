use crate::{
    block::Block,
    Error,
    Result,
};
use bytes::{
    Buf,
    BufMut,
    BytesMut,
};
use std::{
    collections::HashMap,
    sync::Arc,
};

/// Query settings
pub type QuerySettings = HashMap<String, String>;

/// OpenTelemetry tracing context (W3C Trace Context)
/// See: <https://www.w3.org/TR/trace-context/>
#[derive(Clone, Debug, Default)]
pub struct TracingContext {
    /// Trace ID (128-bit identifier)
    pub trace_id: u128,
    /// Span ID (64-bit identifier)
    pub span_id: u64,
    /// Tracestate header value
    pub tracestate: String,
    /// Trace flags (8-bit flags)
    pub trace_flags: u8,
}

impl TracingContext {
    /// Create a new empty tracing context
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a tracing context with trace and span IDs
    pub fn with_ids(trace_id: u128, span_id: u64) -> Self {
        Self { trace_id, span_id, tracestate: String::new(), trace_flags: 0 }
    }

    /// Set trace ID
    pub fn trace_id(mut self, trace_id: u128) -> Self {
        self.trace_id = trace_id;
        self
    }

    /// Set span ID
    pub fn span_id(mut self, span_id: u64) -> Self {
        self.span_id = span_id;
        self
    }

    /// Set tracestate
    pub fn tracestate(mut self, tracestate: impl Into<String>) -> Self {
        self.tracestate = tracestate.into();
        self
    }

    /// Set trace flags
    pub fn trace_flags(mut self, flags: u8) -> Self {
        self.trace_flags = flags;
        self
    }

    /// Check if tracing is enabled (non-zero trace_id)
    pub fn is_enabled(&self) -> bool {
        self.trace_id != 0
    }
}

/// Query structure for building and executing queries
#[derive(Clone)]
pub struct Query {
    /// The SQL query string
    query_text: String,
    /// Query ID (optional)
    query_id: String,
    /// Query settings
    settings: QuerySettings,
    /// Query parameters (for parameterized queries)
    parameters: HashMap<String, String>,
    /// OpenTelemetry tracing context
    tracing_context: Option<TracingContext>,
    /// Progress callback
    on_progress: Option<ProgressCallback>,
    /// Profile callback
    on_profile: Option<ProfileCallback>,
    /// Profile events callback
    on_profile_events: Option<ProfileEventsCallback>,
    /// Server log callback
    on_server_log: Option<ServerLogCallback>,
    /// Exception callback
    on_exception: Option<ExceptionCallback>,
    /// Data callback
    on_data: Option<DataCallback>,
    /// Cancelable data callback
    on_data_cancelable: Option<DataCancelableCallback>,
}

impl Query {
    /// Create a new query
    pub fn new(query_text: impl Into<String>) -> Self {
        Self {
            query_text: query_text.into(),
            query_id: String::new(),
            settings: HashMap::new(),
            parameters: HashMap::new(),
            tracing_context: None,
            on_progress: None,
            on_profile: None,
            on_profile_events: None,
            on_server_log: None,
            on_exception: None,
            on_data: None,
            on_data_cancelable: None,
        }
    }
}

impl From<&str> for Query {
    fn from(s: &str) -> Self {
        Query::new(s)
    }
}

impl From<String> for Query {
    fn from(s: String) -> Self {
        Query::new(s)
    }
}

impl Query {
    /// Set the query ID
    pub fn with_query_id(mut self, query_id: impl Into<String>) -> Self {
        self.query_id = query_id.into();
        self
    }

    /// Set a query setting
    pub fn with_setting(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.settings.insert(key.into(), value.into());
        self
    }

    /// Set a query parameter
    pub fn with_parameter(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.parameters.insert(key.into(), value.into());
        self
    }

    /// Set OpenTelemetry tracing context
    pub fn with_tracing_context(mut self, context: TracingContext) -> Self {
        self.tracing_context = Some(context);
        self
    }

    /// Get the query text
    pub fn text(&self) -> &str {
        &self.query_text
    }

    /// Get the tracing context
    pub fn tracing_context(&self) -> Option<&TracingContext> {
        self.tracing_context.as_ref()
    }

    /// Get the query ID
    pub fn id(&self) -> &str {
        &self.query_id
    }

    /// Get the settings
    pub fn settings(&self) -> &QuerySettings {
        &self.settings
    }

    /// Get the parameters
    pub fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }

    /// Set progress callback
    pub fn on_progress<F>(mut self, callback: F) -> Self
    where
        F: Fn(&Progress) + Send + Sync + 'static,
    {
        self.on_progress = Some(Arc::new(callback));
        self
    }

    /// Set profile callback
    pub fn on_profile<F>(mut self, callback: F) -> Self
    where
        F: Fn(&Profile) + Send + Sync + 'static,
    {
        self.on_profile = Some(Arc::new(callback));
        self
    }

    /// Set profile events callback
    pub fn on_profile_events<F>(mut self, callback: F) -> Self
    where
        F: Fn(&Block) -> bool + Send + Sync + 'static,
    {
        self.on_profile_events = Some(Arc::new(callback));
        self
    }

    /// Set server log callback
    pub fn on_server_log<F>(mut self, callback: F) -> Self
    where
        F: Fn(&Block) -> bool + Send + Sync + 'static,
    {
        self.on_server_log = Some(Arc::new(callback));
        self
    }

    /// Set exception callback
    pub fn on_exception<F>(mut self, callback: F) -> Self
    where
        F: Fn(&Exception) + Send + Sync + 'static,
    {
        self.on_exception = Some(Arc::new(callback));
        self
    }

    /// Set data callback
    pub fn on_data<F>(mut self, callback: F) -> Self
    where
        F: Fn(&Block) + Send + Sync + 'static,
    {
        self.on_data = Some(Arc::new(callback));
        self
    }

    /// Set cancelable data callback (return false to cancel)
    pub fn on_data_cancelable<F>(mut self, callback: F) -> Self
    where
        F: Fn(&Block) -> bool + Send + Sync + 'static,
    {
        self.on_data_cancelable = Some(Arc::new(callback));
        self
    }

    // Internal getters for Client to invoke callbacks

    pub(crate) fn get_on_progress(&self) -> Option<&ProgressCallback> {
        self.on_progress.as_ref()
    }

    pub(crate) fn get_on_profile(&self) -> Option<&ProfileCallback> {
        self.on_profile.as_ref()
    }

    pub(crate) fn get_on_profile_events(
        &self,
    ) -> Option<&ProfileEventsCallback> {
        self.on_profile_events.as_ref()
    }

    pub(crate) fn get_on_server_log(&self) -> Option<&ServerLogCallback> {
        self.on_server_log.as_ref()
    }

    pub(crate) fn get_on_exception(&self) -> Option<&ExceptionCallback> {
        self.on_exception.as_ref()
    }

    pub(crate) fn get_on_data(&self) -> Option<&DataCallback> {
        self.on_data.as_ref()
    }

    pub(crate) fn get_on_data_cancelable(
        &self,
    ) -> Option<&DataCancelableCallback> {
        self.on_data_cancelable.as_ref()
    }
}

/// Client information sent during handshake
#[derive(Clone, Debug)]
pub struct ClientInfo {
    pub interface_type: u8, // 1 = TCP
    pub query_kind: u8,
    pub initial_user: String,
    pub initial_query_id: String,
    pub quota_key: String,
    pub os_user: String,
    pub client_hostname: String,
    pub client_name: String,
    pub client_version_major: u64,
    pub client_version_minor: u64,
    pub client_version_patch: u64,
    pub client_revision: u64,
}

impl Default for ClientInfo {
    fn default() -> Self {
        Self {
            interface_type: 1, // TCP
            query_kind: 0,
            initial_user: String::new(),
            initial_query_id: String::new(),
            quota_key: String::new(),
            os_user: std::env::var("USER")
                .unwrap_or_else(|_| "default".to_string()),
            client_hostname: "localhost".to_string(),
            client_name: "clickhouse-rust".to_string(),
            client_version_major: 1,
            client_version_minor: 0,
            client_version_patch: 0,
            client_revision: 54459, /* DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS */
        }
    }
}

impl ClientInfo {
    /// Serialize to buffer
    pub fn write_to(&self, buffer: &mut BytesMut) -> Result<()> {
        buffer.put_u8(self.interface_type);

        write_string(buffer, &self.os_user);
        write_string(buffer, &self.client_hostname);
        write_string(buffer, &self.client_name);

        write_varint(buffer, self.client_version_major);
        write_varint(buffer, self.client_version_minor);
        write_varint(buffer, self.client_revision);

        Ok(())
    }

    /// Deserialize from buffer
    pub fn read_from(buffer: &mut &[u8]) -> Result<Self> {
        if buffer.is_empty() {
            return Err(Error::Protocol(
                "Not enough data to read ClientInfo".to_string(),
            ));
        }

        let interface_type = buffer[0];
        buffer.advance(1);

        let os_user = read_string(buffer)?;
        let client_hostname = read_string(buffer)?;
        let client_name = read_string(buffer)?;

        let client_version_major = read_varint(buffer)?;
        let client_version_minor = read_varint(buffer)?;
        let client_revision = read_varint(buffer)?;

        Ok(Self {
            interface_type,
            query_kind: 0,
            initial_user: String::new(),
            initial_query_id: String::new(),
            quota_key: String::new(),
            os_user,
            client_hostname,
            client_name,
            client_version_major,
            client_version_minor,
            client_version_patch: 0,
            client_revision,
        })
    }
}

/// Server information received during handshake
#[derive(Clone, Debug, Default)]
pub struct ServerInfo {
    pub name: String,
    pub version_major: u64,
    pub version_minor: u64,
    pub version_patch: u64,
    pub revision: u64,
    pub timezone: String,
    pub display_name: String,
}

impl ServerInfo {
    /// Serialize to buffer
    pub fn write_to(&self, buffer: &mut BytesMut) -> Result<()> {
        write_string(buffer, &self.name);
        write_varint(buffer, self.version_major);
        write_varint(buffer, self.version_minor);
        write_varint(buffer, self.revision);

        if self.revision >= 54058 {
            write_string(buffer, &self.timezone);
        }

        if self.revision >= 54372 {
            write_string(buffer, &self.display_name);
        }

        if self.revision >= 54401 {
            write_varint(buffer, self.version_patch);
        }

        Ok(())
    }

    /// Deserialize from buffer
    pub fn read_from(buffer: &mut &[u8]) -> Result<Self> {
        let name = read_string(buffer)?;
        let version_major = read_varint(buffer)?;
        let version_minor = read_varint(buffer)?;
        let revision = read_varint(buffer)?;

        let timezone = if revision >= 54058 {
            read_string(buffer)?
        } else {
            String::new()
        };

        let display_name = if revision >= 54372 {
            read_string(buffer)?
        } else {
            String::new()
        };

        let version_patch =
            if revision >= 54401 { read_varint(buffer)? } else { 0 };

        Ok(Self {
            name,
            version_major,
            version_minor,
            version_patch,
            revision,
            timezone,
            display_name,
        })
    }
}

/// Progress information
#[derive(Clone, Debug, Default)]
pub struct Progress {
    pub rows: u64,
    pub bytes: u64,
    pub total_rows: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
}

/// Profile information
#[derive(Clone, Debug, Default)]
pub struct Profile {
    pub rows: u64,
    pub blocks: u64,
    pub bytes: u64,
    pub rows_before_limit: u64,
    pub applied_limit: bool,
    pub calculated_rows_before_limit: bool,
}

/// Callback types for query execution
pub type ProgressCallback = Arc<dyn Fn(&Progress) + Send + Sync>;
pub type ProfileCallback = Arc<dyn Fn(&Profile) + Send + Sync>;
pub type ProfileEventsCallback = Arc<dyn Fn(&Block) -> bool + Send + Sync>;
pub type ServerLogCallback = Arc<dyn Fn(&Block) -> bool + Send + Sync>;
pub type ExceptionCallback = Arc<dyn Fn(&Exception) + Send + Sync>;
pub type DataCallback = Arc<dyn Fn(&Block) + Send + Sync>;
pub type DataCancelableCallback = Arc<dyn Fn(&Block) -> bool + Send + Sync>;

impl Progress {
    /// Serialize to buffer
    pub fn write_to(
        &self,
        buffer: &mut BytesMut,
        server_revision: u64,
    ) -> Result<()> {
        write_varint(buffer, self.rows);
        write_varint(buffer, self.bytes);
        write_varint(buffer, self.total_rows);

        if server_revision >= 54405 {
            write_varint(buffer, self.written_rows);
            write_varint(buffer, self.written_bytes);
        }

        Ok(())
    }

    /// Deserialize from buffer
    pub fn read_from(
        buffer: &mut &[u8],
        server_revision: u64,
    ) -> Result<Self> {
        let rows = read_varint(buffer)?;
        let bytes = read_varint(buffer)?;
        let total_rows = read_varint(buffer)?;

        let (written_rows, written_bytes) = if server_revision >= 54405 {
            (read_varint(buffer)?, read_varint(buffer)?)
        } else {
            (0, 0)
        };

        Ok(Self { rows, bytes, total_rows, written_rows, written_bytes })
    }
}

impl Profile {
    /// Deserialize from buffer (ProfileInfo packet)
    pub fn read_from(buffer: &mut &[u8]) -> Result<Self> {
        let rows = read_varint(buffer)?;
        let blocks = read_varint(buffer)?;
        let bytes = read_varint(buffer)?;

        let applied_limit = if !buffer.is_empty() {
            let val = buffer[0];
            buffer.advance(1);
            val != 0
        } else {
            false
        };

        let rows_before_limit = read_varint(buffer)?;

        let calculated_rows_before_limit = if !buffer.is_empty() {
            let val = buffer[0];
            buffer.advance(1);
            val != 0
        } else {
            false
        };

        Ok(Self {
            rows,
            blocks,
            bytes,
            rows_before_limit,
            applied_limit,
            calculated_rows_before_limit,
        })
    }
}

/// Exception from server
#[derive(Clone, Debug)]
pub struct Exception {
    pub code: i32,
    pub name: String,
    pub display_text: String,
    pub stack_trace: String,
    pub nested: Option<Box<Exception>>,
}

impl Exception {
    /// Serialize to buffer
    pub fn write_to(&self, buffer: &mut BytesMut) -> Result<()> {
        buffer.put_i32_le(self.code);
        write_string(buffer, &self.name);
        write_string(buffer, &self.display_text);
        write_string(buffer, &self.stack_trace);

        // Write nested exception
        let has_nested = self.nested.is_some();
        buffer.put_u8(if has_nested { 1 } else { 0 });

        if let Some(nested) = &self.nested {
            nested.write_to(buffer)?;
        }

        Ok(())
    }

    /// Deserialize from buffer
    pub fn read_from(buffer: &mut &[u8]) -> Result<Self> {
        if buffer.len() < 4 {
            return Err(Error::Protocol(
                "Not enough data to read Exception".to_string(),
            ));
        }

        let code = {
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&buffer[..4]);
            buffer.advance(4);
            i32::from_le_bytes(bytes)
        };

        let name = read_string(buffer)?;
        let display_text = read_string(buffer)?;
        let stack_trace = read_string(buffer)?;

        if buffer.is_empty() {
            return Err(Error::Protocol(
                "Not enough data to read nested exception flag".to_string(),
            ));
        }

        let has_nested = buffer[0] != 0;
        buffer.advance(1);

        let nested = if has_nested {
            Some(Box::new(Exception::read_from(buffer)?))
        } else {
            None
        };

        Ok(Self { code, name, display_text, stack_trace, nested })
    }
}

// Helper functions for varint and string encoding
fn read_varint(buffer: &mut &[u8]) -> Result<u64> {
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

fn write_varint(buffer: &mut BytesMut, mut value: u64) {
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

fn read_string(buffer: &mut &[u8]) -> Result<String> {
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

fn write_string(buffer: &mut BytesMut, s: &str) {
    write_varint(buffer, s.len() as u64);
    buffer.put_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_creation() {
        let query = Query::new("SELECT 1");
        assert_eq!(query.text(), "SELECT 1");
        assert_eq!(query.id(), "");
        assert!(query.settings().is_empty());
    }

    #[test]
    fn test_query_with_id() {
        let query = Query::new("SELECT 1").with_query_id("test_query");
        assert_eq!(query.id(), "test_query");
    }

    #[test]
    fn test_query_with_settings() {
        let query = Query::new("SELECT 1")
            .with_setting("max_threads", "4")
            .with_setting("max_memory_usage", "10000000");

        assert_eq!(query.settings().len(), 2);
        assert_eq!(
            query.settings().get("max_threads"),
            Some(&"4".to_string())
        );
    }

    #[test]
    fn test_client_info_roundtrip() {
        let info = ClientInfo::default();
        let mut buffer = BytesMut::new();
        info.write_to(&mut buffer).unwrap();

        let mut reader = &buffer[..];
        let decoded = ClientInfo::read_from(&mut reader).unwrap();

        assert_eq!(decoded.interface_type, 1);
        assert_eq!(decoded.client_name, "clickhouse-rust");
    }

    #[test]
    fn test_server_info_roundtrip() {
        let info = ServerInfo {
            name: "ClickHouse".to_string(),
            version_major: 21,
            version_minor: 8,
            version_patch: 5,
            revision: 54449,
            timezone: "UTC".to_string(),
            display_name: "ClickHouse server".to_string(),
        };

        let mut buffer = BytesMut::new();
        info.write_to(&mut buffer).unwrap();

        let mut reader = &buffer[..];
        let decoded = ServerInfo::read_from(&mut reader).unwrap();

        assert_eq!(decoded.name, "ClickHouse");
        assert_eq!(decoded.version_major, 21);
        assert_eq!(decoded.timezone, "UTC");
    }

    #[test]
    fn test_progress_roundtrip() {
        let progress = Progress {
            rows: 100,
            bytes: 1024,
            total_rows: 1000,
            written_rows: 50,
            written_bytes: 512,
        };

        let mut buffer = BytesMut::new();
        progress.write_to(&mut buffer, 54449).unwrap();

        let mut reader = &buffer[..];
        let decoded = Progress::read_from(&mut reader, 54449).unwrap();

        assert_eq!(decoded.rows, 100);
        assert_eq!(decoded.bytes, 1024);
        assert_eq!(decoded.written_rows, 50);
    }

    #[test]
    fn test_exception_simple() {
        let exc = Exception {
            code: 42,
            name: "UNKNOWN_TABLE".to_string(),
            display_text: "Table doesn't exist".to_string(),
            stack_trace: "at query.cpp:123".to_string(),
            nested: None,
        };

        let mut buffer = BytesMut::new();
        exc.write_to(&mut buffer).unwrap();

        let mut reader = &buffer[..];
        let decoded = Exception::read_from(&mut reader).unwrap();

        assert_eq!(decoded.code, 42);
        assert_eq!(decoded.name, "UNKNOWN_TABLE");
        assert!(decoded.nested.is_none());
    }

    #[test]
    fn test_exception_nested() {
        let nested_exc = Exception {
            code: 1,
            name: "INNER_ERROR".to_string(),
            display_text: "Inner error".to_string(),
            stack_trace: "inner stack".to_string(),
            nested: None,
        };

        let exc = Exception {
            code: 2,
            name: "OUTER_ERROR".to_string(),
            display_text: "Outer error".to_string(),
            stack_trace: "outer stack".to_string(),
            nested: Some(Box::new(nested_exc)),
        };

        let mut buffer = BytesMut::new();
        exc.write_to(&mut buffer).unwrap();

        let mut reader = &buffer[..];
        let decoded = Exception::read_from(&mut reader).unwrap();

        assert_eq!(decoded.code, 2);
        assert!(decoded.nested.is_some());
        assert_eq!(decoded.nested.as_ref().unwrap().code, 1);
    }
}
