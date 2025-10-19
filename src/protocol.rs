/// Types of packets received from server
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum ServerCode {
    Hello = 0,                // Name, version, revision
    Data = 1,                 // Block of data, may be compressed
    Exception = 2,            // Exception during query execution
    Progress = 3,             /* Query execution progress: rows and bytes
                               * read */
    Pong = 4,                 // Response to Ping
    EndOfStream = 5,          // All packets were sent
    ProfileInfo = 6,          // Profiling data
    Totals = 7,               // Block of totals, may be compressed
    Extremes = 8,             // Block of mins and maxs, may be compressed
    TablesStatusResponse = 9, // Response to TableStatus
    Log = 10,                 // Query execution log
    TableColumns = 11,        // Columns' description for default values
    PartUUIDs = 12,           // List of unique parts ids
    ReadTaskRequest = 13,     // UUID describes a request for next task
    ProfileEvents = 14,       // Packet with profile events from server
}

impl TryFrom<u64> for ServerCode {
    type Error = crate::Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ServerCode::Hello),
            1 => Ok(ServerCode::Data),
            2 => Ok(ServerCode::Exception),
            3 => Ok(ServerCode::Progress),
            4 => Ok(ServerCode::Pong),
            5 => Ok(ServerCode::EndOfStream),
            6 => Ok(ServerCode::ProfileInfo),
            7 => Ok(ServerCode::Totals),
            8 => Ok(ServerCode::Extremes),
            9 => Ok(ServerCode::TablesStatusResponse),
            10 => Ok(ServerCode::Log),
            11 => Ok(ServerCode::TableColumns),
            12 => Ok(ServerCode::PartUUIDs),
            13 => Ok(ServerCode::ReadTaskRequest),
            14 => Ok(ServerCode::ProfileEvents),
            _ => Err(crate::Error::Protocol(format!(
                "Unknown server code: {}",
                value
            ))),
        }
    }
}

/// Types of packets sent by client
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum ClientCode {
    Hello = 0,  // Name, version, default database name
    Query = 1,  // Query id, settings, stage, compression, and query text
    Data = 2,   // Data Block (e.g. INSERT data), may be compressed
    Cancel = 3, // Cancel query
    Ping = 4,   // Check server connection
}

/// Should we compress Blocks of data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum CompressionState {
    Disable = 0,
    Enable = 1,
}

/// Query processing stage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum Stage {
    Complete = 2,
}

/// Methods of block compression
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionMethod {
    None = -1,
    LZ4 = 1,
    ZSTD = 2,
}

impl Default for CompressionMethod {
    fn default() -> Self {
        CompressionMethod::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_code_conversion() {
        assert_eq!(ServerCode::try_from(0).unwrap(), ServerCode::Hello);
        assert_eq!(ServerCode::try_from(1).unwrap(), ServerCode::Data);
        assert_eq!(
            ServerCode::try_from(14).unwrap(),
            ServerCode::ProfileEvents
        );
        assert!(ServerCode::try_from(99).is_err());
    }

    #[test]
    fn test_compression_method_default() {
        assert_eq!(CompressionMethod::default(), CompressionMethod::None);
    }
}
