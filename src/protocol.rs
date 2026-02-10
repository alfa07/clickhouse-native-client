/// Types of packets received from server
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum ServerCode {
    /// Server handshake response containing name, version, and revision.
    Hello = 0,
    /// Block of data, may be compressed.
    Data = 1,
    /// Exception that occurred during query execution.
    Exception = 2,
    /// Query execution progress: rows and bytes read.
    Progress = 3,
    /// Response to a client Ping request.
    Pong = 4,
    /// Signals that all packets for the current operation have been sent.
    EndOfStream = 5,
    /// Profiling data for query execution.
    ProfileInfo = 6,
    /// Block of totals, may be compressed.
    Totals = 7,
    /// Block of extremes (mins and maxs), may be compressed.
    Extremes = 8,
    /// Response to a TableStatus request.
    TablesStatusResponse = 9,
    /// Query execution log (always uncompressed).
    Log = 10,
    /// Columns description for default values calculation.
    TableColumns = 11,
    /// List of unique part UUIDs.
    PartUUIDs = 12,
    /// Request for the next distributed read task.
    ReadTaskRequest = 13,
    /// Profile events from the server (always uncompressed).
    ProfileEvents = 14,
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
    /// Client handshake containing name, version, and default database.
    Hello = 0,
    /// Query packet with query id, settings, stage, compression, and query
    /// text.
    Query = 1,
    /// Data block (e.g. INSERT data), may be compressed.
    Data = 2,
    /// Cancel the currently running query.
    Cancel = 3,
    /// Ping the server to check the connection is alive.
    Ping = 4,
}

/// Should we compress Blocks of data
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum CompressionState {
    /// Block compression is disabled.
    Disable = 0,
    /// Block compression is enabled.
    Enable = 1,
}

/// Query processing stage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum Stage {
    /// Fully process the query and return the final result.
    Complete = 2,
}

/// Methods of block compression
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionMethod {
    /// No compression (default).
    #[default]
    None = -1,
    /// LZ4 compression -- fast with good compression ratio.
    Lz4 = 1,
    /// ZSTD compression -- better ratio but slower than LZ4.
    Zstd = 2,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
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
