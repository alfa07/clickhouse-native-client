//! Error types for the ClickHouse client.
//!
//! All fallible operations in this crate return [`Result<T>`], which is an
//! alias for `std::result::Result<T, Error>`.

use thiserror::Error;

/// Errors that can occur when using the ClickHouse client.
#[derive(Error, Debug)]
pub enum Error {
    /// An I/O error occurred on the underlying TCP or TLS connection.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Failed to establish a connection to the ClickHouse server.
    #[error("Connection error: {0}")]
    Connection(String),

    /// A protocol-level error, such as an unexpected packet type or
    /// malformed data from the server.
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// An error during LZ4 or ZSTD compression/decompression.
    #[error("Compression error: {0}")]
    Compression(String),

    /// A type mismatch between expected and actual column types.
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        /// The type that was expected.
        expected: String,
        /// The type that was received.
        actual: String,
    },

    /// A validation error, such as mismatched row counts in a block.
    #[error("Validation error: {0}")]
    Validation(String),

    /// An error returned by the ClickHouse server (exception).
    #[error("Server error {code}: {message}")]
    Server {
        /// ClickHouse error code.
        code: i32,
        /// Error message from the server.
        message: String,
    },

    /// A feature or type that has not been implemented yet.
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// An invalid argument was provided to a function.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// A write exceeded the available buffer capacity.
    #[error("Buffer overflow")]
    BufferOverflow,

    /// Invalid UTF-8 was encountered when reading a string.
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
}

/// A type alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
