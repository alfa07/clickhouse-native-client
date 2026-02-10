//! # ClickHouse Native Client
//!
//! A native async Rust client for ClickHouse database, ported from the C++
//! [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) library.
//!
//! This crate implements the ClickHouse native TCP binary protocol with
//! LZ4/ZSTD compression, TLS support, and all major ClickHouse data types.
//!
//! # Production Readiness
//!
//! Most of the codebase was created by converting the C++ clickhouse-cpp client.
//! Although the client is already used to ingest TiBs of data a day and is
//! relatively well covered by unit tests, there may be bugs. Test your use case
//! before committing.
//!
//! # Quick Start
//!
//! ```no_run
//! use clickhouse_native_client::{Client, ClientOptions, Block};
//! use clickhouse_native_client::column::numeric::ColumnUInt64;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Connect to ClickHouse
//! let opts = ClientOptions::new("localhost", 9000)
//!     .database("default")
//!     .user("default");
//! let mut client = Client::connect(opts).await?;
//!
//! // Execute DDL
//! client.execute("CREATE TABLE IF NOT EXISTS test (id UInt64) ENGINE = Memory").await?;
//!
//! // Insert data
//! let mut col = ColumnUInt64::new();
//! col.append(1);
//! col.append(2);
//! let mut block = Block::new();
//! block.append_column("id", Arc::new(col))?;
//! client.insert("test", block).await?;
//!
//! // Query data
//! let result = client.query("SELECT id FROM test").await?;
//! for block in result.blocks() {
//!     println!("rows: {}", block.row_count());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Feature Flags
//!
//! - **`tls`** - Enables TLS/SSL connections via `rustls` and `tokio-rustls`.
//!
//! # Modules
//!
//! - [`client`] - Async client API (`Client`, `ClientOptions`)
//! - [`block`] - Data blocks (`Block`, `BlockInfo`)
//! - [`mod@column`] - Column types for all ClickHouse data types
//! - [`query`] - Query builder and protocol messages
//! - [`types`] - ClickHouse type system and parser
//! - [`compression`] - LZ4/ZSTD compression
//! - [`protocol`] - Protocol constants (packet types, revisions)
//! - [`error`] - Error types and `Result` alias
//! - [`connection`] - Async TCP/TLS connection wrapper
//! - [`wire_format`] - Wire protocol encoding helpers
//! - [`io`] - Block reader/writer for async I/O
//! - `ssl` - TLS/SSL options (requires `tls` feature)

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![warn(missing_docs)]
/// Data blocks (collections of named columns).
pub mod block;
/// Async client API and connection options.
pub mod client;
/// Column type implementations for all ClickHouse data types.
pub mod column;
/// LZ4 and ZSTD block compression.
pub mod compression;
/// Async TCP/TLS connection wrapper.
pub mod connection;
/// Error types and `Result` alias.
pub mod error;
/// Block reader/writer for async I/O.
pub mod io;
/// Protocol constants (packet types, revision numbers).
pub mod protocol;
/// Query builder and protocol messages.
pub mod query;
/// Re-exports from the connection module.
pub mod socket;
/// ClickHouse type system and type string parser.
pub mod types;
/// Wire protocol encoding helpers (varint, fixed-size types).
pub mod wire_format;

/// TLS/SSL connection options (requires the `tls` feature).
#[cfg(feature = "tls")]
pub mod ssl;

pub use block::{
    Block,
    BlockInfo,
};
pub use client::{
    Client,
    ClientOptions,
    Endpoint,
    QueryResult,
};
pub use connection::ConnectionOptions;
pub use error::{
    Error,
    Result,
};
pub use query::{
    DataCallback,
    DataCancelableCallback,
    Exception,
    ExceptionCallback,
    ExternalTable,
    Profile,
    ProfileCallback,
    ProfileEventsCallback,
    Progress,
    ProgressCallback,
    Query,
    QuerySettingsField,
    ServerLogCallback,
    TracingContext,
};

#[cfg(feature = "tls")]
pub use ssl::SSLOptions;
