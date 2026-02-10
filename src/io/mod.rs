//! I/O module for block streaming between the client and ClickHouse server.
//!
//! Provides `BlockReader` and `BlockWriter` which bridge async network I/O
//! with the synchronous column serialization/deserialization layer.

/// Block reader/writer for streaming data between client and server.
pub mod block_stream;
pub mod buffer_utils;

pub use block_stream::{
    BlockReader,
    BlockWriter,
};
