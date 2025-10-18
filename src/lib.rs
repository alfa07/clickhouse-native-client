pub mod error;
pub mod protocol;
pub mod types;
pub mod wire_format;
pub mod io;
pub mod compression;
pub mod connection;
pub mod socket;
pub mod column;
pub mod block;
pub mod query;
pub mod client;

#[cfg(feature = "tls")]
pub mod ssl;

pub use error::{Error, Result};
pub use block::{Block, BlockInfo};
pub use client::{Client, ClientOptions, Endpoint, QueryResult};
pub use query::{
    Query, Progress, Profile, TracingContext, Exception,
    ProgressCallback, ProfileCallback, ProfileEventsCallback,
    ServerLogCallback, ExceptionCallback, DataCallback, DataCancelableCallback,
};
pub use connection::ConnectionOptions;

#[cfg(feature = "tls")]
pub use ssl::SSLOptions;
