pub mod block;
pub mod client;
pub mod column;
pub mod compression;
pub mod connection;
pub mod error;
pub mod io;
pub mod protocol;
pub mod query;
pub mod socket;
pub mod types;
pub mod wire_format;

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
    ServerLogCallback,
    TracingContext,
};

#[cfg(feature = "tls")]
pub use ssl::SSLOptions;
