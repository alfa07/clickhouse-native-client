# ClickHouse Rust Client

[![CI](https://github.com/alfa07/clickhouse-native-client/workflows/CI/badge.svg)](https://github.com/alfa07/clickhouse-native-client/actions)

🤡 ⚠️ **Do not use in production! This is me clowning around with Claude Code**

A native Rust client for ClickHouse database, converted from the C++ clickhouse-cpp library.

## Features

- ✅ Async-first design using tokio
- ✅ Native TCP protocol implementation
- ✅ LZ4 and ZSTD compression support
- ✅ Type-safe column operations
- ✅ Support for String, numeric (UInt8-64, Int8-64, Float32/64), Nullable, and Array types
- ✅ Query execution and data insertion
- ✅ Comprehensive test coverage (84+ unit tests)

## Architecture

**Async-at-Boundaries Design:**
- **Sync Core**: All data structures (types, columns, blocks, compression)
- **Async Boundary**: Connection wrapper + BlockReader/BlockWriter
- **Public API**: Async Client interface

## Quick Start

```rust
use clickhouse_client::{Client, ClientOptions, Query};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to ClickHouse
    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default");

    let mut client = Client::connect(opts).await?;

    // Execute a query
    let result = client.query("SELECT number FROM system.numbers LIMIT 10").await?;
    println!("Got {} rows", result.total_rows());

    // Ping the server
    client.ping().await?;

    Ok(())
}
```

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
clickhouse-client = { path = "." }
tokio = { version = "1", features = ["full"] }
```

## Development

### Prerequisites

- Rust 1.70+ (for async-await support)
- Docker and Docker Compose (for integration tests)
- `just` command runner (install via `cargo install just`)

### Running Tests

**Unit tests only:**
```bash
cargo test --lib
```

**Integration tests (requires ClickHouse):**
```bash
# Start ClickHouse in Docker
just start-db

# Run integration tests
just test-integration

# Or run everything together
just test-all

# Stop ClickHouse
just stop-db
```

### Available Just Commands

```bash
just --list              # Show all available commands

# Standard Database Commands
just start-db           # Start ClickHouse in Docker (port 9000)
just stop-db            # Stop ClickHouse container
just clean              # Clean up containers and volumes

# TLS Database Commands
just generate-certs     # Generate test certificates for TLS
just start-db-tls       # Start TLS-enabled ClickHouse (port 9440)
just stop-db-tls        # Stop TLS ClickHouse container
just start-db-all       # Start both standard and TLS servers
just stop-db-all        # Stop all servers
just clean-tls          # Clean TLS data only
just clean-certs        # Remove generated certificates

# Testing Commands
just test               # Run unit tests only
just test-integration   # Run integration tests (non-TLS)
just test-tls           # Run TLS integration tests
just test-all           # Run all tests (unit + integration, no TLS)
just test-all-with-tls  # Run ALL tests including TLS

# Development Commands
just build              # Build the project
just build-release      # Build release version
just clippy             # Run clippy linter
just fmt                # Format code
just logs               # View ClickHouse logs
just cli                # Open ClickHouse CLI client
```

## Integration Tests

The integration test suite covers:
- Connection and ping
- Database creation
- Table creation with String, UInt64, Float64 columns
- Data insertion (both SQL INSERT and block-based)
- SELECT queries with WHERE clauses
- Aggregation queries (COUNT, SUM, AVG)
- Cleanup operations

Run with:
```bash
just test-all
```

Or manually:
```bash
# Start ClickHouse
docker-compose up -d

# Wait for ready
sleep 5

# Run tests
cargo test --test integration_test -- --ignored --nocapture

# Cleanup
docker-compose down
```

## TLS Integration Testing

### Overview

The client supports TLS/SSL connections with comprehensive testing infrastructure:

- ✅ Self-signed certificate generation for testing
- ✅ Separate TLS-enabled ClickHouse server (port 9440)
- ✅ 11 comprehensive TLS integration tests
- ✅ Feature-gated with `#[cfg(feature = "tls")]`
- ✅ Automated setup with `just` commands

### Quick Start

```bash
# Generate test certificates (one-time setup)
just generate-certs

# Start TLS-enabled ClickHouse
just start-db-tls

# Run TLS tests
just test-tls

# Or run everything in one command
cargo test --features tls --test tls_integration_test -- --ignored --nocapture
```

### Test Coverage

The TLS test suite includes:

1. **Basic TLS Connection** - Connect with custom CA certificate
2. **SNI Support** - Test with and without Server Name Indication
3. **Query Execution** - Execute queries over TLS
4. **Data Operations** - CREATE TABLE, INSERT, SELECT over TLS
5. **Ping Operations** - Multiple pings over secure connection
6. **Multiple Queries** - Sequential query execution
7. **Endpoint Failover** - TLS with multiple endpoints
8. **Connection Timeout** - Timeout behavior with TLS
9. **Mutual TLS** - Client certificate authentication
10. **Aggregation Queries** - COUNT, SUM, AVG over TLS
11. **Table Management** - Full CRUD operations over TLS

### Certificate Infrastructure

The test certificates are automatically generated with:

```bash
just generate-certs
```

This creates:

```
certs/
├── ca/
│   ├── ca-cert.pem          # CA certificate (for client trust)
│   └── ca-key.pem           # CA private key
├── server/
│   ├── server-cert.pem      # Server certificate
│   ├── server-key.pem       # Server private key
│   └── dhparam.pem          # DH parameters
└── client/
    ├── client-cert.pem      # Client certificate (mutual TLS)
    └── client-key.pem       # Client private key
```

**Certificate Details:**
- **Validity**: 10 years (testing only!)
- **Algorithm**: RSA 4096-bit
- **Server CN**: localhost
- **SANs**: localhost, clickhouse-server-tls, 127.0.0.1, ::1
- **Signed by**: Self-signed CA

### Manual TLS Testing

Start TLS server manually:
```bash
# Generate certificates if not already done
just generate-certs

# Start TLS server
docker-compose up -d clickhouse-tls

# Check logs
docker-compose logs -f clickhouse-tls

# Test with clickhouse-client (from host)
clickhouse-client --secure --port 9440 --query "SELECT 1"
```

### Using TLS in Your Code

```rust
use clickhouse_client::{Client, ClientOptions, SSLOptions};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure SSL
    let ssl_opts = SSLOptions::new()
        .add_ca_cert(PathBuf::from("certs/ca/ca-cert.pem"))
        .use_system_certs(false)
        .use_sni(true);

    // Create client with TLS
    let opts = ClientOptions::new("localhost", 9440)
        .database("default")
        .user("default")
        .ssl_options(ssl_opts);

    let mut client = Client::connect(opts).await?;
    client.ping().await?;

    println!("Connected via TLS!");
    Ok(())
}
```

### Troubleshooting TLS

**Connection refused:**
```bash
# Check if TLS server is running
docker ps | grep clickhouse-server-tls

# Check server logs
docker-compose logs clickhouse-tls

# Verify certificates exist
ls -la certs/ca/ca-cert.pem certs/server/server-cert.pem
```

**Certificate errors:**
```bash
# Regenerate certificates
just clean-certs
just generate-certs
just start-db-tls
```

**Port conflicts:**
```bash
# Check if port 9440 is in use
lsof -i :9440

# Stop all ClickHouse containers
just stop-db-all
```

## Project Structure

```
src/
├── client.rs           # High-level async Client API
├── connection.rs       # Async TCP connection wrapper
├── io/
│   └── block_stream.rs # BlockReader/BlockWriter (async I/O bridge)
├── block.rs           # Block data structure (sync)
├── column/            # Column implementations (sync)
│   ├── mod.rs         # Column trait
│   ├── numeric.rs     # Numeric columns (UInt*, Int*, Float*)
│   ├── string.rs      # String and FixedString
│   ├── nullable.rs    # Nullable wrapper
│   └── array.rs       # Array columns
├── query.rs           # Query builder and protocol messages
├── types/             # Type system
├── compression.rs     # LZ4/ZSTD compression (sync)
├── protocol.rs        # Protocol constants
├── wire_format.rs     # Wire protocol encoding (async)
└── error.rs           # Error types

tests/
└── integration_test.rs # Integration tests

clickhouse-config/     # ClickHouse configuration
docker-compose.yml     # Docker setup
justfile              # Task runner scripts
```

## Type Support

Currently supported ClickHouse types:
- Numeric: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64
- String: String, FixedString(N)
- Nullable: Nullable(T)
- Array: Array(T)
- Date: Date, DateTime

## License

This project is a Rust port of the clickhouse-cpp C++ library.

## Contributing

Contributions are welcome! Please ensure:
1. All unit tests pass: `cargo test --lib`
2. Integration tests pass: `just test-all`
3. Code is formatted: `just fmt`
4. No clippy warnings: `just clippy`
