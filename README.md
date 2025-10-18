# ClickHouse Rust Client

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
just start-db           # Start ClickHouse in Docker
just stop-db            # Stop ClickHouse container
just clean              # Clean up containers and volumes
just test               # Run unit tests only
just test-integration   # Run integration tests
just test-all           # Run all tests (starts/stops DB automatically)
just build              # Build the project
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
