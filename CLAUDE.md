# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**ClickHouse Rust Client** - A native async Rust client for ClickHouse database using the TCP protocol (not HTTP). This is a from-scratch implementation converted from the C++ clickhouse-cpp library with comprehensive type support and protocol-correct implementations.

**Status:** Functional with 100% integration test pass rate. Not production-ready (personal learning project).

## Quick Start Commands

### Running Tests

```bash
# Unit tests only (no ClickHouse required)
just test
# OR: cargo test --lib

# Integration tests (requires ClickHouse server)
just start-db         # Start ClickHouse in Docker
just test-integration # Run integration tests
just stop-db         # Stop ClickHouse

# Run all tests (unit + integration)
just test-all

# TLS integration tests
just test-tls         # Handles cert generation + server start/stop automatically

# Run all tests including TLS
just test-all-with-tls
```

### Building and Linting

```bash
# Build
just build           # Debug build
just build-release   # Release build
cargo check          # Fast check without building

# Linting
just fmt             # Format code
just clippy          # Run clippy (fails on warnings)

# Development
just logs            # View ClickHouse logs
just cli             # Open ClickHouse CLI client
```

### Running Individual Tests

```bash
# Run a specific integration test
cargo test --test integration_test test_insert_and_select_data -- --ignored --nocapture

# Run tests matching a pattern (unit tests)
cargo test nullable

# Run a specific unit test
cargo test column::tests::test_column_uint8_basic

# Run per-column integration tests
cargo test --test integration_numeric -- --ignored --nocapture
cargo test --test integration_string -- --ignored --nocapture
cargo test --test integration_array -- --ignored --nocapture
# etc. (see tests/ directory)
```

### Database Management

```bash
# Standard database (port 9000)
just start-db
just stop-db
just clean           # Remove containers and volumes

# TLS database (port 9440)
just generate-certs  # Generate test certificates (one-time)
just start-db-tls
just stop-db-tls
just clean-tls       # Clean TLS data only
just clean-certs     # Remove certificates

# Both servers
just start-db-all    # Start both standard and TLS
just stop-db-all     # Stop both
```

## Architecture

### Design Philosophy: Async-at-Boundaries

The codebase follows an **async-at-boundaries** design pattern:

- **Sync Core**: All data structures are synchronous (types, columns, blocks, compression)
- **Async Boundary**: Connection wrapper + BlockReader/BlockWriter handle async I/O
- **Public API**: Client interface is fully async

**Why this pattern?**
- Simplifies column implementations (no async in trait methods)
- Matches C++ clickhouse-cpp structure closely
- Avoids boxing futures for recursive types (Array, Nullable, etc.)
- Clean separation: data structures vs I/O

### Module Structure

```
src/
├── client.rs              # Public async Client API (connect, query, insert, ping)
├── connection.rs          # Async TCP connection wrapper (read/write primitives)
├── io/
│   ├── block_stream.rs    # BlockReader/BlockWriter (async <-> sync bridge)
│   └── buffer_utils.rs    # Buffer utilities for reading/writing
├── block.rs              # Block (collection of columns with same row count)
├── column/               # All column implementations (SYNC)
│   ├── mod.rs           # Column trait + factory functions
│   ├── numeric.rs       # UInt8-128, Int8-128, Float32/64
│   ├── string.rs        # String, FixedString
│   ├── nullable.rs      # Nullable<T> wrapper
│   ├── array.rs         # Array<T>
│   ├── lowcardinality.rs # LowCardinality<T> (dictionary encoding)
│   ├── date.rs          # Date, Date32, DateTime, DateTime64
│   ├── decimal.rs       # Decimal32/64/128
│   ├── uuid.rs          # UUID
│   ├── ipv4.rs          # IPv4
│   ├── ipv6.rs          # IPv6
│   ├── enum_column.rs   # Enum8, Enum16
│   ├── tuple.rs         # Tuple(T1, T2, ...)
│   ├── map.rs           # Map<K, V>
│   ├── geo.rs           # Point, Ring, Polygon, MultiPolygon
│   └── nothing.rs       # Nothing type
├── query.rs              # Query builder, protocol messages (ClientInfo, ServerInfo, etc.)
├── types/                # Type system
│   ├── mod.rs           # Type enum + TypeCode
│   └── parser.rs        # Parse ClickHouse type strings
├── compression.rs        # LZ4/ZSTD compression (sync)
├── protocol.rs           # Protocol constants (packet types, revisions)
├── wire_format.rs        # Wire protocol encoding helpers (async)
├── error.rs              # Error types
└── ssl.rs               # TLS/SSL support (feature-gated)

tests/
├── integration_test.rs          # Main integration test suite (8 tests)
├── integration_<type>.rs        # Per-column-type integration tests
├── tls_integration_test.rs      # TLS-specific tests (11 tests)
└── *_test.rs                    # Various unit and edge case tests
```

### Key Types

**Client** (`src/client.rs`)
- Main entry point for users
- Methods: `connect()`, `query()`, `insert()`, `ping()`
- Handles protocol handshake, packet routing, exception handling

**Block** (`src/block.rs`)
- Collection of named columns with same row count
- Methods: `append_column()`, `column()`, `column_count()`, `row_count()`

**Column Trait** (`src/column/mod.rs`)
- Base trait for all column types
- Key methods:
  - `load_from_buffer()` / `save_to_buffer()` - Serialize/deserialize
  - `load_prefix()` / `save_prefix()` - For types needing prefix data (LowCardinality, Array)
  - `append_column()`, `slice()`, `clone_empty()`
  - `as_any()` / `as_any_mut()` - Downcasting

**BlockReader/BlockWriter** (`src/io/block_stream.rs`)
- Bridge between async Connection and sync Column types
- Handles compression/decompression
- Manages ClickHouse block format:
  ```
  [BlockInfo][num_columns:varint][num_rows:varint]
  [Column 1: name, type, data]
  [Column 2: name, type, data]
  ...
  ```

## Critical Protocol Knowledge

### Protocol Packet Flow

**ClickHouse TCP Protocol Structure:**
```
[packet_type:varint][payload:varies_by_type]
```

**Key Packet Types** (see `src/protocol.rs`):
- `Hello (0)` - Server handshake response
- `Data (1)` - Block data (compressed if negotiated)
- `Exception (2)` - Server error
- `Progress (3)` - Query progress
- `Pong (4)` - Ping response
- `EndOfStream (5)` - Query/insert completion
- `ProfileInfo (6)` - Query profiling data
- `Log (10)` - Server logs (always uncompressed!)
- `TableColumns (11)` - Column metadata
- `ProfileEvents (14)` - Profile events (always uncompressed!)

### CRITICAL: Stream Alignment Rule

**Every packet payload MUST be fully consumed**, even if you don't need the data.

**Example - Data Packet:**
```rust
// ❌ WRONG - Stream misalignment!
match packet_type {
    ServerCode::Data => break, // BUG: Didn't consume payload!
}

// ✅ CORRECT - Must consume payload
match packet_type {
    ServerCode::Data => {
        // Consume temp table name (if revision >= 50264)
        if self.server_info.revision >= 50264 {
            let _temp_table = self.conn.read_string().await?;
        }
        // Consume the block (even if empty/unused)
        let _block = self.block_reader.read_block(&mut self.conn).await?;
        break; // Now stream is aligned
    }
}
```

**Why?** If you skip payload bytes, the next read will interpret payload data as the next packet type, causing garbage reads and protocol desynchronization.

### INSERT Protocol Flow

```
Client                               Server
  │
  │ Query("INSERT INTO table (cols) VALUES")
  │──────────────────────────────────────>
  │
  │         TableColumns (metadata)
  │<──────────────────────────────────────
  │
  │    Data (empty block = ready signal)
  │<────────────────────────────────────── ⚠️ MUST CONSUME PAYLOAD!
  │
  │      Data (actual data block)
  │──────────────────────────────────────>
  │
  │    Data (empty block = end marker)
  │──────────────────────────────────────>
  │
  │         ProfileEvents (optional)
  │<──────────────────────────────────────
  │
  │             EndOfStream
  │<──────────────────────────────────────
```

**Key Points:**
- Use `INSERT INTO table (col1, col2) VALUES` format (not `FORMAT Native` - that's HTTP!)
- Server sends empty Data packet as readiness signal - **must consume it**
- Send data block, then empty block to signal completion

### Compression Behavior

| Packet Type | Compressed? | Notes |
|-------------|-------------|-------|
| Data (1) | Yes* | If compression negotiated |
| Log (10) | **No** | Always uncompressed! |
| ProfileEvents (14) | **No** | Always uncompressed! |
| TableColumns (11) | No | Metadata |
| Progress (3) | No | Metadata |
| Exception (2) | No | Metadata |

*Compression is negotiated during handshake via ClientOptions.

**Compression Methods:**
- `Lz4` (default) - Fast, good ratio
- `Zstd` - Better ratio, slower
- `None` - No compression

**Both BlockReader and BlockWriter must have same compression setting!**

### Revision-Dependent Fields

Many protocol features depend on server revision:

```rust
const DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES: u64 = 50264;
const DBMS_MIN_REVISION_WITH_BLOCK_INFO: u64 = 51903;
const DBMS_MIN_REVISION_WITH_CUSTOM_SERIALIZATION: u64 = 54454;
```

Always check `server_info.revision` before reading/writing version-dependent fields.

## Column Type Implementation Pattern

When implementing a new column type, follow this pattern:

```rust
use crate::column::{Column, ColumnRef};
use crate::types::Type;
use bytes::{Buf, BufMut, BytesMut};

pub struct ColumnMyType {
    type_: Type,
    data: Vec<MyDataType>,
}

impl ColumnMyType {
    pub fn new(type_: Type) -> Self {
        Self { type_, data: Vec::new() }
    }

    pub fn append(&mut self, value: MyDataType) {
        self.data.push(value);
    }
}

impl Column for ColumnMyType {
    fn column_type(&self) -> &Type { &self.type_ }
    fn size(&self) -> usize { self.data.len() }
    fn clear(&mut self) { self.data.clear(); }
    fn reserve(&mut self, new_cap: usize) { self.data.reserve(new_cap); }

    fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
        // Read `rows` items from buffer
        for _ in 0..rows {
            let value = ...; // Read from buffer
            self.data.push(value);
        }
        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Write all items to buffer
        for value in &self.data {
            // Write to buffer
        }
        Ok(())
    }

    // ... other trait methods
}
```

**Key Points:**
- Columns are **synchronous** - no async in trait methods
- Use `bytes::Buf` / `BufMut` for reading/writing
- Handle errors gracefully (incomplete data, type mismatches)
- Implement `load_prefix()` / `save_prefix()` if needed (e.g., LowCardinality)

### Type Nesting Restrictions

ClickHouse enforces strict nesting rules:

| Invalid | Error | Correct |
|---------|-------|---------|
| `Nullable(Array(...))` | Error 43 | `Array(Nullable(...))` |
| `Nullable(LowCardinality(...))` | Error 43 | `LowCardinality(Nullable(...))` |

**Valid Nesting:**
- ✅ `Array(Nullable(T))`
- ✅ `Array(LowCardinality(T))`
- ✅ `Array(LowCardinality(Nullable(T)))`
- ✅ `LowCardinality(Nullable(T))`

See `src/column/mod.rs` for detailed documentation.

## Testing Strategy

### Test Organization

1. **Unit Tests** - In `src/` files (e.g., `src/column/numeric.rs`)
   - Test individual functions/methods
   - No ClickHouse server required
   - Run with `cargo test --lib`

2. **Integration Tests** - In `tests/` directory
   - Require running ClickHouse server
   - Marked with `#[ignore]` attribute
   - Run with `just test-integration`

3. **Per-Column Integration Tests** - `tests/integration_<type>.rs`
   - Focused tests for each column type
   - Test full roundtrip (create table, insert, select)
   - Easier to debug than monolithic tests

### Writing Integration Tests

```rust
#[tokio::test]
#[ignore] // Requires running ClickHouse
async fn test_my_feature() -> Result<()> {
    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default");

    let mut client = Client::connect(opts).await?;

    // Your test code here

    Ok(())
}
```

Run with: `cargo test --test integration_test test_my_feature -- --ignored --nocapture`

### TLS Testing

TLS tests are feature-gated and in `tests/tls_integration_test.rs`:

```rust
#[cfg(feature = "tls")]
#[tokio::test]
#[ignore]
async fn test_tls_connection() -> Result<()> {
    let ssl_opts = SSLOptions::new()
        .add_ca_cert(PathBuf::from("certs/ca/ca-cert.pem"));

    let opts = ClientOptions::new("localhost", 9440)
        .ssl_options(ssl_opts);

    // ...
}
```

Run with: `just test-tls` (handles cert generation and server lifecycle)

## Common Development Tasks

### Adding a New Column Type

1. Create `src/column/mytype.rs`
2. Implement `Column` trait
3. Add to `src/column/mod.rs` exports and factory function
4. Update `src/io/block_stream.rs` `create_column()` function
5. Add parsing in `src/types/parser.rs`
6. Write unit tests in `src/column/mytype.rs`
7. Create `tests/integration_mytype.rs` for integration tests

### Debugging Protocol Issues

1. **Enable debug logging** - Add `eprintln!()` statements in `src/client.rs`
2. **Log packet types** - Before/after consuming payloads
3. **Compare with C++ clickhouse-cpp** - Look at equivalent functionality in `cpp/` directory
4. **Check stream alignment** - If seeing garbage packet types, payload wasn't consumed
5. **Use hex dumps** - For low-level debugging (see `tests/hex_dump_hello.rs`)

### Performance Optimization

**Benchmarks** are in `benches/` directory:

```bash
# Run benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench select_benchmarks

# Run with flamegraph (requires cargo-flamegraph)
cargo flamegraph --bench select_benchmarks
```

**Optimization areas:**
- Block size tuning (default: varies by use case)
- Compression method selection (LZ4 vs ZSTD vs None)
- Connection pooling (not yet implemented)
- Batch operations (partially implemented)

## Known Limitations

1. **Array Type Uncompressed Reading** - Returns error for uncompressed Arrays (complex offset handling needed). Workaround: Arrays work fine in compressed mode.

2. **Enum Parsing** - Uses storage type (Int8/Int16), missing name-to-value mapping. Values work correctly.

3. **Advanced Types** - Tuple/Map/LowCardinality partially implemented, may have edge cases.

4. **Query Cancellation** - Not implemented (ClientCode::Cancel packet).

5. **Connection Pooling** - Single connection only.

6. **LowCardinality in Map Keys** - Not supported in ClickHouse itself (see LOWCARDINALITY_ANALYSIS.md).

## Project-Specific Conventions

### Error Handling

- Use `Result<T>` (aliased to `Result<T, crate::Error>`)
- Protocol errors are unrecoverable (stream corrupt, must close connection)
- Server exceptions converted to `Error::Protocol` with details
- I/O errors wrapped in `Error::Io`

### Code Style

- Follow standard Rust conventions (`rustfmt.toml` in root)
- Use meaningful variable names (avoid single letters except in small scopes)
- Document public APIs with `///` comments
- Add examples in doc comments when helpful
- No emojis in code (only in documentation)

### Commit Messages

From user's global instructions:
- Never add "Generated by Claude Code" or "Co-Authored-By: Claude" to commits
- Follow existing commit message style (check `git log` for patterns)

## Helpful Resources

**ClickHouse Documentation:**
- [Native Protocol Specification](https://clickhouse.com/docs/en/native-protocol/)
- [Data Types](https://clickhouse.com/docs/en/sql-reference/data-types/)
- [TCP Protocol Details](https://clickhouse.com/docs/en/interfaces/tcp/)

**Reference Implementation:**
- `cpp/clickhouse-cpp/` - C++ reference client (invaluable for protocol details)
- Check `cpp/clickhouse-cpp/clickhouse/client.cpp` for protocol flows

**Project Documentation:**
- `README.md` - Quick start and usage
- `IMPLEMENTATION_STATUS.md` - Current status and roadmap
- `LOWCARDINALITY_ANALYSIS.md` - LowCardinality type insights
- `BENCHMARK_RESULTS.md` - Performance benchmarks

## Troubleshooting

### Tests Failing with "Connection refused"

```bash
# Check if ClickHouse is running
docker ps | grep clickhouse

# Start ClickHouse
just start-db

# Check logs
just logs
```

### "Unknown packet type" or Garbage Values

- **Likely cause:** Stream misalignment (packet payload not consumed)
- **Fix:** Ensure all packet handlers consume their payloads
- **Debug:** Add logging before/after payload consumption

### Compression Errors

- **"Unknown compression method: 0x53"** - Reading 'S' (String type) as compression byte
- **Likely cause:** BlockWriter compression not enabled, but BlockReader expects it
- **Fix:** Enable compression on both reader and writer

### TLS Connection Issues

```bash
# Regenerate certificates
just clean-certs
just generate-certs

# Restart TLS server
just stop-db-tls
just start-db-tls

# Check server logs
docker-compose logs clickhouse-tls
```

## Quick Reference: Important File Locations

**Protocol Constants:**
- `src/protocol.rs` - Packet types, compression methods, revision constants

**Client Implementation:**
- `src/client.rs:481-608` - INSERT implementation
- `src/client.rs:227-298` - Query response loop

**Block I/O:**
- `src/io/block_stream.rs:37-125` - Block reading
- `src/io/block_stream.rs:377-407` - Block writing

**Type System:**
- `src/types/parser.rs` - Type string parsing
- `src/column/mod.rs` - Column trait + factory

**Main Integration Tests:**
- `tests/integration_test.rs` - 8 core integration tests
- `tests/tls_integration_test.rs` - 11 TLS tests
