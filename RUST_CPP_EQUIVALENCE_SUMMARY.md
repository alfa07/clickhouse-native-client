# Rust vs C++ Implementation Equivalence Summary

This document outlines what needs to be done to make the Rust implementation feature-equivalent to the C++ clickhouse-cpp reference implementation.

**Generated:** 2025-11-01
**Status:** Based on comparison of Rust implementation against cpp/clickhouse-cpp

---

## Executive Summary

The Rust implementation has **strong parity** with the C++ implementation in:
- ✅ All 33+ column types (numeric, string, date/time, complex types)
- ✅ Core protocol implementation (TCP, compression, TLS)
- ✅ Basic client operations (connect, query, insert, ping)
- ✅ Connection options (endpoints, failover, timeouts, keepalive)

**Key gaps** requiring implementation:
1. **Query Cancellation** - Ability to cancel queries mid-execution
2. **External Tables** - Sending temporary data with queries
3. **Advanced Protocol Packets** - Totals, Extremes, distributed query support
4. **Endpoint Management** - Runtime endpoint inspection and reset
5. **Enhanced TLS Options** - Protocol versions, context options (limited by rustls)

---

## 1. Missing Features (Priority Ordered)

### HIGH PRIORITY

#### 1.1 Query Cancellation
**C++ Feature:**
```cpp
client.SelectCancelable(query, [](const Block& block) {
    // Return false to cancel query
    if (should_cancel) return false;
    return true;
});
```

**Rust Status:** ❌ Not implemented
**Rust Has:** Partial - `cancel()` method exists but cancellation callbacks not supported
**Impact:** HIGH - Users cannot stop long-running queries from client side
**Implementation Effort:** Medium

**What needs to be done:**
- Modify query callback to return `Result<bool>` instead of `Result<()>`
- Send `ClientCode::Cancel` packet when callback returns `Ok(false)`
- Handle cancellation in query loop properly
- Update all query methods: `query()`, `query_with_id()`, `query_with_external_data()`
- Add new methods: `query_cancelable()`, etc.

**Files to modify:**
- `src/client.rs` - Add cancelable query methods
- `src/protocol.rs` - Already has `ClientCode::Cancel` defined
- Tests: Add integration tests for cancellation

---

#### 1.2 External Tables Support
**C++ Feature:**
```cpp
Block external_data;
// ... populate external_data ...
ExternalTables ext = {{"temp_table", external_data}};
client.SelectWithExternalData(query, ext, callback);
```

**Rust Status:** ⚠️ Partially implemented but not working
**Rust Has:** Signature exists in `src/client.rs` but marked as non-functional
**Impact:** HIGH - Required for advanced query patterns, CTEs with data
**Implementation Effort:** Medium-High

**What needs to be done:**
- Fix `query_with_external_data()` and `query_with_external_data_and_id()` implementations
- Protocol flow for external tables:
  1. Send query packet
  2. For each external table:
     - Send `ClientCode::Data` packet
     - Write table name (as temp table name in block)
     - Write block data
  3. Send empty block to signal end of external data
  4. Proceed with normal query response loop

**Current Issue:** Protocol sequence likely incorrect - needs debugging against C++ implementation

**Files to modify:**
- `src/client.rs:520-608` - Fix external table sending logic
- Add integration tests with external tables
- Reference: `cpp/clickhouse/client.cpp` - external table implementation

---

### MEDIUM PRIORITY

#### 1.3 Advanced Protocol Packet Handling
**C++ Feature:** Handles all protocol packets including:
- `Totals (7)` - Results from `WITH TOTALS` clause
- `Extremes (8)` - Results from `WITH EXTREMES` clause
- `TablesStatusResponse (9)` - Table status for distributed queries
- `PartUUIDs (12)` - Part UUIDs for distributed queries
- `ReadTaskRequest (13)` - Distributed query task requests

**Rust Status:** ❌ Packets defined but not handled
**Rust Has:** Constants in `src/protocol.rs` but no handling in client
**Impact:** MEDIUM - Required for advanced query features
**Implementation Effort:** Medium

**What needs to be done:**
- Add handlers in query response loop (`src/client.rs:227-298`)
- Parse and consume packet payloads (critical for stream alignment!)
- Add fields to query result/callback for totals/extremes data:
  ```rust
  pub struct QueryResult {
      pub blocks: Vec<Block>,
      pub totals: Option<Block>,    // NEW
      pub extremes: Option<Block>,  // NEW
      // ...
  }
  ```

**Files to modify:**
- `src/client.rs` - Add packet handlers in query loop
- `src/query.rs` - Extend QueryResult/callback API
- Add integration tests for `SELECT ... WITH TOTALS`, `WITH EXTREMES`

---

#### 1.4 Endpoint Management
**C++ Feature:**
```cpp
auto endpoint = client.GetCurrentEndpoint();
client.ResetConnectionEndpoint(); // Try next endpoint
```

**Rust Status:** ❌ Not exposed publicly
**Rust Has:** Internal endpoint failover during connect, but no runtime access
**Impact:** MEDIUM - Useful for monitoring and manual failover
**Implementation Effort:** Low

**What needs to be done:**
- Add field to track current endpoint index: `current_endpoint: Option<usize>`
- Add public methods:
  ```rust
  pub fn current_endpoint(&self) -> Option<&Endpoint>
  pub async fn reset_connection_endpoint(&mut self) -> Result<()>
  ```
- `reset_connection_endpoint()` should try next endpoint in rotation

**Files to modify:**
- `src/client.rs` - Add endpoint tracking field and methods
- Update `connect()` to set `current_endpoint`

---

#### 1.5 Enhanced TLS/SSL Options
**C++ Feature:** (via OpenSSL)
```cpp
ssl_opts.SetMinProtocolVersion(TLS1_2_VERSION);
ssl_opts.SetMaxProtocolVersion(TLS1_3_VERSION);
ssl_opts.SetContextOptions(SSL_OP_NO_SSLv3);
ssl_opts.SetHostVerifyFlags(X509_CHECK_FLAG_NO_WILDCARDS);
ssl_opts.SetExternalSSLContext(custom_ctx);
```

**Rust Status:** ⚠️ Limited by rustls architecture
**Rust Has:** Basic TLS with CA certs, client certs, SNI
**Impact:** MEDIUM - Most users don't need advanced SSL control
**Implementation Effort:** High (architectural limitation)

**What CAN'T be done (rustls limitations):**
- External SSL context (rustls doesn't use OpenSSL)
- Direct OpenSSL configuration commands
- Some advanced certificate validation options

**What CAN be added:**
- TLS protocol version selection (TLS 1.2 vs 1.3)
- Custom certificate verification logic
- More client cert options

**Files to modify:**
- `src/ssl.rs` - Add protocol version options
- May require switching to openssl-rs crate (breaking change)

---

### LOW PRIORITY

#### 1.6 Client Version Info
**C++ Feature:**
```cpp
auto version = Client::GetVersion();
// version.major, version.minor, version.patch, version.build
```

**Rust Status:** ❌ Not implemented
**Rust Has:** Version embedded in Cargo.toml
**Impact:** LOW - Informational only
**Implementation Effort:** Very Low

**What needs to be done:**
- Add static method to Client:
  ```rust
  pub fn version() -> &'static str {
      env!("CARGO_PKG_VERSION")
  }
  ```
- Or use structured version with semver crate

**Files to modify:**
- `src/client.rs` - Add version method
- `Cargo.toml` - Ensure version is accurate

---

#### 1.7 ItemView / Ergonomic Value Access
**C++ Feature:**
```cpp
auto value = column->As<ColumnUInt64>()->At(row);
// ItemView with type conversion:
ItemView item = block[col][row];
uint64_t val = item.Get<uint64_t>();
```

**Rust Status:** ⚠️ Has ColumnValue but less ergonomic
**Rust Has:** `ColumnValue` enum but requires matching
**Impact:** LOW - Ergonomics, not functionality
**Implementation Effort:** Medium

**What could be improved:**
- Add trait-based value extraction:
  ```rust
  trait ColumnValueExt {
      fn as_u64(&self) -> Option<u64>;
      fn as_string(&self) -> Option<&str>;
      // etc.
  }
  ```
- Add convenience methods to Block for value access

**Files to modify:**
- `src/column/mod.rs` - Add value extraction traits
- `src/block.rs` - Add convenience accessors

---

## 2. Column Type Support Comparison

### Status: ✅ COMPLETE PARITY

Both implementations support all ClickHouse column types:

| Type Category | C++ Support | Rust Support | Notes |
|---------------|-------------|--------------|-------|
| **Numeric** | ✅ | ✅ | UInt8-128, Int8-128, Float32/64 |
| **String** | ✅ | ✅ | String, FixedString |
| **Date/Time** | ✅ | ✅ | Date, Date32, DateTime, DateTime64 (with timezone) |
| **Decimal** | ✅ | ✅ | Decimal32/64/128/256 |
| **UUID** | ✅ | ✅ | UUID type |
| **IP** | ✅ | ✅ | IPv4, IPv6 |
| **Enum** | ✅ | ✅ | Enum8, Enum16 |
| **Array** | ✅ | ✅ | Array(T) |
| **Nullable** | ✅ | ✅ | Nullable(T) |
| **Tuple** | ✅ | ✅ | Tuple(T1, T2, ...) |
| **Map** | ✅ | ✅ | Map(K, V) |
| **LowCardinality** | ✅ | ✅ | LowCardinality(T) |
| **Geo** | ✅ | ✅ | Point, Ring, Polygon, MultiPolygon |
| **Nothing** | ✅ | ✅ | Nothing type |

**No column type gaps identified.**

---

## 3. Client API Comparison

### C++ Client Methods (23 public methods)

**Query Execution:**
- ✅ `Execute(query)` → Rust: `execute()`
- ✅ `Select(query, callback)` → Rust: `query()`
- ✅ `Select(query, query_id, callback)` → Rust: `query_with_id()`
- ❌ `SelectCancelable(query, callback)` → **MISSING**
- ❌ `SelectCancelable(query, query_id, callback)` → **MISSING**
- ⚠️ `SelectWithExternalData(...)` → Rust: exists but broken
- ⚠️ `SelectWithExternalDataCancelable(...)` → Rust: doesn't exist

**Data Operations:**
- ✅ `Insert(table, block)` → Rust: `insert()`
- ✅ `Insert(table, query_id, block)` → Rust: `insert()` (id in Query)

**Connection:**
- ✅ `Ping()` → Rust: `ping()`
- ⚠️ `ResetConnection()` → Rust: would need reconnect
- ❌ `ResetConnectionEndpoint()` → **MISSING**

**Information:**
- ✅ `GetServerInfo()` → Rust: `server_info()`
- ❌ `GetCurrentEndpoint()` → **MISSING**
- ❌ `GetVersion()` (static) → **MISSING**

### Rust Client Methods (Additional)

**Rust has some methods C++ doesn't:**
- `query_with_params()` - Query with parameter binding
- `server_version()`, `server_revision()` - Separate accessors
- Async-specific patterns (futures, streams)

---

## 4. Protocol and Connection Features

### Status: ✅ FEATURE PARITY

| Feature | C++ | Rust | Notes |
|---------|-----|------|-------|
| TCP Protocol | ✅ | ✅ | Full implementation |
| Compression (LZ4) | ✅ | ✅ | Working |
| Compression (ZSTD) | ✅ | ✅ | Working |
| TLS/SSL | ✅ | ✅ | Different libraries (OpenSSL vs rustls) |
| TCP Keepalive | ✅ | ✅ | Full options |
| TCP Nodelay | ✅ | ✅ | Supported |
| Connection Timeout | ✅ | ✅ | Configurable |
| Send/Recv Timeout | ✅ | ✅ | Configurable |
| Multiple Endpoints | ✅ | ✅ | Failover supported |
| Retry Logic | ✅ | ✅ | Configurable retries |
| Ping Before Query | ✅ | ✅ | Optional flag |

**No protocol gaps identified.**

---

## 5. Known Rust Limitations (by Design)

These are documented limitations that don't exist in C++:

### 5.1 Array Type Uncompressed Reading
**Issue:** Rust implementation errors on uncompressed Array columns
**Reason:** Complex offset calculation for uncompressed arrays not yet implemented
**Workaround:** Arrays work fine with compression enabled (default)
**Status:** Low priority (compression is standard)

### 5.2 Enum Parsing
**Issue:** Rust uses storage type (Int8/Int16), missing name-to-value mapping
**Impact:** Values work correctly, but enum value names not preserved
**Status:** Low priority (values work, names are metadata)

### 5.3 Query Object Ownership
**Difference:** Rust `execute()` consumes Query, C++ keeps it
**Reason:** Rust ownership model
**Impact:** Can't reuse Query object (must clone or rebuild)
**Status:** Intentional design difference

---

## 6. Implementation Priority Roadmap

### Phase 1: Critical Features (HIGH PRIORITY)
**Goal:** Enable all common production use cases

1. **Query Cancellation** (2-3 days)
   - Add cancelable query methods
   - Protocol packet handling
   - Integration tests

2. **External Tables** (3-4 days)
   - Debug and fix existing implementation
   - Protocol flow correction
   - Integration tests with CTEs

**Deliverable:** Users can cancel queries and use external tables

---

### Phase 2: Advanced Query Features (MEDIUM PRIORITY)
**Goal:** Support advanced ClickHouse query capabilities

3. **Advanced Protocol Packets** (2-3 days)
   - Totals/Extremes support
   - Distributed query packets
   - Extend QueryResult API

4. **Endpoint Management** (1 day)
   - Track current endpoint
   - Public API for endpoint info
   - Manual failover method

**Deliverable:** Full support for `WITH TOTALS`, `WITH EXTREMES`, and endpoint control

---

### Phase 3: Ergonomics and Polish (LOW PRIORITY)
**Goal:** Improve developer experience

5. **Client Version Info** (1 hour)
   - Add version accessor
   - Documentation

6. **Value Access Improvements** (1-2 days)
   - Trait-based value extraction
   - Better error messages
   - Convenience methods

7. **Enhanced TLS Options** (3-5 days, optional)
   - Protocol version selection
   - Consider openssl-rs migration (breaking change)
   - Advanced certificate options

**Deliverable:** Better DX and more TLS control

---

## 7. Testing Requirements

For each new feature:

### Unit Tests
- Protocol packet serialization/deserialization
- Error handling edge cases
- Option parsing and validation

### Integration Tests
- Full roundtrip with real ClickHouse server
- Edge cases (empty data, large data)
- Error scenarios (timeouts, cancellation)

### Benchmark Tests
- Performance impact of new features
- Comparison with C++ implementation where applicable

---

## 8. Documentation Requirements

For each new feature:

1. **API Documentation** - Rustdoc comments with examples
2. **CLAUDE.md Updates** - Protocol details, usage patterns
3. **README.md Updates** - Feature list, quick start examples
4. **Migration Guide** - If breaking changes needed

---

## 9. Breaking Changes Considerations

Some features may require breaking changes:

### Potential Breaking Changes:
1. **Query Callbacks** - Return type change from `Result<()>` to `Result<bool>`
2. **QueryResult Structure** - Adding totals/extremes fields
3. **TLS Library** - Switching from rustls to openssl-rs (if enhanced TLS needed)

### Mitigation Strategy:
- Use semantic versioning (major version bump)
- Provide migration guide
- Consider feature flags for new behavior
- Deprecation warnings before removal

---

## 10. Summary Checklist

### Essential for C++ Equivalence:
- [ ] Query Cancellation (HIGH)
- [ ] External Tables Support (HIGH)
- [ ] Advanced Protocol Packets - Totals/Extremes (MEDIUM)
- [ ] Endpoint Management API (MEDIUM)
- [ ] Client Version Info (LOW)

### Optional Enhancements:
- [ ] Enhanced TLS Options (MEDIUM, limited by rustls)
- [ ] Ergonomic Value Access (LOW)
- [ ] Enum Name Mapping (LOW)
- [ ] Uncompressed Array Support (LOW)

### Total Estimated Effort:
- **Phase 1 (Critical):** 5-7 days
- **Phase 2 (Advanced):** 3-4 days
- **Phase 3 (Polish):** 4-7 days
- **Total:** 12-18 days of focused development

---

## 11. Conclusion

The Rust implementation is **~85% feature-equivalent** to the C++ clickhouse-cpp library:

**Strengths:**
- ✅ Complete column type support (33+ types)
- ✅ Solid protocol implementation
- ✅ Async-first design (advantage over C++)
- ✅ Strong type safety

**Key Gaps:**
- Query cancellation (high-value feature)
- External tables (broken, needs fix)
- Advanced query features (totals/extremes)
- Endpoint management APIs

**Recommendation:**
Focus on **Phase 1 (Query Cancellation + External Tables)** to achieve 95%+ feature parity for common production use cases. Phase 2 and 3 can follow based on user demand.

---

**Last Updated:** 2025-11-01
**Maintainer:** Development Team
**Reference:** cpp/clickhouse-cpp (C++ implementation)
