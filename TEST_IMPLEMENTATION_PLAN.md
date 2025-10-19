# Comprehensive Test Implementation Plan
## ClickHouse Rust Client - Missing Tests Analysis

**Date:** 2025-10-18
**Status:** Planning Phase
**Based on:** C++ clickhouse-cpp reference implementation analysis

---

## Executive Summary

### Current State
- **C++ Reference Tests:** 191 test cases across 25 files
- **Rust Implementation:** 485 test annotations across 20 test files + inline unit tests
- **Current Coverage:** ~70% functional parity, but gaps in specific areas

### Coverage Analysis

**What We Have Well Covered:**
- ✅ Type parser (20 tests) - comparable to C++ (21 tests)
- ✅ Column operations (44 specialized + 17 general = 61 tests) - exceeds C++ (53 tests)
- ✅ Roundtrip tests (14 tests) - good coverage vs C++ (18 tests)
- ✅ Integration tests (17 tests) - good client functionality coverage
- ✅ Block operations (24 tests) - exceeds C++ (5 tests)
- ✅ TLS support (12 tests) - exceeds C++ (2 tests)
- ✅ Advanced types (Map, LowCardinality, Geo) - well covered

**Critical Gaps Identified:**
- ❌ **Performance benchmarks** - C++ has dedicated performance_tests.cpp
- ❌ **Connection failure scenarios** - C++ has connection_failed_client_test.cpp
- ❌ **Readonly client tests** - C++ has readonly_client_test.cpp
- ❌ **Array of arrays edge cases** - C++ has array_of_low_cardinality_tests.cpp
- ❌ **Socket-level tests** - C++ has socket_ut.cpp (4 tests)
- ❌ **Stream/buffer tests** - C++ has stream_ut.cpp
- ❌ **Abnormal column names** - C++ has abnormal_column_names_test.cpp
- ❌ **ItemView tests** - C++ has itemview_ut.cpp (4 tests)
- ❌ **Utils tests** - C++ has utils_ut.cpp (12 tests)
- ❌ **CreateColumnByType tests** - C++ has CreateColumnByType_ut.cpp (7 tests)

---

## Detailed Test Gap Analysis

### 1. Performance & Benchmarking Tests (HIGH PRIORITY)

**C++ Reference:** `performance_tests.cpp` (1 test file, multiple benchmarks)

**What's Missing:**
```rust
// Performance tests for:
- Column append performance (1M items)
- Serialization/deserialization speed
- Compression performance (LZ4 vs ZSTD vs None)
- Memory usage benchmarks
- Network throughput tests
- Query execution benchmarks
```

**Proposed Implementation:**
- New file: `tests/performance_benchmarks.rs`
- Use `criterion` crate for proper benchmarking
- Measure:
  - Column operations: append, slice, clone
  - Serialization: write_to/read_from for all column types
  - Compression: compress/decompress with different methods
  - Network: roundtrip latency, throughput
  - Memory: allocation patterns, peak usage

**Estimated Test Count:** 15-20 benchmarks

---

### 2. Connection Failure & Error Handling (HIGH PRIORITY)

**C++ Reference:** `connection_failed_client_test.cpp` (1 test)

**What's Missing:**
```rust
// Connection failure scenarios:
- Invalid hostname
- Invalid port
- Connection timeout
- Network unreachable
- Authentication failure (wrong credentials)
- SSL/TLS handshake failures
- Server version mismatch
- Protocol version incompatibility
```

**Current Coverage:** We have `error_handling_test.rs` (12 tests) but it's incomplete

**Gaps to Fill:**
```rust
#[tokio::test]
async fn test_connection_invalid_host() { /* ... */ }

#[tokio::test]
async fn test_connection_invalid_port() { /* ... */ }

#[tokio::test]
async fn test_connection_timeout() { /* ... */ }

#[tokio::test]
async fn test_auth_failure_wrong_password() { /* ... */ }

#[tokio::test]
async fn test_tls_handshake_failure() { /* ... */ }

#[tokio::test]
async fn test_protocol_version_mismatch() { /* ... */ }
```

**Proposed Implementation:**
- Expand `tests/error_handling_test.rs`
- Add `tests/connection_failure_test.rs`
- Test against mock server (use `tcp_server.cpp` pattern)

**Estimated Test Count:** 10-15 new tests

---

### 3. Readonly Client Tests (MEDIUM PRIORITY)

**C++ Reference:** `readonly_client_test.cpp` (1 test)

**What's Missing:**
```rust
// Readonly client scenarios:
- Connect with readonly=1 setting
- Execute SELECT queries (should work)
- Attempt INSERT (should fail)
- Attempt CREATE (should fail)
- Attempt DROP (should fail)
- Verify error messages match expected patterns
```

**Proposed Implementation:**
- New file: `tests/readonly_client_test.rs`
- Use client options: `ClientOptions::default().readonly(true)`
- Verify behavior matches ClickHouse readonly mode

**Estimated Test Count:** 5-8 tests

---

### 4. Array Edge Cases (MEDIUM PRIORITY)

**C++ Reference:**
- `array_of_low_cardinality_tests.cpp` (2 tests)
- `low_cardinality_nullable_tests.cpp` (4 tests)

**What's Missing:**
```rust
// Complex nested scenarios:
- Array(LowCardinality(String))
- Array(LowCardinality(Nullable(String)))
- Array(Array(LowCardinality(UInt64)))
- Nullable(Array(LowCardinality(String)))
- LowCardinality(Nullable(Array(String))) // if valid?

// Edge cases:
- Empty arrays in LowCardinality
- All-null arrays in Nullable(Array(...))
- Very deep nesting (3-4 levels)
- Mixed empty/non-empty in nested arrays
```

**Current Coverage:** We have some nested tests in `advanced_types_test.rs`

**Gaps to Fill:**
- Dedicated test file for complex nesting
- Specific LowCardinality + Array combinations
- Performance with deep nesting

**Proposed Implementation:**
- New file: `tests/nested_complex_types_test.rs`
- Systematic testing of all valid combinations

**Estimated Test Count:** 12-15 tests

---

### 5. Socket-Level Tests (LOW PRIORITY)

**C++ Reference:** `socket_ut.cpp` (4 tests)

**What's Missing:**
```rust
// Low-level socket operations:
- Socket creation and binding
- Connection establishment timing
- Socket options (TCP_NODELAY, keepalive, etc.)
- IPv4 vs IPv6 behavior
- Socket timeout behavior
```

**Current State:** Our `Connection` struct abstracts this

**Proposed Implementation:**
- New file: `tests/socket_behavior_test.rs` (if needed)
- Or expand existing `src/connection.rs` unit tests
- May not be critical if tokio handles this well

**Estimated Test Count:** 3-5 tests

---

### 6. Stream/Buffer Tests (LOW PRIORITY)

**C++ Reference:** `stream_ut.cpp` (1 test)

**What's Missing:**
```rust
// Stream buffer behavior:
- Buffered reads
- Buffered writes
- Flush behavior
- Buffer overflow handling
```

**Current State:** Abstracted by tokio streams

**Proposed Implementation:**
- Unit tests in `src/io/block_stream.rs` (already has 4 tests)
- May not need dedicated file

**Estimated Test Count:** 2-4 additional tests

---

### 7. Abnormal Column Names (MEDIUM PRIORITY)

**C++ Reference:** `abnormal_column_names_test.cpp` (1 test)

**What's Missing:**
```rust
// Edge case column names:
- Names with special characters: `col.name`, `col-name`, `col name`
- Names with backticks: `col`name`
- Reserved keywords: `select`, `from`, `where`
- Unicode names: `列名`, `имя`
- Very long names (>255 chars)
- Empty name (if allowed)
```

**Proposed Implementation:**
- New file: `tests/column_name_edge_cases_test.rs`
- Test both:
  - Column creation with abnormal names
  - Roundtrip with abnormal names
  - Query construction with escaping

**Estimated Test Count:** 8-10 tests

---

### 8. ItemView Tests (LOW PRIORITY)

**C++ Reference:** `itemview_ut.cpp` (4 tests)

**What's Missing:**
- ItemView is a C++ specific abstraction for accessing column items
- Rust equivalent would be our column access patterns

**Current State:** Our column types have `get()`, `get_unchecked()`, indexing

**Proposed Implementation:**
- Verify existing column access tests are sufficient
- Add edge cases if needed in `tests/column_tests.rs`

**Estimated Test Count:** 0-3 new tests (likely covered)

---

### 9. Utils Tests (MEDIUM PRIORITY)

**C++ Reference:** `utils_ut.cpp` (12 tests)

**What's Missing:**
```rust
// Utility function tests:
- String escaping/unescaping
- Type conversion utilities
- Varint encoding/decoding edge cases
- Compression utility functions
- Buffer management utilities
```

**Current Coverage:** We have inline tests in modules

**Gaps to Fill:**
```rust
// In wire_format.rs:
#[test]
fn test_varint_max_values() { /* ... */ }

#[test]
fn test_varint_negative_values() { /* ... */ }

// In compression.rs:
#[test]
fn test_compress_empty_data() { /* ... */ }

#[test]
fn test_compress_incompressible_data() { /* ... */ }

// String utilities:
#[test]
fn test_escape_backticks() { /* ... */ }

#[test]
fn test_escape_quotes() { /* ... */ }
```

**Proposed Implementation:**
- Expand inline tests in relevant modules
- New file: `tests/utility_functions_test.rs` for integration-level utils

**Estimated Test Count:** 10-15 tests

---

### 10. CreateColumnByType Tests (HIGH PRIORITY)

**C++ Reference:** `CreateColumnByType_ut.cpp` (7 tests)

**What's Missing:**
```rust
// Dynamic column creation from type strings:
- Create column from "UInt64" -> ColumnUInt64
- Create column from "Array(String)" -> ColumnArray<ColumnString>
- Create column from "Nullable(DateTime)" -> ColumnNullable<ColumnDateTime>
- Create column from complex nested types
- Handle invalid type strings gracefully
- Verify created column has correct type metadata
```

**Current State:** We have `create_column_tests.rs` (32 tests) - GOOD!

**Verification Needed:**
- Check if we cover all type creation scenarios
- Verify error handling for invalid types
- Ensure parity with C++ coverage

**Proposed Action:**
- Review `tests/create_column_tests.rs`
- Add any missing type creation scenarios
- Ensure error cases are tested

**Estimated Test Count:** 3-5 additional tests

---

## Missing Client Feature Tests

### From C++ client_ut.cpp Analysis

**C++ has 33 client tests. Comparing with our tests:**

| C++ Test | Rust Equivalent | Status |
|----------|----------------|--------|
| Version | ❌ No version test | **MISSING** |
| Array | ✅ Covered in roundtrip | OK |
| Date | ✅ Covered | OK |
| LowCardinality | ✅ Covered extensively | OK |
| Generic | ❌ Generic block handling | **PARTIAL** |
| Nullable | ✅ Covered | OK |
| Nothing | ✅ Covered | OK |
| Numbers | ✅ Covered | OK |
| SimpleAggregateFunction | ❌ Not tested | **MISSING** |
| Cancellable | ❌ Query cancellation | **MISSING** |
| Exception | ✅ Covered in error_handling | OK |
| Enum | ✅ Covered | OK |
| Decimal | ✅ Covered | OK |
| ColEscapeNameTest | ❌ See section 7 | **MISSING** |
| DateTime64 | ✅ Covered | OK |
| Query_ID | ❌ Query ID tracking | **MISSING** |
| ArrayArrayUInt64 | ✅ Covered in roundtrip | OK |
| OnProgress | ✅ Covered in callbacks | OK |
| QuerySettings | ❌ Per-query settings | **PARTIAL** |
| ServerLogs | ❌ Log packet handling | **MISSING** |
| TracingContext | ❌ Tracing support | **MISSING** |
| OnProfileEvents | ❌ ProfileEvents packet | **MISSING** |
| OnProfile | ❌ ProfileInfo packet | **MISSING** |
| SelectAggregateFunction | ❌ AggregateFunction type | **MISSING** |
| ResetConnection | ❌ Connection reset | **MISSING** |
| QueryParameters | ❌ Parameterized queries | **MISSING** |
| ClientName | ❌ Client name setting | **MISSING** |

**New Tests Needed:**

```rust
// tests/client_feature_tests.rs

#[tokio::test]
async fn test_client_version() {
    // Verify client version constants are accessible
}

#[tokio::test]
async fn test_simple_aggregate_function() {
    // Test SimpleAggregateFunction column type
}

#[tokio::test]
async fn test_query_cancellation() {
    // Test cancelling a running query
}

#[tokio::test]
async fn test_query_id_tracking() {
    // Verify query IDs are generated and tracked
}

#[tokio::test]
async fn test_per_query_settings() {
    // Test applying settings to individual queries
}

#[tokio::test]
async fn test_server_logs_callback() {
    // Test receiving server log packets
}

#[tokio::test]
async fn test_tracing_context() {
    // Test distributed tracing support
}

#[tokio::test]
async fn test_profile_events_callback() {
    // Test ProfileEvents packet handling
}

#[tokio::test]
async fn test_profile_info_callback() {
    // Test ProfileInfo packet handling
}

#[tokio::test]
async fn test_aggregate_function_column() {
    // Test AggregateFunction column type (if supported)
}

#[tokio::test]
async fn test_reset_connection() {
    // Test resetting connection without reconnecting
}

#[tokio::test]
async fn test_parameterized_queries() {
    // Test query parameter binding
}

#[tokio::test]
async fn test_client_name_setting() {
    // Test setting custom client name
}
```

**Estimated Test Count:** 15-20 tests

---

## Test Organization Recommendations

### Current Structure (Good)
```
tests/
├── integration_test.rs          (17 tests) - Core integration
├── type_parser_test.rs          (20 tests) - Type parsing
├── column_tests.rs              (17 tests) - Basic columns
├── specialized_column_tests.rs  (44 tests) - Advanced columns
├── block_tests.rs               (24 tests) - Block operations
├── roundtrip_tests.rs           (14 tests) - Insert/Select
├── client_*.rs                  (37 tests) - Client features
└── ... (others)
```

### Proposed Additions
```
tests/
├── performance_benchmarks.rs    (NEW - 15-20 benchmarks)
├── connection_failure_test.rs   (NEW - 10-15 tests)
├── readonly_client_test.rs      (NEW - 5-8 tests)
├── nested_complex_types_test.rs (NEW - 12-15 tests)
├── column_name_edge_cases_test.rs (NEW - 8-10 tests)
├── utility_functions_test.rs    (NEW - 10-15 tests)
├── client_feature_tests.rs      (NEW - 15-20 tests)
├── socket_behavior_test.rs      (OPTIONAL - 3-5 tests)
└── (expand existing files)
```

---

## Implementation Priority

### Phase 1: Critical (Implement First)
1. **Client Feature Tests** (15-20 tests)
   - Query ID tracking
   - Server logs callback
   - Profile events/info callbacks
   - Client name setting
   - Query parameters

2. **CreateColumnByType Verification** (3-5 tests)
   - Review existing tests
   - Add missing type scenarios

3. **Connection Failure Tests** (10-15 tests)
   - Invalid credentials
   - Network errors
   - Timeout scenarios

### Phase 2: Important (Implement Soon)
4. **Performance Benchmarks** (15-20 benchmarks)
   - Column operations
   - Compression performance
   - Network throughput

5. **Nested Complex Types** (12-15 tests)
   - Array + LowCardinality combinations
   - Deep nesting scenarios

6. **Column Name Edge Cases** (8-10 tests)
   - Special characters
   - Unicode names
   - Reserved keywords

7. **Readonly Client Tests** (5-8 tests)
   - Readonly mode behavior
   - Error handling

### Phase 3: Nice to Have (Implement Later)
8. **Utils Tests** (10-15 tests)
   - Expand inline tests
   - Integration-level utilities

9. **Socket Tests** (3-5 tests)
   - Low-level socket behavior
   - IPv4/IPv6 handling

10. **Stream/Buffer Tests** (2-4 tests)
    - Buffer management
    - Flush behavior

---

## Test Metrics Goals

### Current State
- **Total Tests:** ~485 (including inline)
- **Integration Tests:** ~200 (in tests/)
- **Unit Tests:** ~285 (in src/)

### Target After Implementation
- **Total Tests:** ~600-650
- **New Integration Tests:** ~100-120
- **Enhanced Unit Tests:** ~15-20
- **Benchmarks:** ~15-20

### Coverage Goals
- **Line Coverage:** 85%+ (currently ~75%)
- **Feature Parity:** 95%+ with C++ reference
- **Edge Cases:** All critical paths covered
- **Performance:** Baseline benchmarks established

---

## Test Quality Standards

### All New Tests Must:
1. ✅ Have descriptive names following convention: `test_feature_scenario`
2. ✅ Include inline comments explaining non-obvious assertions
3. ✅ Use `#[ignore]` for tests requiring external dependencies
4. ✅ Clean up resources (tables, connections) in test body or via Drop
5. ✅ Use appropriate async/sync based on operation
6. ✅ Include both success and failure scenarios where applicable
7. ✅ Test edge cases (empty, null, max values, etc.)
8. ✅ Use consistent test data patterns
9. ✅ Assert specific error messages, not just error presence
10. ✅ Include performance expectations for benchmarks

---

## Implementation Checklist

### Before Starting
- [ ] Review all existing tests to avoid duplication
- [ ] Set up benchmarking infrastructure (`criterion` crate)
- [ ] Create test data generators for consistency
- [ ] Set up CI to run new tests
- [ ] Document test requirements and setup

### During Implementation
- [ ] Implement Phase 1 tests (Critical)
- [ ] Run all tests and verify they pass
- [ ] Review test coverage reports
- [ ] Implement Phase 2 tests (Important)
- [ ] Document any new test patterns
- [ ] Implement Phase 3 tests (Nice to Have)

### After Implementation
- [ ] Run full test suite
- [ ] Generate coverage report
- [ ] Document test organization in README
- [ ] Create test maintenance guide
- [ ] Set up performance regression tracking

---

## Special Considerations

### Testing Against Real ClickHouse Server
- All integration tests require running ClickHouse instance
- Use `#[ignore]` attribute and document in test comments
- Provide docker-compose setup for test environment
- Document minimum ClickHouse version requirements

### Mock Server Considerations
- Some tests may benefit from mock TCP server (see `tcp_server.cpp`)
- Useful for connection failure scenarios
- Allows testing without ClickHouse dependency

### Performance Test Infrastructure
- Use `criterion` for benchmarking
- Establish baseline metrics
- Set up CI to track performance regressions
- Document hardware specs for reproducibility

---

## Estimated Timeline

### Phase 1 (Critical) - 2-3 weeks
- Client feature tests: 1 week
- Connection failure tests: 1 week
- Review and verification: 3-5 days

### Phase 2 (Important) - 3-4 weeks
- Performance benchmarks: 1.5 weeks
- Nested complex types: 1 week
- Column name edge cases: 3-4 days
- Readonly client: 3-4 days

### Phase 3 (Nice to Have) - 1-2 weeks
- Utils tests: 1 week
- Socket and stream tests: 3-5 days

**Total Estimated Time:** 6-9 weeks for complete implementation

---

## Success Criteria

Test implementation is considered complete when:
1. ✅ All Phase 1 tests implemented and passing
2. ✅ 90%+ of Phase 2 tests implemented
3. ✅ Test coverage ≥ 85%
4. ✅ All existing tests still passing
5. ✅ Performance benchmarks established
6. ✅ CI integration complete
7. ✅ Documentation updated
8. ✅ Feature parity with C++ ≥ 95%

---

## Appendix: Test File Mapping

### C++ → Rust Test File Correspondence

| C++ File | Rust File(s) | Coverage Status |
|----------|--------------|-----------------|
| client_ut.cpp | integration_test.rs, client_*.rs | 70% (missing 10 scenarios) |
| columns_ut.cpp | column_tests.rs, specialized_column_tests.rs | 90% |
| type_parser_ut.cpp | type_parser_test.rs | 95% |
| block_ut.cpp | block_tests.rs | 95% (exceeds C++) |
| roundtrip_tests.cpp | roundtrip_tests.rs | 80% |
| CreateColumnByType_ut.cpp | create_column_tests.rs | 90% |
| low_cardinality*.cpp | map_lowcard_geo_tests.rs | 85% |
| array_*.cpp | advanced_types_test.rs | 70% (needs more nesting) |
| performance_tests.cpp | ❌ MISSING | 0% |
| connection_failed*.cpp | error_handling_test.rs (partial) | 30% |
| readonly_client_test.cpp | ❌ MISSING | 0% |
| abnormal_column_names_test.cpp | ❌ MISSING | 0% |
| socket_ut.cpp | connection.rs (unit tests) | 50% |
| stream_ut.cpp | block_stream.rs (unit tests) | 70% |
| ssl_ut.cpp | tls_integration_test.rs | 100% (exceeds C++) |
| itemview_ut.cpp | column_tests.rs (covered differently) | ~80% |
| utils_ut.cpp | Various inline tests | 60% |
| types_ut.cpp | type_methods_tests.rs | 90% |

---

**Document Version:** 1.0
**Last Updated:** 2025-10-18
**Status:** Ready for Review

