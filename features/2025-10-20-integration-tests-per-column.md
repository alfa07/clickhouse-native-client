# Integration Tests Per Column Type

**Date**: 2025-10-20
**Branch**: integration-tests-per-column
**Status**: Completed

## Summary

Added comprehensive integration tests for all ClickHouse column types, organized into separate test files per column type category. Each test creates a table, inserts data, and verifies roundtrip consistency.

## Changes

### New Test Files Created

#### Simple Column Types
- `tests/integration_numeric.rs` - Tests for all numeric types (Int8-128, UInt8-128, Float32/64, Bool)
  - Boundary value tests (min, max, zero)
  - Property-based tests using proptest for random data

- `tests/integration_string.rs` - Tests for String and FixedString types
  - Empty strings, UTF-8, special characters
  - Long strings (1KB, 10KB, 100KB)
  - Various FixedString sizes (1, 5, 16, 32, 64, 128)
  - Property-based testing

- `tests/integration_date.rs` - Tests for Date, Date32, DateTime, DateTime64
  - Boundary dates (min/max for each type)
  - Various DateTime64 precisions (0, 3, 6, 9)

- `tests/integration_decimal.rs` - Tests for Decimal types
  - Various precision and scale combinations
  - Positive, negative, and zero values

- `tests/integration_enum.rs` - Tests for Enum8 and Enum16
  - Multiple enum values

- `tests/integration_ipv4.rs` - Tests for IPv4 type
  - Various IPv4 addresses including min (0.0.0.0) and max (255.255.255.255)

- `tests/integration_ipv6.rs` - Tests for IPv6 type
  - Various IPv6 addresses including :: and max

- `tests/integration_uuid.rs` - Tests for UUID type
  - Fixed UUIDs and generated UUIDs

#### Compound Column Types
- `tests/integration_array.rs` - Tests for Array types
  - Array(Float32), Array(Float64)
  - Array(String)
  - Array(Int32), Array(Int64)
  - Array(Date), Array(Date32)
  - Array(DateTime), Array(DateTime64)
  - Array(LowCardinality(String))
  - Array(Decimal(10, 2))
  - Empty arrays

- `tests/integration_nullable.rs` - Tests for Nullable types
  - Nullable(String) with NULLs and non-NULLs
  - Nullable with all NULLs
  - Nullable(Array(IPv6))
  - Nullable(Tuple(IPv6, IPv4))
  - Empty strings vs NULLs

- `tests/integration_tuple.rs` - Tests for Tuple types
  - Tuple(Float32, Float64)
  - Tuple(Int32, Int64)
  - Tuple(String, Int64)
  - Tuple(String, Int64, Array(String))
  - Tuples with empty values

- `tests/integration_map.rs` - Tests for Map types
  - Map(Int8, String)
  - Map(String, Array(Array(Int8)))
  - Map(UUID, Nullable(String))
  - Map(UUID, Nullable(LowCardinality(String)))
  - Empty maps

- `tests/integration_lowcardinality.rs` - Tests for LowCardinality types
  - LowCardinality(String) with repeated values
  - LowCardinality(Int64)
  - LowCardinality(UUID)
  - Empty strings
  - Boundary values
  - Many unique values (1000)

### New Infrastructure
- `tests/common/mod.rs` - Common test helpers
  - `get_clickhouse_host()` - Read from env or default to localhost
  - `create_test_client()` - Create basic test client
  - `unique_database_name()` - Generate unique database names for test isolation
  - `create_isolated_test_client()` - Create client with unique database
  - `cleanup_test_database()` - Drop test database after completion

### Modified Files
- `Cargo.toml` - Added `proptest = "1.4"` to dev-dependencies

## Test Strategy

### Boundary Testing
All numeric and date types test:
- Minimum values
- Maximum values
- Zero/empty values
- Near-boundary values

### Empty Value Testing
- Empty strings
- Empty arrays
- Empty maps
- All-NULL nullable columns

### Property-Based Testing
Using proptest for:
- Random UInt32 values (1-100 elements)
- Random Int64 values (1-100 elements)
- Random Float64 values (finite only, 1-100 elements)
- Random String values (1-50 elements)

### Complex Type Combinations
Tested nested types as specified:
- Array(LowCardinality(String))
- Array(Decimal(10, 2))
- Nullable(Array(IPv6))
- Nullable(Tuple(IPv6, IPv4))
- Map(UUID, Nullable(LowCardinality(String)))
- Tuple(String, Int64, Array(String))

## Implementation Notes

### SQL-Based INSERT Strategy
Most tests use SQL INSERT statements rather than programmatic block construction because:
1. Tests the full roundtrip through ClickHouse's SQL parser
2. Simpler and more maintainable test code
3. Avoids complex API details of nested column construction
4. More realistic integration test scenario

### Test Isolation
Each test uses a unique database name based on:
- Test name
- Nanosecond timestamp

This allows parallel test execution without conflicts.

### Ignored Tests
All integration tests are marked with `#[ignore]` because they require:
- A running ClickHouse server
- Network connectivity
- Proper permissions

Run with: `cargo test --ignored` when ClickHouse is available.

## Test Coverage

### Simple Types (Complete)
- ✅ All numeric types (Int8-128, UInt8-128, Float32/64, Bool)
- ✅ String, FixedString
- ✅ Date, Date32, DateTime, DateTime64
- ✅ Decimal
- ✅ Enum8, Enum16
- ✅ IPv4, IPv6
- ✅ UUID

### Compound Types (Complete)
- ✅ Array(T) for Float32, Float64, String, Int32, Int64, Date, Date32, DateTime, DateTime64
- ✅ Array(LowCardinality(String))
- ✅ Array(Decimal(10, 2))
- ✅ Nullable(String)
- ✅ Nullable(Array(IPv6))
- ✅ Nullable(Tuple(IPv6, IPv4))
- ✅ LowCardinality(String), LowCardinality(Int64), LowCardinality(UUID)
- ✅ Tuple(Float32, Float64), Tuple(Int32, Int64), Tuple(String, Int64), Tuple(String, Int64, Array(String))
- ✅ Map(Int8, String), Map(String, Array(Array(Int8)))
- ✅ Map(UUID, Nullable(String)), Map(UUID, Nullable(LowCardinality(String)))

## Files Created/Modified

### New Files (16)
- tests/common/mod.rs
- tests/integration_numeric.rs
- tests/integration_string.rs
- tests/integration_date.rs
- tests/integration_decimal.rs
- tests/integration_enum.rs
- tests/integration_ipv4.rs
- tests/integration_ipv6.rs
- tests/integration_uuid.rs
- tests/integration_array.rs
- tests/integration_nullable.rs
- tests/integration_tuple.rs
- tests/integration_map.rs
- tests/integration_lowcardinality.rs

### Modified Files (1)
- Cargo.toml

## Test Statistics

- **Total new integration test files**: 14
- **Total integration tests**: ~50 tests
- **Property-based tests**: 3 (UInt32, Int64, Float64, String)
- **Boundary tests**: All numeric and date types
- **Empty value tests**: Strings, Arrays, Maps, Nullable
- **Complex nested types**: 10+ combinations

## Benefits

1. **Complete Coverage**: Every column type has dedicated integration tests
2. **Regression Prevention**: Boundary and edge cases are tested
3. **Documentation**: Tests serve as usage examples for each column type
4. **Test Isolation**: Unique databases prevent test interference
5. **Property-Based Testing**: Random data tests help find edge cases
6. **Maintainability**: Organized by column type for easy navigation

## Next Steps

To run the tests:
```bash
# Ensure ClickHouse is running
docker run -d -p 9000:9000 clickhouse/clickhouse-server

# Run all integration tests
cargo test --ignored

# Run specific column type tests
cargo test --test integration_numeric --ignored
cargo test --test integration_array --ignored
```
