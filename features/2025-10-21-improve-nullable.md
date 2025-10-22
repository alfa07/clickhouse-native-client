# ColumnNullable C++ Compatibility Improvements

**Date:** 2025-10-21
**Branch:** improve-nullable

## Summary

Matched the Rust `ColumnNullable` implementation to the C++ reference implementation (`cpp/clickhouse/columns/nullable.h` and `cpp/clickhouse/columns/nullable.cpp`). This improves type safety and provides a more ergonomic typed API via the new `ColumnNullableT<T>` wrapper.

## Changes

### 1. ColumnNullable Storage Refactoring

**Before:**
```rust
pub struct ColumnNullable {
    type_: Type,
    nested: ColumnRef,
    nulls: Vec<u8>, // Direct vector storage
}
```

**After:**
```rust
pub struct ColumnNullable {
    type_: Type,
    nested: ColumnRef,
    nulls: ColumnRef, // ColumnRef pointing to ColumnUInt8
}
```

**Benefits:**
- Matches C++ `std::shared_ptr<ColumnUInt8> nulls_` implementation
- More consistent with the rest of the codebase (everything is a Column)
- Allows for better abstraction and reuse of ColumnUInt8 functionality

### 2. New API Methods

Added methods to match C++ interface:

- `append(bool isnull)` - Append a null flag (matches C++ API signature)
- `nested() -> ColumnRef` - Get nested column (matches C++ `Nested()`)
- `nulls() -> ColumnRef` - Get nulls column (matches C++ `Nulls()`)
- `from_parts(nested, nulls) -> Result<Self>` - Create from existing columns with validation

### 3. ColumnNullableT<T> Generic Wrapper

Implemented new `ColumnNullableT<T>` struct matching C++ `ColumnNullableT<ColumnType>`:

```rust
pub struct ColumnNullableT<T: Column> {
    inner: ColumnNullable,
    _phantom: PhantomData<T>,
}
```

**Key Methods:**
- `from_parts(nested: Arc<T>, nulls: ColumnRef) -> Result<Self>` - Create typed nullable
- `from_nested(nested: Arc<T>) -> Self` - Create from nested (all non-null)
- `wrap(col: ColumnNullable) -> Self` - Wrap existing ColumnNullable (matches C++ `Wrap()`)
- `wrap_ref(col: ColumnRef) -> Result<Self>` - Wrap from ColumnRef
- `typed_nested() -> Result<Arc<T>>` - Get typed nested column
- `is_null(index: usize) -> bool` - Check if value is null
- `inner() / inner_mut()` - Access underlying ColumnNullable

**Implements Column trait** - Can be used anywhere ColumnRef is expected

### 4. Test Updates

Updated all integration tests to properly handle the new API:
- Fixed temporary value borrowing issues (`.nested()` now returns `ColumnRef` which must be stored)
- Updated tests in:
  - `tests/integration_block_nullable_int64.rs` (4 fixes)
  - `tests/integration_block_nullable_string.rs` (3 fixes)
  - `tests/integration_block_nullable_uuid.rs` (3 fixes)
  - `tests/integration_block_nullable_ipv6.rs` (1 fix)

### 5. Implementation Details

**Validation in `from_parts`:**
- Ensures nulls column is ColumnUInt8
- Validates nested and nulls have same size
- Returns proper errors for invalid input

**Clone Implementation:**
- Added `Clone` for `ColumnNullable` (shallow clone via Arc)
- Enables more flexible usage patterns

## Testing

### Unit Tests
- All 190 unit tests pass
- 15 nullable-specific unit tests including new ColumnNullableT tests

### Integration Tests
- All 27 main integration tests pass
- Nullable-specific integration tests:
  - `integration_block_nullable_int64`: 5 tests ✓
  - `integration_block_nullable_string`: 4 tests ✓
  - `integration_block_nullable_uuid`: 4 tests ✓
  - `integration_block_nullable_ipv6`: 4 tests ✓
  - `integration_nullable`: 3 tests ✓

**Total: 220 tests passing**

## Files Modified

### Core Implementation
- `src/column/nullable.rs` - Complete refactoring

### Integration Tests
- `tests/integration_block_nullable_int64.rs`
- `tests/integration_block_nullable_string.rs`
- `tests/integration_block_nullable_uuid.rs`
- `tests/integration_block_nullable_ipv6.rs`

## Breaking Changes

**Potential API Changes:**
- `nulls()` method now returns `ColumnRef` instead of `&[u8]`
- Code that accessed the nulls bitmap directly will need to downcast to `ColumnUInt8`

**Migration Example:**
```rust
// Before:
let nulls = col.nulls(); // &[u8]
let is_null = nulls[i] != 0;

// After:
let nulls_ref = col.nulls(); // ColumnRef
let nulls_col = nulls_ref.as_any().downcast_ref::<ColumnUInt8>().unwrap();
let is_null = nulls_col.at(i) != 0;

// Or use the is_null() method:
let is_null = col.is_null(i); // Preferred!
```

## Compatibility

- Fully compatible with C++ clickhouse-cpp nullable implementation
- Wire format unchanged - no protocol changes
- All existing integration tests pass without changes to test logic

## Future Improvements

Potential enhancements building on this foundation:
- Type-safe `at()` and `append()` methods on `ColumnNullableT<T>` that work with `Option<T::ValueType>`
- Builder pattern for constructing nullable columns
- Better integration with Rust's type system for compile-time nullable checks
