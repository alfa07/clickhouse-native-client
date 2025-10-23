# Refactor ColumnVector to Use Generic `new()` Constructor

**Date:** 2025-10-22
**Branch:** ref-numeric-use-new
**Status:** Completed

## Summary

Refactored `ColumnVector<T>` to remove the `with_type()` and `with_type_and_capacity()` methods in favor of using the generic `new()` and `with_capacity()` constructors that infer the type from the generic parameter T.

This change simplifies the API by leveraging Rust's type system and the `ToType` trait to automatically determine the correct ClickHouse type from the Rust type parameter.

## Changes Made

### 1. ColumnVector API Simplification (`src/column/numeric.rs`)

**Removed methods:**
- `pub fn with_type(type_: Type) -> Self`
- `pub fn with_type_and_capacity(type_: Type, capacity: usize) -> Self`

**Why removed:**
These methods required manually passing the type, which is redundant when using generic programming. The type can be automatically inferred from the generic parameter T using the `ToType` trait.

**Kept methods:**
- `pub fn new() -> Self` - Type inferred via `T::to_type()`
- `pub fn with_capacity(capacity: usize) -> Self` - Type inferred via `T::to_type()`
- `pub fn from_vec(type_: Type, data: Vec<T>) -> Self` - Still needed for specific type variants

### 2. Trait Bound Updates

**Added `ToType` bound to Column implementation:**
```rust
// Before:
impl<T: FixedSize> Column for ColumnVector<T>

// After:
impl<T: FixedSize + ToType> Column for ColumnVector<T>
```

**Added `ToType` bound to ColumnTyped implementation:**
```rust
// Before:
impl<T: FixedSize + Clone + Send + Sync + 'static> ColumnTyped<T> for ColumnVector<T>

// After:
impl<T: FixedSize + ToType + Clone + Send + Sync + 'static> ColumnTyped<T> for ColumnVector<T>
```

**Rationale:**
Since all numeric types used with ColumnVector already implement ToType, this is not a breaking change. The bound ensures that `clone_empty()` and other methods can call `T::to_type()` to create new instances.

### 3. Updated `clone_empty()` Implementation

**Before:**
```rust
fn clone_empty(&self) -> ColumnRef {
    Arc::new(ColumnVector::<T>::with_type(self.type_.clone()))
}
```

**After:**
```rust
fn clone_empty(&self) -> ColumnRef {
    Arc::new(ColumnVector::<T>::new())
}
```

**Benefit:** Simpler and more idiomatic Rust code. The type is automatically inferred from T.

### 4. Block Stream Updates (`src/io/block_stream.rs`)

Updated all numeric column creation in `create_column()` function:

**Before:**
```rust
TypeCode::UInt8 => Ok(Arc::new(ColumnUInt8::with_type(type_.clone()))),
TypeCode::UInt16 => Ok(Arc::new(ColumnUInt16::with_type(type_.clone()))),
// ... etc
```

**After:**
```rust
TypeCode::UInt8 => Ok(Arc::new(ColumnUInt8::new())),
TypeCode::UInt16 => Ok(Arc::new(ColumnUInt16::new())),
// ... etc
```

**Impact:** Cleaner code, no need to pass `type_` parameter since it's automatically inferred.

### 5. Test Updates

Updated all test files to use the new API:

**Files modified:**
- `src/column/numeric.rs` - Unit test updated
- `tests/column_tests.rs` - Integration test updated
- `tests/roundtrip_tests.rs` - Integration test updated

**Before:**
```rust
let mut col = ColumnUInt64::with_type(Type::uint64());
```

**After:**
```rust
let mut col = ColumnUInt64::new();
```

## Rust Best Practices Applied

1. **Type Inference:** Leverage Rust's type system to infer types from generic parameters
2. **Trait Bounds:** Use trait bounds (`ToType`) to ensure compile-time type safety
3. **Reduced Redundancy:** Eliminate redundant type passing when it can be inferred
4. **Simplified API:** Fewer methods with clearer purpose
5. **Consistency:** All numeric columns now use the same creation pattern

## Testing

### Unit Tests
- ✅ All 199 unit tests pass
- ✅ No new warnings introduced (fixed unused import in tests)

### Integration Tests
- ✅ Main integration tests (27 tests) pass
- ✅ Numeric integration tests (16 tests) pass
- ✅ All roundtrip tests pass for all numeric types

**Test Coverage:**
- `test_uint8_roundtrip` through `test_uint128_roundtrip`
- `test_int8_roundtrip` through `test_int128_roundtrip`
- `test_float32_roundtrip` and `test_float64_roundtrip`
- Property-based tests for int64, uint32, and float64

## Migration Guide

For users upgrading their code:

**Old code:**
```rust
let col = ColumnUInt32::with_type(Type::uint32());
let col = ColumnInt64::with_type_and_capacity(Type::int64(), 1000);
```

**New code:**
```rust
let col = ColumnUInt32::new();
let col = ColumnInt64::with_capacity(1000);
```

**Note:** The `from_vec()` method still exists for cases where you need to specify a custom type variant.

## Files Changed

1. `src/column/numeric.rs`
   - Removed `with_type()` and `with_type_and_capacity()` methods
   - Updated trait bounds to include `ToType`
   - Updated `clone_empty()` to use `new()`
   - Updated unit tests

2. `src/io/block_stream.rs`
   - Updated all numeric column creation to use `new()`
   - Simplified Point type creation

3. `tests/column_tests.rs`
   - Updated Array test to use `new()`

4. `tests/roundtrip_tests.rs`
   - Updated Array roundtrip test to use `new()`

## Benefits

1. **Cleaner API:** Fewer methods, clearer intent
2. **Type Safety:** Type inference ensures correctness at compile time
3. **Less Boilerplate:** No need to specify type when it's already known from T
4. **Better Rust Idioms:** Follows Rust conventions for generic constructors
5. **Maintainability:** Easier to understand and maintain
6. **No Performance Impact:** All changes are compile-time, no runtime overhead

## Compatibility

**Breaking Change:** Yes
- Code using `with_type()` or `with_type_and_capacity()` will need to be updated
- However, this is only used internally and in tests
- Migration is straightforward (see Migration Guide above)

**All Tests Pass:** Yes
- Unit tests: 199/199 ✅
- Integration tests: All numeric tests ✅
- No regressions detected

## Next Steps

None required. The refactoring is complete and all tests pass.
