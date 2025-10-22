# Refactor Nested Column Access API

**Date:** 2025-10-22
**Branch:** ref-nested-nested-mut
**Status:** Completed

## Summary

Refactored the nested column access API for all compound column types (Nullable, Array, LowCardinality, Map) to use generic methods with type parameters instead of returning ColumnRef, improving type safety and reducing boilerplate code.

## Changes

### API Changes

#### ColumnNullable

**Old API:**
```rust
pub fn nested(&self) -> ColumnRef { ... }
pub fn nested_mut(&mut self) -> &mut ColumnRef { ... }
```

**New API:**
```rust
pub fn nested<T: Column + 'static>(&self) -> &T { ... }
pub fn nested_mut<T: Column + 'static>(&mut self) -> &mut T { ... }
pub fn nested_ref(&self) -> ColumnRef { ... }
pub fn nested_ref_mut(&mut self) -> &mut ColumnRef { ... }
```

#### ColumnArray

**Old API:**
```rust
pub fn nested(&self) -> &ColumnRef { ... }
pub fn nested_mut(&mut self) -> &mut ColumnRef { ... }
```

**New API:**
```rust
pub fn nested<T: Column + 'static>(&self) -> &T { ... }
pub fn nested_mut<T: Column + 'static>(&mut self) -> &mut T { ... }
pub fn nested_ref(&self) -> ColumnRef { ... }
```

#### ColumnLowCardinality

**Old API:**
```rust
pub fn dictionary(&self) -> &ColumnRef { ... }
```

**New API:**
```rust
pub fn dictionary<T: Column + 'static>(&self) -> &T { ... }
pub fn dictionary_mut<T: Column + 'static>(&mut self) -> &mut T { ... }
pub fn dictionary_ref(&self) -> ColumnRef { ... }
```

#### ColumnMap

**Old API:**
```rust
pub fn data(&self) -> &ColumnRef { ... }
```

**New API:**
```rust
pub fn data<T: Column + 'static>(&self) -> &T { ... }
pub fn data_mut<T: Column + 'static>(&mut self) -> &mut T { ... }
pub fn data_ref(&self) -> ColumnRef { ... }
```

### Benefits

1. **Type Safety:** Generic methods provide compile-time type checking, eliminating the need for runtime downcasting
2. **Less Boilerplate:** Reduced from 4-5 lines of downcasting code to a single method call with type parameter
3. **Better Ergonomics:** More idiomatic Rust with direct typed access
4. **Backward Compatibility:** Added `*_ref()` methods for cases requiring dynamic dispatch

### Usage Examples

**Before:**
```rust
let nested_ref = result_col.nested();
let nested = nested_ref
    .as_any()
    .downcast_ref::<ColumnUInt32>()
    .unwrap();
assert_eq!(nested.at(0), 42);
```

**After:**
```rust
let nested: &ColumnUInt32 = result_col.nested();
assert_eq!(nested.at(0), 42);
```

**Before (mutable):**
```rust
Arc::get_mut(nullable_col.nested_mut())
    .unwrap()
    .as_any_mut()
    .downcast_mut::<ColumnUInt32>()
    .unwrap()
    .append(value);
```

**After (mutable):**
```rust
nullable_col.nested_mut::<ColumnUInt32>().append(value);
```

## Files Modified

### Source Files
- `src/column/nullable.rs` - Added generic nested/nested_mut methods
- `src/column/array.rs` - Added generic nested/nested_mut methods
- `src/column/lowcardinality.rs` - Added generic dictionary/dictionary_mut methods
- `src/column/map.rs` - Added generic data/data_mut methods
- `src/column/column_value.rs` - Updated to use nested_ref()

### Test Files
- `tests/roundtrip_tests.rs` - Updated to use new API
- `tests/integration_block_nullable_uuid.rs` - Updated to use new API
- `tests/integration_block_nullable_string.rs` - Updated to use new API
- `tests/integration_block_nullable_ipv6.rs` - Updated to use new API
- `tests/integration_block_nullable_int64.rs` - Updated to use new API
- `tests/integration_test.rs` - Updated to use new API
- `tests/create_column_tests.rs` - Updated to use nested_ref() for dynamic checks

## Testing

- ✅ All unit tests pass (190 tests)
- ✅ Library compiles successfully
- ✅ Code formatted with `cargo +nightly fmt --all`

## Notes

- The `*_ref()` methods are provided for scenarios requiring dynamic dispatch (e.g., when concrete type is unknown at compile time)
- The `nested_ref_mut()` method on ColumnNullable provides mutable access to ColumnRef for dynamic dispatch scenarios
- This change follows Rust best practices for type-safe APIs while maintaining flexibility where needed
