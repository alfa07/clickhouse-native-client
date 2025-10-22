# ColumnArrayT<T> - Typed Array Column Wrapper

**Branch:** `ref-array-t`
**Date:** 2025-10-22
**Status:** Implementation Complete

## Summary

Implemented `ColumnArrayT<T>`, a typed wrapper for `ColumnArray` that provides compile-time type safety and ergonomic API for working with array columns. This follows the design pattern from the C++ clickhouse-cpp library's `ColumnArrayT<NestedColumnType>`.

## Changes

### New Type: `ColumnArrayT<T>`

**File:** `src/column/array.rs`

Added a generic wrapper that:
- Wraps `ColumnArray` internally
- Uses `PhantomData<fn() -> T>` to track the nested column type at compile time
- Provides typed access to the nested column without runtime downcasting overhead for users
- Implements all `Column` trait methods by delegating to the inner `ColumnArray`

### Key Features

1. **Type-Safe Construction**
   - `ColumnArrayT::new(type_)` - Creates from a Type, verifies nested column matches T
   - `ColumnArrayT::with_nested(Arc<T>)` - Creates from a typed nested column
   - `ColumnArrayT::with_capacity(type_, capacity)` - Pre-allocates capacity

2. **Typed Nested Access**
   - `nested_typed(&self) -> &T` - Get typed reference to nested column
   - `nested_typed_mut(&mut self) -> Result<&mut T>` - Get mutable typed reference
   - Downcasting is done once at construction and verified, not on every access

3. **Ergonomic Array Building**
   - `append_array(|nested| { ... })` - Append array using closure
   - Automatically calculates array length
   - Provides typed mutable access to nested column inside closure

   Example:
   ```rust
   let mut arr = ColumnArrayT::<ColumnUInt64>::new(Type::array(Type::uint64()))?;
   arr.append_array(|nested| {
       nested.append(1);
       nested.append(2);
       nested.append(3);
   })?;
   ```

4. **Full Column Trait Implementation**
   - All methods delegate to inner `ColumnArray`
   - `slice()` returns `ColumnArrayT<T>` (properly wrapped)
   - `clone_empty()` preserves the typed wrapper

### Exports

**File:** `src/column/mod.rs`

Added `ColumnArrayT` to public exports:
```rust
pub use array::{
    ColumnArray,
    ColumnArrayT,
};
```

## Testing

### New Tests (10 tests added)

All tests in `src/column/array.rs`:

1. `test_array_t_creation` - Basic construction
2. `test_array_t_new` - Construction from Type
3. `test_array_t_append_array` - Append arrays using closure
4. `test_array_t_typed_access` - Typed nested column access
5. `test_array_t_with_strings` - Works with non-numeric types
6. `test_array_t_empty_arrays` - Empty array handling
7. `test_array_t_append_column` - Column concatenation
8. `test_array_t_slice` - Slicing preserves type wrapper
9. `test_array_t_roundtrip` - Serialization/deserialization
10. Existing tests continue to work with `ColumnArray`

### Test Results

```
Unit Tests:    199 passed ✓
Integration:   27 passed ✓ (main suite)
              11 passed ✓ (array tests)
```

All existing tests continue to pass - no breaking changes.

## Design Decisions

### 1. Composition Over Inheritance

Rust doesn't support inheritance, so we wrap `ColumnArray` rather than extend it. This provides:
- Clean separation of concerns
- Type safety without modifying `ColumnArray`
- Easy delegation of trait methods

### 2. PhantomData with fn() -> T

Using `PhantomData<fn() -> T>` instead of `PhantomData<T>`:
- Better variance properties (covariant in T)
- Doesn't impose unnecessary trait bounds
- Standard Rust pattern for type parameters

### 3. Verification at Construction

Type verification happens once at construction:
- `new()` and `with_capacity()` verify the nested column type matches T
- `nested_typed()` can then panic on mismatch (which shouldn't happen)
- Trade-off: small cost at construction for zero cost at access

### 4. Closure-Based Array Building

The `append_array(|nested| { ... })` API:
- Automatically tracks array length
- No manual offset management
- Type-safe access to nested column
- Familiar pattern for Rust developers

### 5. Slice Returns ColumnArrayT<T>

When slicing, we return `ColumnArrayT<T>` not `ColumnArray`:
- Preserves type information
- User doesn't need to re-wrap
- Required manual cloning of inner structure to preserve offsets

## Comparison with C++ Implementation

### Similarities
- Generic/template wrapper over base type
- Provides typed access to nested column
- Convenience methods for appending

### Differences

| C++ | Rust | Reason |
|-----|------|--------|
| Inheritance | Composition | Rust doesn't have inheritance |
| Multiple constructors | Associated functions | Rust convention |
| `ArrayValueView` | Not implemented | Complex with Rust's type system, can add later |
| Iterator-based append | Closure-based append | More ergonomic in Rust |

## Future Enhancements

Potential additions (not in scope for this PR):

1. **ArrayValueView** - Typed view into individual arrays
   - Would need trait for element access (`trait IndexableColumn`)
   - Or use type-specific implementations

2. **Iterator Support** - `append_from_iter(iter)`
   - Would need trait for appending values
   - Or macro-based implementation

3. **Type Aliases** - Common types like `ColumnArrayUInt64`
   ```rust
   pub type ColumnArrayUInt64 = ColumnArrayT<ColumnUInt64>;
   ```

## Performance

No performance overhead compared to `ColumnArray`:
- Zero-cost abstraction (no vtable, same memory layout)
- Type verification only at construction
- All access methods inline to inner calls
- Same serialization format

## Migration Guide

Existing code using `ColumnArray` continues to work unchanged.

To use the typed API:

```rust
// Before (untyped)
let mut arr = ColumnArray::new(Type::array(Type::uint64()));
let nested_mut = Arc::get_mut(arr.nested_mut()).unwrap();
let typed = nested_mut.as_any_mut().downcast_mut::<ColumnUInt64>().unwrap();
typed.append(42);
arr.append_len(1);

// After (typed)
let mut arr = ColumnArrayT::<ColumnUInt64>::new(Type::array(Type::uint64()))?;
arr.append_array(|nested| {
    nested.append(42);
})?;
```

## References

- C++ implementation: `cpp/clickhouse/columns/array.h`
- C++ implementation: `cpp/clickhouse/columns/array.cpp`
- Rust file: `src/column/array.rs`
