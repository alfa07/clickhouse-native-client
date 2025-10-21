# Improve Decimal Column Implementation

**Date:** 2025-10-20
**Branch:** improve-decimal
**Status:** ✅ Complete - All tests passing (187 unit + 27 integration)

## Summary

Refactored `ColumnDecimal` to use efficient internal representation based on precision, matching the C++ clickhouse-cpp implementation. Instead of always storing decimals as `i128`, the column now delegates to `ColumnInt32`, `ColumnInt64`, or `ColumnInt128` based on precision.

## Changes

### Core Refactoring

**File:** `src/column/decimal.rs`

#### 1. Changed Internal Data Representation

**Before:**
```rust
pub struct ColumnDecimal {
    type_: Type,
    precision: usize,
    scale: usize,
    data: Vec<i128>, // Always i128 - inefficient!
}
```

**After:**
```rust
pub struct ColumnDecimal {
    type_: Type,
    precision: usize,
    scale: usize,
    data: ColumnRef, // Delegates to ColumnInt32/Int64/Int128
}
```

#### 2. Precision-Based Column Selection

Matches C++ implementation logic:

```rust
let data: ColumnRef = if precision <= 9 {
    Arc::new(ColumnInt32::new(Type::int32()))  // 4 bytes per value
} else if precision <= 18 {
    Arc::new(ColumnInt64::new(Type::int64()))  // 8 bytes per value
} else {
    Arc::new(ColumnInt128::new(Type::int128())) // 16 bytes per value
};
```

#### 3. Simplified I/O Operations

Delegate to underlying column for efficient bulk operations:

**Before:**
```rust
fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
    // Manual byte parsing with match on precision
    for _ in 0..rows {
        let value = match bytes_per_value {
            4 => buffer.get_i32_le() as i128,
            8 => buffer.get_i64_le() as i128,
            16 => { /* complex i128 parsing */ }
            _ => unreachable!(),
        };
        self.data.push(value);
    }
}
```

**After:**
```rust
fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
    // Delegate to underlying column - uses efficient bulk copy
    let data_mut = Arc::get_mut(&mut self.data).expect("Cannot modify shared column");
    data_mut.load_from_buffer(buffer, rows)
}
```

#### 4. Updated Method Implementations

All methods now delegate to the underlying column:
- `append()` - downcasts and appends to Int32/Int64/Int128
- `at()` - downcasts and retrieves value, converting to i128
- `len()`, `is_empty()` - delegates to `data.size()`
- `clear()`, `reserve()` - delegates to underlying column
- `append_column()` - delegates column appending
- `slice()` - delegates slicing to underlying column

## Benefits

### 1. Memory Efficiency

**Precision 9 (Decimal(9,2)):**
- Before: 16 bytes per value (i128)
- After: 4 bytes per value (i32)
- **Savings: 75%**

**Precision 18 (Decimal(18,4)):**
- Before: 16 bytes per value (i128)
- After: 8 bytes per value (i64)
- **Savings: 50%**

**Precision 38 (Decimal(38,10)):**
- Before: 16 bytes per value (i128)
- After: 16 bytes per value (i128)
- **No overhead**

### 2. Performance Improvements

- Leverages existing optimized bulk copy operations from `ColumnVector<T>`
- Reduces cache pressure with smaller data types
- Matches wire protocol size exactly (no conversion overhead)

### 3. Code Simplification

- Removed manual byte parsing logic (40+ lines eliminated)
- Centralized column operations through delegation
- Easier to maintain - changes to numeric columns automatically apply

## Testing

### Unit Tests Added (16 total)

1. **Type Selection Tests:**
   - `test_decimal_uses_int32_for_precision_9`
   - `test_decimal_uses_int64_for_precision_18`
   - `test_decimal_uses_int128_for_precision_38`

2. **Memory Efficiency Tests:**
   - `test_decimal_memory_efficiency` - verifies byte-level efficiency

3. **Bulk Copy Tests:**
   - `test_decimal_bulk_copy_int32` - 5 values
   - `test_decimal_bulk_copy_int64` - 5 values with large numbers
   - `test_decimal_bulk_copy_int128` - 5 values with very large numbers
   - `test_decimal_bulk_copy_large_dataset` - 10,000 values

4. **Operation Tests:**
   - `test_decimal_append_column` - column concatenation
   - `test_decimal_slice` - slicing operations
   - `test_decimal_clear_and_reuse` - clear and reuse
   - `test_decimal_with_data_constructor` - constructor testing

5. **Existing Tests (maintained):**
   - `test_parse_decimal`
   - `test_format_decimal`
   - `test_decimal_column`
   - `test_decimal_precision_scale`

### Test Results

✅ **Unit Tests:** 187/187 passed
✅ **Integration Tests:** 27/27 passed

All existing tests continue to pass, confirming backward compatibility.

## C++ Reference Implementation

Based on `clickhouse-cpp/clickhouse/columns/decimal.cpp`:

```cpp
ColumnDecimal::ColumnDecimal(size_t precision, size_t scale)
    : Column(Type::CreateDecimal(precision, scale))
{
    if (precision <= 9) {
        data_ = std::make_shared<ColumnInt32>();
    } else if (precision <= 18) {
        data_ = std::make_shared<ColumnInt64>();
    } else {
        data_ = std::make_shared<ColumnInt128>();
    }
}
```

Our Rust implementation now mirrors this approach exactly.

## Backward Compatibility

✅ **Public API unchanged** - all existing code using `ColumnDecimal` continues to work
✅ **Wire format unchanged** - serialization/deserialization remains identical
✅ **All integration tests pass** - verified with actual ClickHouse server

## Performance Impact

Expected improvements for common use cases:

| Decimal Type | Storage Reduction | Typical Use Case |
|--------------|-------------------|------------------|
| Decimal(9,2) | 75% | Currency (e.g., USD with cents) |
| Decimal(18,4) | 50% | High-precision financial data |
| Decimal(38,10) | 0% | Maximum precision decimals |

For a table with 1 million Decimal(9,2) values:
- Before: ~16 MB
- After: ~4 MB
- **Memory savings: 12 MB per million rows**

## Files Modified

- `src/column/decimal.rs` - Core implementation refactored
  - Added imports for `ColumnInt32`, `ColumnInt64`, `ColumnInt128`
  - Removed `BufMut` import (unused after refactoring)
  - Simplified all Column trait methods
  - Added 12 new unit tests

## Related Documentation

See `CLAUDE.md` for context on:
- Column implementation patterns in this codebase
- C++ vs Rust delegation patterns
- Bulk copy safety (reserve() + set_len() pattern)

## Next Steps

This refactoring provides a foundation for similar optimizations:
- [ ] Consider similar delegation for other complex types
- [ ] Benchmark real-world performance improvements
- [ ] Document best practices for column delegation pattern
