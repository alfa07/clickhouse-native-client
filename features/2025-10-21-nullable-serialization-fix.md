# Nullable Column Serialization Fix

**Date**: 2025-10-21
**Branch**: `add-block-integration-tests`
**Status**: ✅ **Fixed** - All Nullable tests now passing

## Issue Summary

All Nullable column Block integration tests were failing in CI with error:
```
DB::Exception (code 432): Unknown codec family code: 141
```

This error occurred for:
- Nullable(String)
- Nullable(IPv6)
- Nullable(UUID)
- Nullable(Int64)

## Root Cause

After examining the C++ reference implementation (`cpp/clickhouse/columns/nullable.cpp` lines 97-104), discovered that **when appending NULL to a Nullable column, you MUST append a placeholder value to the nested column** to maintain size alignment between the null bitmap and nested data.

### Why This Matters

The Nullable column wire format consists of:
1. Null bitmap (1 byte per row: 0 = non-null, 1 = null)
2. Nested column data (all rows, including placeholders for nulls)

**Critical invariant**: `null_bitmap.len() == nested_column.size()`

When we only called `append_null()` without appending a placeholder, we created:
- Null bitmap: [0, 1, 0] (3 bytes)
- Nested column: ["hello", "world"] (2 elements)

ClickHouse tried to read 3 elements but found only 2, reading garbage bytes (141/0x8D) as a codec family code.

## Solution Applied

Added placeholder values for NULL in all Nullable test files:

### Nullable(String) - 4 occurrences fixed
```rust
// BEFORE (WRONG):
nullable_col.append_null();

// AFTER (CORRECT):
nullable_col.append_null();
Arc::get_mut(nullable_col.nested_mut())
    .unwrap()
    .as_any_mut()
    .downcast_mut::<ColumnString>()
    .unwrap()
    .append(""); // Placeholder for null value
```

### Nullable(IPv6) - 4 occurrences fixed
```rust
nullable_col.append_null();
Arc::get_mut(nullable_col.nested_mut())
    .unwrap()
    .as_any_mut()
    .downcast_mut::<ColumnIpv6>()
    .unwrap()
    .append([0u8; 16]); // Placeholder for null value
```

### Nullable(UUID) - 4 occurrences fixed
```rust
nullable_col.append_null();
Arc::get_mut(nullable_col.nested_mut())
    .unwrap()
    .as_any_mut()
    .downcast_mut::<ColumnUuid>()
    .unwrap()
    .append(Uuid::new(0, 0)); // Placeholder for null value
```

### Nullable(Int64) - 4 occurrences fixed
```rust
nullable_col.append_null();
Arc::get_mut(nullable_col.nested_mut())
    .unwrap()
    .as_any_mut()
    .downcast_mut::<ColumnInt64>()
    .unwrap()
    .append(0); // Placeholder for null value
```

## C++ Reference Implementation

From `cpp/clickhouse/columns/nullable.cpp` lines 97-104:
```cpp
template <typename ValueType>
inline void Append(ValueType value) {
    ColumnNullable::Append(!value.has_value());
    if (value.has_value()) {
        typed_nested_data_->Append(std::move(*value));
    } else {
        typed_nested_data_->Append(typename ValueType::value_type{});  // Always append placeholder!
    }
}
```

This confirms the pattern: **ALWAYS append to nested column, even for NULL values**.

## Files Modified

- `tests/integration_block_nullable_string.rs` - 4 fixes (lines 51, 154, 236, 319)
- `tests/integration_block_nullable_ipv6.rs` - 4 fixes (lines 51, 148, 223, 310)
- `tests/integration_block_nullable_uuid.rs` - 4 fixes (lines 51, 169, 251, 337)
- `tests/integration_block_nullable_int64.rs` - 4 fixes (lines 50, 155, 237, 390)

## Test Results

### Before Fix
```
❌ test_nullable_string_block_insert_basic ... FAILED
❌ test_nullable_string_block_insert_boundary ... FAILED
❌ test_nullable_string_block_insert_all_nulls ... FAILED
❌ test_nullable_string_block_insert_random ... FAILED
(Same for IPv6, UUID, and Int64)
```

### After Fix
```
✅ All Nullable(String) tests pass (4/4)
✅ All Nullable(IPv6) tests pass (4/4)
✅ All Nullable(UUID) tests pass (4/4)
✅ All Nullable(Int64) tests pass (5/5 - includes all_non_null test)
```

### CI Results

**Latest Run**: https://github.com/alfa07/clickhouse-native-client/actions/runs/18692464761

- ✅ Format Check: PASS
- ✅ Unit Tests: PASS
- ✅ Clippy: PASS
- ✅ Build (Release): PASS
- ✅ All Nullable Type Tests: PASS (16/16 tests across 4 files)
- ✅ All Numeric Type Tests: PASS
- ✅ All String/UUID Type Tests: PASS
- ✅ All Date/Time Type Tests: PASS
- ✅ All Array Type Tests: PASS
- ✅ All Other Simple Type Tests: PASS
- ⚠️ LowCardinality Type Tests: FAIL (pre-existing bug, not related to NULL serialization)

## Commits

1. `cbf3448` - fix: add placeholder values for NULL in Nullable(String) tests
2. `149f35e` - fix: add placeholder values for NULL in Nullable(IPv6) tests
3. `5a49c02` - fix: add placeholder values for NULL in Nullable(UUID) tests
4. `2394b0c` - fix: add placeholder values for NULL in Nullable(Int64) tests

## Conclusion

Successfully resolved the "Unknown codec family code: 141" error by implementing the correct Nullable column serialization pattern from the C++ reference implementation. All 16 Nullable column tests now pass in both local and CI environments.

The key insight: **Nullable columns must maintain size alignment between null bitmap and nested data by always appending placeholder values for NULL entries.**

## Remaining Work

LowCardinality tests have unrelated failures (not NULL serialization issues). These appear to be bugs in the test generation logic and should be addressed separately.
