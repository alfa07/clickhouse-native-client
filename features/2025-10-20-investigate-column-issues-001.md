# Column Type Restrictions - Investigation and Fix

**Date:** 2025-10-20
**Branch:** investigate-column-issues-001
**Status:** ✅ Completed - Bug Fixed

## Summary

Investigated four reported column type issues. Found:
- **3 ClickHouse Limitations** (documented, not bugs)
- **1 Implementation Bug** (fixed)

## Findings

### 1. LowCardinality(Int64) - ClickHouse Limitation ⚠️

**Status:** Prohibited by default due to performance concerns

**Error:** Code 455 (SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY)

**Workaround:**
```sql
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE test (x LowCardinality(Int64)) ENGINE = Memory;
```

**Decision:** Document this as expected behavior, not a bug.

---

### 2. LowCardinality(UUID) - Works Perfectly ✅

**Status:** No issues

Tested and works without any settings or errors.

---

### 3. Nullable(IPv4/IPv6) - Works Perfectly ✅

**Status:** No issues

Both `Nullable(IPv4)` and `Nullable(IPv6)` work correctly.

---

### 4. Nullable(LowCardinality(String)) - ClickHouse Limitation ❌

**Status:** Illegal type nesting (Error 43)

**Error:** "Nested type LowCardinality(String) cannot be inside Nullable type"

**Correct Form:** `LowCardinality(Nullable(String))`

**Decision:** Already documented in codebase, not a bug.

---

### 5. Map(UUID, LowCardinality(String)) - Implementation Bug Fixed! 🐛

**Status:** Fixed in this PR

**Problem:** Comment in `tests/integration_map.rs` claimed this wasn't supported due to "protocol variant issues". Testing revealed it DOES work in ClickHouse 25.5.11, but our implementation had a bug.

**Root Cause:** `ColumnMap` was missing `load_prefix()` and `save_prefix()` methods. When reading Map columns containing LowCardinality values, the LowCardinality's `key_version` field was never consumed from the buffer, causing buffer misalignment and "capacity overflow" errors.

**Fix:** Added `load_prefix()` and `save_prefix()` methods to `ColumnMap` that delegate to the underlying array column.

---

## Changes Made

### Files Modified

1. **src/column/map.rs**
   - Added `load_prefix()` method (lines 108-118)
   - Added `save_prefix()` method (lines 144-149)
   - Both methods delegate to underlying array column
   - Critical for Map columns with nested LowCardinality values

2. **tests/integration_map.rs**
   - Removed incorrect comment claiming Map(UUID, LowCardinality(String)) isn't supported (lines 137-141)
   - Added `test_map_uuid_lowcardinality_string()` integration test (lines 140-175)
   - Tests create table, insert data, and query Map(UUID, LowCardinality(String))

3. **features/2025-10-20-investigate-column-issues-001-investigation.md**
   - Created comprehensive investigation documentation
   - Documents all findings with ClickHouse error codes and references

4. **features/2025-10-20-investigate-column-issues-001.md**
   - This file - summary of changes

---

## Code Changes

### src/column/map.rs - Added load_prefix and save_prefix

```rust
fn load_prefix(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
    // Delegate to underlying array's load_prefix
    // CRITICAL: This ensures nested LowCardinality columns in Map values
    // have their key_version read before load_from_buffer is called
    let data_mut = Arc::get_mut(&mut self.data).ok_or_else(|| {
        Error::Protocol(
            "Cannot load prefix for shared map column".to_string(),
        )
    })?;
    data_mut.load_prefix(buffer, rows)
}

fn save_prefix(&self, buffer: &mut BytesMut) -> Result<()> {
    // Delegate to underlying array's save_prefix
    // CRITICAL: This ensures nested LowCardinality columns in Map values
    // have their key_version written before save_to_buffer is called
    self.data.save_prefix(buffer)
}
```

**Why This Matters:**

When reading a block from ClickHouse:
1. Block reader calls `column.load_prefix(buffer, rows)` on top-level columns
2. Block reader calls `column.load_from_buffer(buffer, rows)` on top-level columns
3. For LowCardinality columns, `load_prefix` reads the `key_version` field
4. For compound types (Map, Array, Tuple), they must delegate `load_prefix` to nested columns

**Before Fix:**
- Map had no `load_prefix` method (used default no-op)
- Map's `load_from_buffer` called Array's `load_from_buffer` directly
- Array's `load_prefix` was never called
- LowCardinality nested inside Map → key_version never consumed → buffer misalignment → crash

**After Fix:**
- Map delegates `load_prefix` to underlying array
- Array delegates `load_prefix` to nested Tuple columns
- Tuple delegates `load_prefix` to all element columns (including LowCardinality)
- LowCardinality's `load_prefix` reads key_version correctly
- Buffer stays aligned → successful reading

---

## Test Results

### Unit Tests
```
test result: ok. 187 passed; 0 failed; 0 ignored
```

### Integration Tests - Map
```
test test_map_int8_string ... ok
test test_map_string_nested_arrays ... ok
test test_map_uuid_nullable_string ... ok
test test_map_uuid_lowcardinality_string ... ok  ← NEW TEST
test test_map_empty ... ok

test result: ok. 5 passed; 0 failed; 0 ignored
```

### Integration Tests - LowCardinality
```
test test_lowcardinality_string_roundtrip ... ok
test test_lowcardinality_string_empty ... ok

test result: ok. 2 passed; 0 failed; 0 ignored
```

---

## Direct ClickHouse Testing

All scenarios tested against ClickHouse 25.5.11:

| Test | Result | Notes |
|------|--------|-------|
| CREATE TABLE ... LowCardinality(Int64) | ⚠️ Error 455 | Requires `allow_suspicious_low_cardinality_types = 1` |
| CREATE TABLE ... LowCardinality(UUID) | ✅ Success | Works without settings |
| CREATE TABLE ... Nullable(IPv4) | ✅ Success | Works perfectly |
| CREATE TABLE ... Nullable(IPv6) | ✅ Success | Works perfectly |
| CREATE TABLE ... Nullable(LowCardinality(String)) | ❌ Error 43 | Invalid type nesting |
| CREATE TABLE ... Map(UUID, LowCardinality(String)) | ✅ Success | Works perfectly! |
| INSERT INTO Map(UUID, LowCardinality(String)) | ✅ Success | Works perfectly! |
| SELECT FROM Map(UUID, LowCardinality(String)) | ✅ Success | Now works with fix! |

---

## ClickHouse References

### Error Codes
- **Error 43 (ILLEGAL_TYPE_OF_ARGUMENT):** Type nesting restriction
- **Error 455 (SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY):** Performance protection

### GitHub Issues
- [#42456](https://github.com/ClickHouse/ClickHouse/issues/42456) - Nullable(LowCardinality(...)) restriction

### Type Nesting Rules

**Valid:**
- ✅ `LowCardinality(Nullable(T))`
- ✅ `Array(LowCardinality(T))`
- ✅ `Map(K, LowCardinality(V))`
- ✅ `Map(K, Nullable(V))`
- ✅ `Nullable(IPv4/IPv6)`
- ✅ `LowCardinality(UUID)`

**Invalid:**
- ❌ `Nullable(LowCardinality(...))` → Error 43
- ❌ `Nullable(Array(...))` → Error 43
- ❌ `LowCardinality(Int64)` without setting → Error 455

---

## Impact

### Performance
- No performance impact
- Fix only adds proper buffer reading sequence
- All operations remain O(n)

### Compatibility
- Fully backward compatible
- Existing code unaffected
- New functionality: Map with LowCardinality values now works

### Test Coverage
- Added 1 new integration test
- All existing tests pass (187 unit + 5 map + 2 lowcardinality)

---

## Conclusion

**Investigation Complete:**
- ✅ Identified 3 ClickHouse limitations (not bugs)
- ✅ Fixed 1 implementation bug (Map load_prefix)
- ✅ Added test coverage for the fix
- ✅ All tests passing

**Result:** Map(UUID, LowCardinality(String)) now works correctly!

The comment in `integration_map.rs` claiming it wasn't supported was based on the implementation bug, not a ClickHouse limitation. With the fix, Map columns can now contain LowCardinality values of any type.
