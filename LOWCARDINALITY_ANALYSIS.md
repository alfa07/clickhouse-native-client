# LowCardinality Implementation Analysis: C++ vs Rust

## Critical Differences Found

### 1. **Index Column Type** ⚠️ CRITICAL - Memory Inefficiency

**C++ (Dynamic):**
```cpp
ColumnRef index_column_;  // Can be UInt8, UInt16, UInt32, or UInt64
```
- Creates appropriate size based on dictionary size
- UInt8 for dict < 256 (1 byte per index)
- UInt16 for dict < 65536 (2 bytes per index)
- UInt32 for dict < 4B (4 bytes per index)
- UInt64 for larger (8 bytes per index)

**Rust (Always 64-bit):**
```rust
indices: Vec<u64>,  // Always 8 bytes per index
```

**Impact:**
- Dictionary with 100 unique values, 1M rows:
  - C++: 1MB (1 byte × 1M)
  - Rust: 8MB (8 bytes × 1M)
  - **8x memory waste!**

---

### 2. **Wire Protocol SaveBody** ⚠️ CRITICAL - Protocol Mismatch

**C++ (Correct Protocol):**
```cpp
void SaveBody(OutputStream* output) {
    // 1. index_serialization_type with flags
    const uint64_t index_serialization_type =
        indexTypeFromIndexColumn(*index_column_) | IndexFlag::HasAdditionalKeysBit;
    WireFormat::WriteFixed(*output, index_serialization_type);

    // 2. number_of_keys
    const uint64_t number_of_keys = dictionary_column_->Size();
    WireFormat::WriteFixed(*output, number_of_keys);

    // 3. Dictionary data (for Nullable, only nested part)
    if (auto columnNullable = dictionary_column_->As<ColumnNullable>()) {
        columnNullable->Nested()->SaveBody(output);
    } else {
        dictionary_column_->SaveBody(output);
    }

    // 4. number_of_rows
    const uint64_t number_of_rows = index_column_->Size();
    WireFormat::WriteFixed(*output, number_of_rows);

    // 5. Index data
    index_column_->SaveBody(output);
}
```

**Rust (Simplified - WRONG):**
```rust
fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
    buffer.put_u64_le(1);  // version (should be in SavePrefix!)
    buffer.put_u64_le(self.dictionary.size() as u64);  // Missing index_serialization_type!
    self.dictionary.save_to_buffer(buffer)?;  // Wrong for Nullable!
    for &index in &self.indices {
        buffer.put_u64_le(index);  // Should use dynamic type!
    }
    Ok(())
}
```

**Missing:**
- index_serialization_type field
- HasAdditionalKeysBit flag
- number_of_rows field
- Special handling for Nullable dictionary (only save nested)
- Dynamic index size

---

### 3. **LoadPrefix/SavePrefix Separation** ⚠️ Missing

**C++:**
```cpp
bool LoadPrefix(InputStream* input, size_t) {
    uint64_t key_version;
    if (!WireFormat::ReadFixed(*input, &key_version))
        throw ProtocolError("Failed to read key serialization version.");
    if (key_version != KeySerializationVersion::SharedDictionariesWithAdditionalKeys)
        throw ProtocolError("Invalid key serialization version value.");
    return true;
}

void SavePrefix(OutputStream* output) {
    const auto version = static_cast<uint64_t>(
        KeySerializationVersion::SharedDictionariesWithAdditionalKeys);
    WireFormat::WriteFixed(*output, version);
}
```

**Rust:**
- No LoadPrefix/SavePrefix methods
- load_from_buffer reads key_version inline
- save_to_buffer writes version inline

**Impact:** Inconsistent with Column trait pattern

---

### 4. **Null/Default Item Initialization** ⚠️ Protocol Compliance

**C++ Constructor:**
```cpp
ColumnLowCardinality::ColumnLowCardinality(std::shared_ptr<ColumnNullable> dictionary_column)
    : dictionary_column_(dictionary_column->CloneEmpty())
{
    AppendNullItem();     // Position 0 = null
    Setup(dictionary_column);
}

void Setup(ColumnRef dictionary_column) {
    AppendDefaultItem();  // Position 1 = default (or 0 for non-nullable)
    // ... then add actual values
}
```

**Rust Constructor:**
```rust
pub fn new(type_: Type) -> Self {
    let dictionary = create_column(&dictionary_type).expect("...");
    Self {
        dictionary,
        indices: Vec::new(),
        unique_map: HashMap::new(),
    }
    // ❌ No null/default items added!
}
```

**Impact:** Missing required protocol items for nullable dictionaries

---

### 5. **Slice Implementation** - Memory Inefficiency

**C++ (Compact):**
```cpp
ColumnRef Slice(size_t begin, size_t len) const {
    auto result = std::make_shared<ColumnLowCardinality>(
        dictionary_column_->CloneEmpty());

    // Only add referenced items
    for (size_t i = begin; i < begin + len; ++i)
        result->AppendUnsafe(this->GetItem(i));

    return result;
}
```
Result: New compact dictionary with only referenced items

**Rust (Copies Everything):**
```rust
fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
    let mut sliced = ColumnLowCardinality::new(self.type_.clone());
    sliced.dictionary = self.dictionary.clone();     // Entire dict!
    sliced.indices = self.indices[begin..begin + len].to_vec();
    sliced.unique_map = self.unique_map.clone();      // Entire map!
    Ok(Arc::new(sliced))
}
```
Result: Full dictionary even if slice uses 2 unique values from 10,000

---

### 6. **Reserve Heuristic** - Optimization Missing

**C++:**
```cpp
void Reserve(size_t new_cap) {
    // Smart estimation: dictionary size ≈ sqrt(row count)
    dictionary_column_->Reserve(
        static_cast<size_t>(ceil(sqrt(static_cast<double>(new_cap)))));
    index_column_->Reserve(new_cap + 2); // +2 for null/default
}
```

**Rust:**
```rust
fn reserve(&mut self, new_cap: usize) {
    self.indices.reserve(new_cap);
    // ❌ No dictionary reservation!
}
```

**Impact:** No dictionary pre-allocation, more reallocations during insertion

---

### 7. **Clear Method** - Different Semantics

**C++ (Reinitialize):**
```cpp
void Clear() {
    index_column_->Clear();
    dictionary_column_->Clear();
    unique_items_map_.clear();

    // Re-add required items
    if (auto columnNullable = dictionary_column_->As<ColumnNullable>()) {
        AppendNullItem();
    }
    AppendDefaultItem();
}
```

**Rust (Preserve Dictionary):**
```rust
fn clear(&mut self) {
    self.indices.clear();
    self.unique_map.clear();
    // Note: We don't clear the dictionary to preserve unique values
}
```

**Impact:** Different behavior - Rust preserves dictionary, C++ resets completely

---

## What Works Correctly ✅

1. **Hash-based deduplication** - Matches C++ perfectly
2. **Two-hash collision reduction** - Uses (u64, u64) tuple
3. **append_unsafe logic** - Correct deduplication flow
4. **append_column with merging** - Correct hash-based merge
5. **LoadBody format** - Correctly reads wire protocol
6. **Nullable dictionary loading** - Correct nested-only reading

---

## Priority Fixes Needed

### Priority 1: Protocol Correctness (Breaks Interop)
1. Fix save_to_buffer wire format
2. Add LoadPrefix/SavePrefix separation

### Priority 2: Memory Efficiency (Production Impact)
3. Add dynamic index column type
4. Fix Slice to compact dictionary

### Priority 3: Protocol Compliance (Edge Cases)
5. Add null/default item initialization
6. Add Reserve dictionary heuristic

---

## Test Coverage Needed

1. **Dynamic Index Type Tests:**
   - Small dictionary (<256 items) → UInt8 indices
   - Medium dictionary (<65536 items) → UInt16 indices
   - Large dictionary → UInt32/UInt64 indices

2. **Wire Format Tests:**
   - Save/Load round-trip
   - Verify index_serialization_type flags
   - Nullable dictionary save (only nested)

3. **Slice Tests:**
   - Verify compaction (unreferenced items removed)
   - Memory usage comparison

4. **Nullable Tests:**
   - Null item at position 0
   - Default item initialization
   - Clear preserves null/default

5. **Reserve Tests:**
   - Dictionary pre-allocation
   - Performance comparison with/without Reserve

---

## Memory Impact Examples

**Scenario: 1M rows, 100 unique values**

| Component | C++ | Rust (Current) | Rust (Fixed) |
|-----------|-----|----------------|--------------|
| Index | 1 MB (UInt8) | 8 MB (u64) | 1 MB (UInt8) |
| Dictionary | ~10 KB | ~10 KB | ~10 KB |
| Hash Map | ~2 KB | ~2 KB | ~2 KB |
| **Total** | **~1 MB** | **~8 MB** | **~1 MB** |
| **Waste** | - | **7 MB (700%)** | - |

**Scenario: 1M rows, 50K unique values**

| Component | C++ | Rust (Current) | Rust (Fixed) |
|-----------|-----|----------------|--------------|
| Index | 2 MB (UInt16) | 8 MB (u64) | 2 MB (UInt16) |
| Dictionary | ~500 KB | ~500 KB | ~500 KB |
| Hash Map | ~1 MB | ~1 MB | ~1 MB |
| **Total** | **~3.5 MB** | **~9.5 MB** | **~3.5 MB** |
| **Waste** | - | **6 MB (171%)** | - |

---

## Implementation Strategy

1. **Phase 1:** Fix protocol correctness (save_to_buffer, LoadPrefix/SavePrefix)
2. **Phase 2:** Add dynamic index type (biggest impact)
3. **Phase 3:** Fix Slice compaction
4. **Phase 4:** Add null/default initialization
5. **Phase 5:** Optimize Reserve

Each phase: implement → test → commit
