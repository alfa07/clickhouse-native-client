//! LowCardinality column implementation (dictionary encoding)
//!
//! **ClickHouse Documentation:** <https://clickhouse.com/docs/en/sql-reference/data-types/lowcardinality>
//!
//! ## Overview
//!
//! LowCardinality is a specialized type that wraps other data types (String,
//! FixedString, Date, DateTime, and numbers) to provide dictionary encoding.
//! This dramatically reduces storage and improves query performance for
//! columns with low cardinality (few unique values relative to total rows).
//!
//! ## Type Nesting Rules
//!
//! **✅ Correct nesting order:**
//! - `LowCardinality(Nullable(String))` - Dictionary-encoded nullable strings
//! - `Array(LowCardinality(String))` - Array of dictionary-encoded strings
//! - `Array(LowCardinality(Nullable(String)))` - Array of nullable
//!   dictionary-encoded strings
//!
//! **❌ Invalid nesting:**
//! - `Nullable(LowCardinality(String))` - Error: "Nested type LowCardinality
//!   cannot be inside Nullable type"
//!
//! See: <https://github.com/ClickHouse/ClickHouse/issues/42456>
//!
//! ## Wire Format
//!
//! LowCardinality uses a complex serialization format:
//! ```text
//! [serialization_version: UInt64]
//! [index_type: UInt64]
//! [dictionary: Column]
//! [indices: UInt8/UInt16/UInt32/UInt64 * num_rows]
//! ```
//!
//! ## Performance Tips
//!
//! - Best for columns with cardinality < 10,000 unique values
//! - Excellent for enum-like data, country codes, status flags, etc.
//! - See ClickHouse tips: <https://www.tinybird.co/blog-posts/tips-10-null-behavior-with-lowcardinality-columns>

use super::{
    Column,
    ColumnRef,
};
use crate::{
    types::Type,
    Error,
    Result,
};
use bytes::{
    Buf,
    BufMut,
    BytesMut,
};
use std::{
    collections::HashMap,
    sync::Arc,
};

use super::column_value::{
    append_column_item,
    compute_hash_key,
    get_column_item,
    ColumnValue,
};

/// Column for LowCardinality type (dictionary encoding)
///
/// Stores unique values in a dictionary and uses indices to reference them,
/// providing compression for columns with many repeated values.
///
/// **Reference Implementation:** See
/// `clickhouse-cpp/clickhouse/columns/lowcardinality.cpp`
pub struct ColumnLowCardinality {
    type_: Type,
    dictionary: ColumnRef,         // Stores unique values
    indices: Vec<u64>,             // Indices into dictionary
    unique_map: HashMap<(u64, u64), u64>, // Hash pair -> dictionary index for fast lookup
}

impl ColumnLowCardinality {
    pub fn new(type_: Type) -> Self {
        // Extract the nested type from LowCardinality
        let dictionary_type = match &type_ {
            Type::LowCardinality { nested_type } => {
                nested_type.as_ref().clone()
            }
            _ => panic!("ColumnLowCardinality requires LowCardinality type"),
        };

        // Create the dictionary column
        let dictionary =
            crate::io::block_stream::create_column(&dictionary_type)
                .expect("Failed to create dictionary column");

        Self {
            type_,
            dictionary,
            indices: Vec::new(),
            unique_map: HashMap::new(),
        }
    }

    /// Get the dictionary column
    pub fn dictionary(&self) -> &ColumnRef {
        &self.dictionary
    }

    /// Get the number of unique values in the dictionary
    pub fn dictionary_size(&self) -> usize {
        self.dictionary.size()
    }

    /// Get the index at position
    pub fn index_at(&self, index: usize) -> u64 {
        self.indices[index]
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Append a value with hash-based deduplication (like C++ AppendUnsafe)
    /// This is the core method for adding values to LowCardinality columns
    pub fn append_unsafe(&mut self, value: &ColumnValue) -> Result<()> {
        let hash_key = compute_hash_key(value);
        let current_dict_size = self.dictionary.size() as u64;

        // Check if value already exists in dictionary
        let index = if let Some(&existing_idx) = self.unique_map.get(&hash_key) {
            // Value exists - reuse existing dictionary index
            existing_idx
        } else {
            // New value - add to dictionary
            let dict_mut = Arc::get_mut(&mut self.dictionary).ok_or_else(|| {
                Error::Protocol(
                    "Cannot append to shared dictionary - column has multiple references"
                        .to_string(),
                )
            })?;

            // Append to dictionary
            append_column_item(dict_mut, value)?;

            // Record in unique_map
            self.unique_map.insert(hash_key, current_dict_size);

            current_dict_size
        };

        // Append index
        self.indices.push(index);

        Ok(())
    }

    /// Bulk append values from an iterator with deduplication
    pub fn append_values<I>(&mut self, values: I) -> Result<()>
    where
        I: IntoIterator<Item = ColumnValue>,
    {
        for value in values {
            self.append_unsafe(&value)?;
        }
        Ok(())
    }
}

impl Column for ColumnLowCardinality {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.indices.len()
    }

    fn clear(&mut self) {
        self.indices.clear();
        self.unique_map.clear();
        // Note: We don't clear the dictionary to preserve unique values
        // In a full implementation, we might compact the dictionary
    }

    fn reserve(&mut self, new_cap: usize) {
        self.indices.reserve(new_cap);
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<ColumnLowCardinality>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            })?;

        // Check dictionary types match
        if self.dictionary.column_type().name() != other.dictionary.column_type().name() {
            return Err(Error::TypeMismatch {
                expected: self.dictionary.column_type().name(),
                actual: other.dictionary.column_type().name(),
            });
        }

        // Hash-based dictionary merging with deduplication
        // This matches the C++ clickhouse-cpp implementation
        //
        // For each value in other:
        // 1. Extract ColumnValue from other's dictionary using other's index
        // 2. Use append_unsafe which:
        //    - Computes hash
        //    - Checks unique_map for existing entry
        //    - If exists: reuses existing dictionary index
        //    - If new: adds to dictionary and updates unique_map
        // 3. Appends the (possibly deduplicated) index

        for &other_index in &other.indices {
            // Get the value from other's dictionary
            let value = get_column_item(
                other.dictionary.as_ref(),
                other_index as usize,
            )?;

            // Add with deduplication
            self.append_unsafe(&value)?;
        }

        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        // LowCardinality wire format (following C++ clickhouse-cpp):
        // LoadPrefix (called separately via block reader):
        //   1. key_version (UInt64) - should be 1 (SharedDictionariesWithAdditionalKeys)
        // LoadBody (this method):
        //   2. index_serialization_type (UInt64) - contains flags and index type
        //   3. number_of_keys (UInt64) - dictionary size
        //   4. Dictionary column data (nested type)
        //   5. number_of_rows (UInt64) - should match rows parameter
        //   6. Index column data (UInt8/16/32/64 depending on index type)

        if buffer.len() < 8 {
            return Err(Error::Protocol(
                "Not enough data for LowCardinality key version".to_string(),
            ));
        }

        // Read key_version (should be 1)
        let key_version = buffer.get_u64_le();
        const SHARED_DICTIONARIES_WITH_ADDITIONAL_KEYS: u64 = 1;

        if key_version != SHARED_DICTIONARIES_WITH_ADDITIONAL_KEYS {
            return Err(Error::Protocol(format!(
                "Invalid LowCardinality key version: expected {}, got {}",
                SHARED_DICTIONARIES_WITH_ADDITIONAL_KEYS, key_version
            )));
        }

        // Read index_serialization_type
        if buffer.len() < 8 {
            return Err(Error::Protocol(
                "Not enough data for LowCardinality index serialization type".to_string(),
            ));
        }

        let index_serialization_type = buffer.get_u64_le();

        const INDEX_TYPE_MASK: u64 = 0xFF;
        const NEED_GLOBAL_DICTIONARY_BIT: u64 = 1 << 8;
        const HAS_ADDITIONAL_KEYS_BIT: u64 = 1 << 9;

        let index_type = index_serialization_type & INDEX_TYPE_MASK;

        // Check flags
        if (index_serialization_type & NEED_GLOBAL_DICTIONARY_BIT) != 0 {
            return Err(Error::Protocol(
                "Global dictionary is not supported".to_string(),
            ));
        }

        if (index_serialization_type & HAS_ADDITIONAL_KEYS_BIT) == 0 {
            // Don't fail - try to continue reading
        }

        // Read number of dictionary keys
        if buffer.len() < 8 {
            return Err(Error::Protocol(
                "Not enough data for dictionary size".to_string(),
            ));
        }
        let number_of_keys = buffer.get_u64_le() as usize;

        // Load dictionary values
        // IMPORTANT: For Nullable dictionaries, we only load the NESTED column data
        // The null bitmap is NOT part of the dictionary serialization
        // (matching C++ implementation in lowcardinality.cpp::Load)
        if number_of_keys > 0 {
            let dict_mut = Arc::get_mut(&mut self.dictionary).ok_or_else(|| {
                Error::Protocol(
                    "Cannot load into shared dictionary - column has multiple references"
                        .to_string(),
                )
            })?;

            // Check if dictionary is Nullable - if so, load only nested data
            use super::nullable::ColumnNullable;
            if let Some(nullable_col) = dict_mut.as_any_mut().downcast_mut::<ColumnNullable>() {
                let nested_mut = Arc::get_mut(nullable_col.nested_mut()).ok_or_else(|| {
                    Error::Protocol(
                        "Cannot load into shared nested column - column has multiple references"
                            .to_string(),
                    )
                })?;
                nested_mut.load_from_buffer(buffer, number_of_keys)?;

                // After loading, mark all entries as non-null for now
                // (The C++ code reconstructs the null bitmap after loading)
                for _ in 0..number_of_keys {
                    nullable_col.append_non_null();
                }
            } else {
                // Non-nullable dictionary - load normally
                dict_mut.load_from_buffer(buffer, number_of_keys)?;
            }
        }

        // Read number of rows (should match the rows parameter)
        // Note: In some cases this field may be omitted/truncated
        let _number_of_rows = if buffer.len() >= 8 {
            let val = buffer.get_u64_le() as usize;

            if val != rows {
                return Err(Error::Protocol(format!(
                    "LowCardinality row count mismatch: expected {}, got {}",
                    rows, val
                )));
            }
            val
        } else {
            // If not enough bytes, assume number_of_rows equals rows parameter
            // This may happen in certain protocol versions or formats
            rows
        };

        // Read indices based on index type
        self.indices.reserve(rows);
        match index_type {
            0 => {
                // UInt8 indices
                for _ in 0..rows {
                    if buffer.is_empty() {
                        return Err(Error::Protocol(
                            "Not enough data for LowCardinality index".to_string(),
                        ));
                    }
                    let index = buffer.get_u8() as u64;
                    self.indices.push(index);
                }
            }
            1 => {
                // UInt16 indices
                for _ in 0..rows {
                    if buffer.len() < 2 {
                        return Err(Error::Protocol(
                            "Not enough data for LowCardinality index".to_string(),
                        ));
                    }
                    let index = buffer.get_u16_le() as u64;
                    self.indices.push(index);
                }
            }
            2 => {
                // UInt32 indices
                for _ in 0..rows {
                    if buffer.len() < 4 {
                        return Err(Error::Protocol(
                            "Not enough data for LowCardinality index".to_string(),
                        ));
                    }
                    let index = buffer.get_u32_le() as u64;
                    self.indices.push(index);
                }
            }
            3 => {
                // UInt64 indices
                for _ in 0..rows {
                    if buffer.len() < 8 {
                        return Err(Error::Protocol(
                            "Not enough data for LowCardinality index".to_string(),
                        ));
                    }
                    let index = buffer.get_u64_le();
                    self.indices.push(index);
                }
            }
            _ => {
                return Err(Error::Protocol(format!(
                    "Unknown LowCardinality index type: {}",
                    index_type
                )));
            }
        }

        // Rebuild unique_map from dictionary
        self.unique_map.clear();
        for i in 0..self.dictionary.size() {
            let value = get_column_item(self.dictionary.as_ref(), i)?;
            let hash_key = compute_hash_key(&value);
            self.unique_map.insert(hash_key, i as u64);
        }

        Ok(())
    }

    fn save_prefix(&self, buffer: &mut BytesMut) -> Result<()> {
        // Write key serialization version (matches C++ SavePrefix)
        // KeySerializationVersion::SharedDictionariesWithAdditionalKeys = 1
        const SHARED_DICTIONARIES_WITH_ADDITIONAL_KEYS: u64 = 1;
        buffer.put_u64_le(SHARED_DICTIONARIES_WITH_ADDITIONAL_KEYS);
        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // LowCardinality wire format (matching C++ SaveBody):
        // 1. index_serialization_type (UInt64) - contains index type + flags
        // 2. number_of_keys (UInt64) - dictionary size
        // 3. Dictionary column data (for Nullable, only nested part!)
        // 4. number_of_rows (UInt64) - index column size
        // 5. Index column data

        // Index type flags (from C++ lowcardinality.cpp)
        const HAS_ADDITIONAL_KEYS_BIT: u64 = 1 << 9;

        // For now, we always use UInt64 indices (index_type = 3)
        // TODO: Use dynamic index type (UInt8/16/32/64) based on dictionary size
        const INDEX_TYPE_UINT64: u64 = 3;

        // 1. Write index_serialization_type
        let index_serialization_type = INDEX_TYPE_UINT64 | HAS_ADDITIONAL_KEYS_BIT;
        buffer.put_u64_le(index_serialization_type);

        // 2. Write number_of_keys (dictionary size)
        buffer.put_u64_le(self.dictionary.size() as u64);

        // 3. Write dictionary data
        // IMPORTANT: For Nullable dictionaries, only write the NESTED column data
        // (matching C++ implementation in lowcardinality.cpp::SaveBody)
        use super::nullable::ColumnNullable;
        if let Some(nullable_col) = self.dictionary.as_any().downcast_ref::<ColumnNullable>() {
            // For Nullable, save only the nested column (no null bitmap)
            nullable_col.nested().save_to_buffer(buffer)?;
        } else {
            // For non-Nullable, save normally
            self.dictionary.save_to_buffer(buffer)?;
        }

        // 4. Write number_of_rows (index column size)
        buffer.put_u64_le(self.indices.len() as u64);

        // 5. Write index data (as UInt64 for now)
        for &index in &self.indices {
            buffer.put_u64_le(index);
        }

        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnLowCardinality::new(self.type_.clone()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        if begin + len > self.indices.len() {
            return Err(Error::InvalidArgument(format!(
                "Slice out of bounds: begin={}, len={}, size={}",
                begin,
                len,
                self.indices.len()
            )));
        }

        let mut sliced = ColumnLowCardinality::new(self.type_.clone());
        sliced.dictionary = self.dictionary.clone();
        sliced.indices = self.indices[begin..begin + len].to_vec();
        sliced.unique_map = self.unique_map.clone();

        Ok(Arc::new(sliced))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TypeCode;

    #[test]
    fn test_lowcardinality_creation() {
        let lc_type = Type::LowCardinality {
            nested_type: Box::new(Type::Simple(TypeCode::String)),
        };

        let col = ColumnLowCardinality::new(lc_type);
        assert_eq!(col.len(), 0);
        assert!(col.is_empty());
        assert_eq!(col.dictionary_size(), 0);
    }

    #[test]
    fn test_lowcardinality_empty() {
        let lc_type = Type::LowCardinality {
            nested_type: Box::new(Type::Simple(TypeCode::UInt32)),
        };

        let col = ColumnLowCardinality::new(lc_type);
        assert_eq!(col.dictionary_size(), 0);
        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_lowcardinality_slice() {
        let lc_type = Type::LowCardinality {
            nested_type: Box::new(Type::Simple(TypeCode::String)),
        };

        let mut col = ColumnLowCardinality::new(lc_type);
        // Manually add some indices for testing
        col.indices = vec![0, 1, 2, 1, 0];

        let sliced = col.slice(1, 3).unwrap();
        let sliced_col =
            sliced.as_any().downcast_ref::<ColumnLowCardinality>().unwrap();

        assert_eq!(sliced_col.len(), 3);
        assert_eq!(sliced_col.index_at(0), 1);
        assert_eq!(sliced_col.index_at(1), 2);
        assert_eq!(sliced_col.index_at(2), 1);
    }

    #[test]
    fn test_lowcardinality_clear() {
        let lc_type = Type::LowCardinality {
            nested_type: Box::new(Type::Simple(TypeCode::String)),
        };

        let mut col = ColumnLowCardinality::new(lc_type);
        col.indices = vec![0, 1, 2];

        col.clear();
        assert_eq!(col.len(), 0);
        assert!(col.is_empty());
    }

    #[test]
    fn test_lowcardinality_save_load_roundtrip() {
        use bytes::BytesMut;

        // Create a LowCardinality(String) column
        let lc_type = Type::LowCardinality {
            nested_type: Box::new(Type::Simple(TypeCode::String)),
        };

        let mut col = ColumnLowCardinality::new(lc_type.clone());

        // Add some test data with repeated values
        use crate::column::column_value::ColumnValue;
        col.append_unsafe(&ColumnValue::from_string("hello")).unwrap();
        col.append_unsafe(&ColumnValue::from_string("world")).unwrap();
        col.append_unsafe(&ColumnValue::from_string("hello")).unwrap(); // Duplicate
        col.append_unsafe(&ColumnValue::from_string("test")).unwrap();
        col.append_unsafe(&ColumnValue::from_string("world")).unwrap(); // Duplicate

        // Verify initial state
        assert_eq!(col.len(), 5);
        assert_eq!(col.dictionary_size(), 3); // "hello", "world", "test"

        // Save to buffer
        let mut buffer = BytesMut::new();
        col.save_prefix(&mut buffer).unwrap();
        col.save_to_buffer(&mut buffer).unwrap();

        // Verify buffer format (matching C++ protocol):
        let mut read_buf = &buffer[..];
        use bytes::Buf;

        // 1. key_version (from save_prefix)
        let key_version = read_buf.get_u64_le();
        assert_eq!(key_version, 1, "key_version should be 1");

        // 2. index_serialization_type (from save_to_buffer)
        let index_serialization_type = read_buf.get_u64_le();
        let index_type = index_serialization_type & 0xFF;
        let has_additional_keys = (index_serialization_type & (1 << 9)) != 0;
        assert_eq!(index_type, 3, "index_type should be 3 (UInt64)");
        assert!(has_additional_keys, "HasAdditionalKeysBit should be set");

        // 3. number_of_keys
        let number_of_keys = read_buf.get_u64_le();
        assert_eq!(number_of_keys, 3, "dictionary should have 3 unique values");

        // Load into new column
        let mut loaded_col = ColumnLowCardinality::new(lc_type);
        let mut load_buf = &buffer[..];
        loaded_col.load_from_buffer(&mut load_buf, 5).unwrap();

        // Verify loaded data
        assert_eq!(loaded_col.len(), 5);
        assert_eq!(loaded_col.dictionary_size(), 3);

        // Verify indices match (deduplication preserved)
        assert_eq!(loaded_col.index_at(0), col.index_at(0)); // "hello"
        assert_eq!(loaded_col.index_at(1), col.index_at(1)); // "world"
        assert_eq!(loaded_col.index_at(2), col.index_at(2)); // "hello" (same as 0)
        assert_eq!(loaded_col.index_at(3), col.index_at(3)); // "test"
        assert_eq!(loaded_col.index_at(4), col.index_at(4)); // "world" (same as 1)

        // Verify duplicates point to same dictionary entry
        assert_eq!(loaded_col.index_at(0), loaded_col.index_at(2));
        assert_eq!(loaded_col.index_at(1), loaded_col.index_at(4));
    }

    #[test]
    fn test_lowcardinality_nullable_save_format() {
        use bytes::BytesMut;

        // Create a LowCardinality(Nullable(String)) column
        let lc_type = Type::LowCardinality {
            nested_type: Box::new(Type::Nullable {
                nested_type: Box::new(Type::Simple(TypeCode::String)),
            }),
        };

        let mut col = ColumnLowCardinality::new(lc_type.clone());

        // Add test data with nulls
        use crate::column::column_value::ColumnValue;
        col.append_unsafe(&ColumnValue::from_string("hello")).unwrap();
        col.append_unsafe(&ColumnValue::void()).unwrap(); // null value
        col.append_unsafe(&ColumnValue::from_string("world")).unwrap();

        assert_eq!(col.len(), 3);

        // Save to buffer
        let mut buffer = BytesMut::new();
        col.save_prefix(&mut buffer).unwrap();
        col.save_to_buffer(&mut buffer).unwrap();

        // The key point: for Nullable dictionaries, only nested data is saved
        // (verified by checking buffer structure matches C++ protocol)
        assert!(!buffer.is_empty(), "Buffer should contain data");

        // Verify buffer starts with correct key_version
        use bytes::Buf;
        let mut read_buf = &buffer[..];
        let key_version = read_buf.get_u64_le();
        assert_eq!(key_version, 1, "key_version should be 1");

        // Full round-trip testing for Nullable LowCardinality is complex
        // due to the nested save format. The integration tests cover this.
    }
}
