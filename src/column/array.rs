//! Array column implementation
//!
//! **ClickHouse Documentation:** <https://clickhouse.com/docs/en/sql-reference/data-types/array>
//!
//! ## Overview
//!
//! Array columns store variable-length arrays of elements. All elements are
//! stored in a single nested column (flattened), with offsets tracking where
//! each array begins/ends.
//!
//! ## Important Restriction
//!
//! **Arrays cannot be wrapped in Nullable:**
//! - ❌ `Nullable(Array(String))` - Error: "Nested type Array(String) cannot
//!   be inside Nullable type" (Error code 43)
//! - ✅ `Array(Nullable(String))` - CORRECT: Each element can be NULL
//!
//! If you need to represent "no array", use an empty array `[]` instead of
//! NULL.
//!
//! See: <https://github.com/ClickHouse/ClickHouse/issues/1062>
//!
//! ## Wire Format
//!
//! ```text
//! [offsets: UInt64 * num_arrays]  // Cumulative element counts
//! [nested_column_data]             // All elements concatenated
//! ```
//!
//! Example: `[[1,2], [3], [4,5,6]]`
//! - Offsets: `[2, 3, 6]` (2 elements in first array, 3 total after second, 6
//!   total after third)
//! - Nested data: `[1, 2, 3, 4, 5, 6]`

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
    BytesMut,
};
use std::sync::Arc;

/// Column for arrays of variable length
///
/// Stores a nested column with all array elements concatenated,
/// and an offsets array that marks where each array ends.
///
/// **Reference Implementation:** See
/// `clickhouse-cpp/clickhouse/columns/array.cpp`
pub struct ColumnArray {
    type_: Type,
    nested: ColumnRef,
    offsets: Vec<u64>, /* Cumulative offsets: offsets[i] = total elements
                        * up to and including array i */
}

impl ColumnArray {
    /// Create a new array column from an array type
    pub fn new(type_: Type) -> Self {
        // Extract nested type and create nested column
        let nested = match &type_ {
            Type::Array { item_type } => {
                crate::io::block_stream::create_column(item_type)
                    .expect("Failed to create nested column")
            }
            _ => panic!("ColumnArray requires Array type"),
        };

        Self { type_, nested, offsets: Vec::new() }
    }

    /// Create a new array column with an existing nested column
    pub fn with_nested(nested: ColumnRef) -> Self {
        let nested_type = nested.column_type().clone();
        Self { type_: Type::array(nested_type), nested, offsets: Vec::new() }
    }

    /// Create a new array column from parts (for geo types that need custom
    /// type names)
    pub(crate) fn from_parts(type_: Type, nested: ColumnRef) -> Self {
        Self { type_, nested, offsets: Vec::new() }
    }

    /// Create with reserved capacity
    pub fn with_capacity(type_: Type, capacity: usize) -> Self {
        let nested = match &type_ {
            Type::Array { item_type } => {
                crate::io::block_stream::create_column(item_type)
                    .expect("Failed to create nested column")
            }
            _ => panic!("ColumnArray requires Array type"),
        };

        Self { type_, nested, offsets: Vec::with_capacity(capacity) }
    }

    /// Append an array (specified by the number of elements in the nested
    /// column to consume) The caller must ensure that `len` elements have
    /// been added to the nested column
    pub fn append_len(&mut self, len: u64) {
        let new_offset = if self.offsets.is_empty() {
            len
        } else {
            self.offsets.last().unwrap() + len
        };
        self.offsets.push(new_offset);
    }

    /// Get the start and end indices for the array at the given index
    pub fn get_array_range(&self, index: usize) -> Option<(usize, usize)> {
        if index >= self.offsets.len() {
            return None;
        }

        let end = self.offsets[index] as usize;
        let start =
            if index == 0 { 0 } else { self.offsets[index - 1] as usize };

        Some((start, end))
    }

    /// Get the length of the array at the given index
    pub fn get_array_len(&self, index: usize) -> Option<usize> {
        self.get_array_range(index).map(|(start, end)| end - start)
    }

    /// Get the nested column
    pub fn nested(&self) -> &ColumnRef {
        &self.nested
    }

    /// Get mutable access to the nested column
    pub fn nested_mut(&mut self) -> &mut ColumnRef {
        &mut self.nested
    }

    /// Get the offsets
    pub fn offsets(&self) -> &[u64] {
        &self.offsets
    }

    /// Append an entire array column as a single array element
    /// This takes all the data from the provided column and adds it as one
    /// array
    pub fn append_array(&mut self, array_data: ColumnRef) {
        let len = array_data.size() as u64;

        // Append the array data to nested column
        let nested_mut = Arc::get_mut(&mut self.nested)
            .expect("Cannot append to shared array column - column has multiple references");
        nested_mut
            .append_column(array_data)
            .expect("Failed to append array data to nested column");

        // Update offsets
        self.append_len(len);
    }

    /// Get the array at the given index as a sliced column
    pub fn at(&self, index: usize) -> ColumnRef {
        if let Some((start, end)) = self.get_array_range(index) {
            self.nested.slice(start, end - start).expect("Valid slice")
        } else {
            panic!("Array index out of bounds: {}", index);
        }
    }

    /// Get the number of arrays (alias for size())
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Check if the array column is empty
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
}

impl Column for ColumnArray {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.offsets.len()
    }

    fn clear(&mut self) {
        self.offsets.clear();
        // CRITICAL: Must also clear nested data to maintain consistency
        // If we clear offsets but not nested data, the column is in a corrupt
        // state
        let nested_mut = Arc::get_mut(&mut self.nested)
            .expect("Cannot clear shared array column - column has multiple references");
        nested_mut.clear();
    }

    fn reserve(&mut self, new_cap: usize) {
        self.offsets.reserve(new_cap);
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let other =
            other.as_any().downcast_ref::<ColumnArray>().ok_or_else(|| {
                Error::TypeMismatch {
                    expected: self.type_.name(),
                    actual: other.column_type().name(),
                }
            })?;

        // Check that nested types match
        if self.nested.column_type().name()
            != other.nested.column_type().name()
        {
            return Err(Error::TypeMismatch {
                expected: self.nested.column_type().name(),
                actual: other.nested.column_type().name(),
            });
        }

        // Adjust offsets from other and append
        let offset_base = self.offsets.last().copied().unwrap_or(0);
        for &offset in &other.offsets {
            self.offsets.push(offset_base + offset);
        }

        // CRITICAL: Must also append the nested data!
        // Without this, offsets point to wrong/missing data → DATA CORRUPTION
        let nested_mut = Arc::get_mut(&mut self.nested)
            .ok_or_else(|| Error::Protocol(
                "Cannot append to shared array column - column has multiple references".to_string()
            ))?;
        nested_mut.append_column(other.nested.clone())?;

        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        self.offsets.reserve(rows);

        // Read offsets (fixed UInt64, not varint!)
        // Wire format: UInt64 values stored as 8-byte little-endian
        let bytes_needed = rows * 8;
        if buffer.len() < bytes_needed {
            return Err(Error::Protocol(format!(
                "Buffer underflow reading array offsets: need {} bytes, have {}",
                bytes_needed,
                buffer.len()
            )));
        }

        // Use bulk copy for performance
        self.offsets.reserve(rows);
        let current_len = self.offsets.len();
        unsafe {
            // Set length first to claim ownership of the memory
            self.offsets.set_len(current_len + rows);
            // Cast dest to bytes and use byte offset
            let dest_ptr =
                (self.offsets.as_mut_ptr() as *mut u8).add(current_len * 8);
            std::ptr::copy_nonoverlapping(
                buffer.as_ptr(),
                dest_ptr,
                bytes_needed,
            );
        }

        buffer.advance(bytes_needed);

        // CRITICAL: Must also load the nested column data
        // The total number of nested elements is the last offset value
        let total_nested_elements =
            self.offsets.last().copied().unwrap_or(0) as usize;
        if total_nested_elements > 0 {
            let nested_mut = Arc::get_mut(&mut self.nested)
                .ok_or_else(|| Error::Protocol(
                    "Cannot load into shared array column - column has multiple references".to_string()
                ))?;
            nested_mut.load_from_buffer(buffer, total_nested_elements)?;
        }

        Ok(())
    }

    fn load_prefix(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
        // Delegate to nested column's load_prefix
        // Critical for Array(LowCardinality(X)) to read LowCardinality
        // key_version before offsets
        let nested_mut = Arc::get_mut(&mut self.nested).ok_or_else(|| {
            Error::Protocol(
                "Cannot load prefix for shared array column".to_string(),
            )
        })?;
        nested_mut.load_prefix(buffer, rows)
    }

    fn save_prefix(&self, buffer: &mut BytesMut) -> Result<()> {
        // Delegate to nested column's save_prefix
        // Critical for Array(LowCardinality(X)) to write LowCardinality
        // version before offsets
        self.nested.save_prefix(buffer)
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Write offsets as fixed UInt64 (not varints!)
        // Wire format: UInt64 values stored as 8-byte little-endian
        // This matches load_from_buffer which reads fixed UInt64
        if !self.offsets.is_empty() {
            let byte_slice = unsafe {
                std::slice::from_raw_parts(
                    self.offsets.as_ptr() as *const u8,
                    self.offsets.len() * 8,
                )
            };
            buffer.extend_from_slice(byte_slice);
        }

        // Write nested column data
        self.nested.save_to_buffer(buffer)?;

        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnArray::with_nested(self.nested.clone_empty()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        if begin + len > self.offsets.len() {
            return Err(Error::InvalidArgument(format!(
                "Slice out of bounds: begin={}, len={}, size={}",
                begin,
                len,
                self.offsets.len()
            )));
        }

        // Calculate the range of nested elements we need
        let nested_start =
            if begin == 0 { 0 } else { self.offsets[begin - 1] as usize };
        let nested_end = self.offsets[begin + len - 1] as usize;
        let nested_len = nested_end - nested_start;

        // Slice the nested column
        let sliced_nested = self.nested.slice(nested_start, nested_len)?;

        // Adjust offsets for the slice
        let mut sliced_offsets = Vec::with_capacity(len);
        let offset_base = if begin == 0 { 0 } else { self.offsets[begin - 1] };

        for i in begin..begin + len {
            sliced_offsets.push(self.offsets[i] - offset_base);
        }

        let mut result = ColumnArray::with_nested(sliced_nested);
        result.offsets = sliced_offsets;

        Ok(Arc::new(result))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// Helper functions removed - using buffer_utils module

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        column::{
            numeric::ColumnUInt64,
            string::ColumnString,
        },
        types::Type,
    };

    #[test]
    fn test_array_creation() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let col = ColumnArray::with_nested(nested);
        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_array_append() {
        let mut nested = ColumnUInt64::new(Type::uint64());
        // First array: [1, 2, 3]
        nested.append(1);
        nested.append(2);
        nested.append(3);

        let mut col = ColumnArray::with_nested(Arc::new(nested));
        col.append_len(3); // Array of 3 elements

        // Second array: [4, 5]
        let nested_mut = Arc::get_mut(&mut col.nested)
            .unwrap()
            .as_any_mut()
            .downcast_mut::<ColumnUInt64>()
            .unwrap();
        nested_mut.append(4);
        nested_mut.append(5);

        col.append_len(2); // Array of 2 more elements

        assert_eq!(col.size(), 2);
        assert_eq!(col.get_array_len(0), Some(3));
        assert_eq!(col.get_array_len(1), Some(2));
        assert_eq!(col.get_array_range(0), Some((0, 3)));
        assert_eq!(col.get_array_range(1), Some((3, 5)));
    }

    #[test]
    fn test_array_offsets() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnArray::with_nested(nested);

        col.append_len(3); // Array with 3 elements
        col.append_len(0); // Empty array
        col.append_len(2); // Array with 2 elements

        assert_eq!(col.offsets(), &[3, 3, 5]);
        assert_eq!(col.get_array_len(0), Some(3));
        assert_eq!(col.get_array_len(1), Some(0));
        assert_eq!(col.get_array_len(2), Some(2));
    }

    #[test]
    fn test_array_empty_arrays() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnArray::with_nested(nested);

        col.append_len(0);
        col.append_len(0);
        col.append_len(0);

        assert_eq!(col.size(), 3);
        assert_eq!(col.get_array_len(0), Some(0));
        assert_eq!(col.get_array_len(1), Some(0));
        assert_eq!(col.get_array_len(2), Some(0));
    }

    #[test]
    fn test_array_save_load() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnArray::with_nested(nested);

        col.append_len(3);
        col.append_len(2);
        col.append_len(1);

        let mut buffer = BytesMut::new();
        col.save_to_buffer(&mut buffer).unwrap();

        // Verify offsets are written
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_array_load_offsets() {
        use bytes::BufMut;

        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnArray::with_nested(nested);

        // Encode offsets manually as fixed UInt64: 3, 5, 8 (total 8 nested
        // elements)
        let mut data = BytesMut::new();
        data.put_u64_le(3);
        data.put_u64_le(5);
        data.put_u64_le(8);

        // Must also include nested data (8 UInt64 values)
        for i in 0..8u64 {
            data.put_u64_le(i);
        }

        let mut reader = &data[..];
        col.load_from_buffer(&mut reader, 3).unwrap();

        assert_eq!(col.size(), 3);
        assert_eq!(col.offsets(), &[3, 5, 8]);
    }

    #[test]
    fn test_array_slice() {
        let mut nested = ColumnUInt64::new(Type::uint64());
        // Arrays: [1,2,3], [4,5], [6], [7,8,9,10]
        for i in 1..=10 {
            nested.append(i);
        }

        let mut col = ColumnArray::with_nested(Arc::new(nested));
        col.append_len(3); // offset: 3
        col.append_len(2); // offset: 5
        col.append_len(1); // offset: 6
        col.append_len(4); // offset: 10

        let sliced = col.slice(1, 2).unwrap(); // Take arrays [4,5] and [6]
        let sliced_col =
            sliced.as_any().downcast_ref::<ColumnArray>().unwrap();

        assert_eq!(sliced_col.size(), 2);
        assert_eq!(sliced_col.offsets(), &[2, 3]); // Adjusted offsets
    }

    #[test]
    fn test_array_with_strings() {
        let nested = Arc::new(ColumnString::new(Type::string()));
        let mut col = ColumnArray::with_nested(nested);

        col.append_len(2); // Array with 2 strings
        col.append_len(3); // Array with 3 strings

        assert_eq!(col.size(), 2);
        assert_eq!(col.get_array_len(0), Some(2));
        assert_eq!(col.get_array_len(1), Some(3));
    }

    #[test]
    fn test_array_type_mismatch() {
        let nested1 = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col1 = ColumnArray::with_nested(nested1);

        let nested2 = Arc::new(ColumnString::new(Type::string()));
        let col2 = ColumnArray::with_nested(nested2);

        let result = col1.append_column(Arc::new(col2));
        assert!(result.is_err());
    }

    #[test]
    fn test_array_out_of_bounds() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnArray::with_nested(nested);

        col.append_len(3);
        col.append_len(2);

        assert_eq!(col.get_array_len(100), None);
        assert_eq!(col.get_array_range(100), None);
    }

    #[test]
    fn test_array_append_column() {
        // Create first array column with data: [[1, 2], [3]]
        let mut nested1 = ColumnUInt64::new(Type::uint64());
        nested1.append(1);
        nested1.append(2);
        nested1.append(3);

        let mut col1 = ColumnArray::with_nested(Arc::new(nested1));
        col1.append_len(2); // First array: [1, 2]
        col1.append_len(1); // Second array: [3]

        // Create second array column with data: [[4, 5, 6]]
        let mut nested2 = ColumnUInt64::new(Type::uint64());
        nested2.append(4);
        nested2.append(5);
        nested2.append(6);

        let mut col2 = ColumnArray::with_nested(Arc::new(nested2));
        col2.append_len(3); // Third array: [4, 5, 6]

        // Append col2 to col1
        col1.append_column(Arc::new(col2))
            .expect("append_column should succeed");

        // Verify we have 3 arrays total
        assert_eq!(col1.size(), 3, "Should have 3 arrays after append");

        // Verify array lengths
        assert_eq!(
            col1.get_array_len(0),
            Some(2),
            "First array should have 2 elements"
        );
        assert_eq!(
            col1.get_array_len(1),
            Some(1),
            "Second array should have 1 element"
        );
        assert_eq!(
            col1.get_array_len(2),
            Some(3),
            "Third array should have 3 elements"
        );

        // CRITICAL: Verify nested data was actually appended
        // The nested column should contain [1, 2, 3, 4, 5, 6]
        let nested =
            col1.nested.as_any().downcast_ref::<ColumnUInt64>().unwrap();
        assert_eq!(
            nested.size(),
            6,
            "Nested column should have 6 total elements"
        );

        // Verify offsets are correct: [2, 3, 6]
        assert_eq!(col1.offsets(), &[2, 3, 6], "Offsets should be [2, 3, 6]");
    }

    #[test]
    #[should_panic(
        expected = "Cannot clear shared array column - column has multiple references"
    )]
    fn test_array_clear_panics_on_shared_nested() {
        // Create an array column
        let mut nested = ColumnUInt64::new(Type::uint64());
        nested.append(1);
        nested.append(2);
        nested.append(3);

        let nested_arc = Arc::new(nested);
        let mut col = ColumnArray::with_nested(nested_arc.clone());
        col.append_len(3);

        // Create a second reference to the nested column
        let _shared_ref = nested_arc.clone();

        // Now nested has multiple Arc references, so clear() MUST panic
        // to prevent data corruption (clearing offsets but not nested data)
        col.clear();
    }

    #[test]
    fn test_array_roundtrip_nested_data() {
        use bytes::BytesMut;

        // Create array column with actual nested data: [[1, 2], [3, 4, 5]]
        let mut nested = ColumnUInt64::new(Type::uint64());
        nested.append(1);
        nested.append(2);
        nested.append(3);
        nested.append(4);
        nested.append(5);

        let mut col = ColumnArray::with_nested(Arc::new(nested));
        col.append_len(2); // First array: [1, 2]
        col.append_len(3); // Second array: [3, 4, 5]

        assert_eq!(col.size(), 2, "Original should have 2 arrays");

        // Save to buffer
        let mut buffer = BytesMut::new();
        col.save_to_buffer(&mut buffer).expect("save should succeed");

        // Load into new array column
        let nested_empty = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col_loaded = ColumnArray::with_nested(nested_empty);

        let mut buf_slice = &buffer[..];
        col_loaded
            .load_from_buffer(&mut buf_slice, 2)
            .expect("load should succeed");

        // Verify arrays structure
        assert_eq!(col_loaded.size(), 2, "Loaded should have 2 arrays");
        assert_eq!(
            col_loaded.get_array_len(0),
            Some(2),
            "First array should have 2 elements"
        );
        assert_eq!(
            col_loaded.get_array_len(1),
            Some(3),
            "Second array should have 3 elements"
        );

        // CRITICAL: Verify nested data was actually loaded
        let nested_loaded =
            col_loaded.nested.as_any().downcast_ref::<ColumnUInt64>().unwrap();
        assert_eq!(
            nested_loaded.size(),
            5,
            "Nested should have 5 total elements after load"
        );

        // Verify we can retrieve the actual arrays
        let arr0 = col_loaded.at(0);
        let arr0_data = arr0.as_any().downcast_ref::<ColumnUInt64>().unwrap();
        assert_eq!(arr0_data.size(), 2, "First array should have 2 elements");

        let arr1 = col_loaded.at(1);
        let arr1_data = arr1.as_any().downcast_ref::<ColumnUInt64>().unwrap();
        assert_eq!(arr1_data.size(), 3, "Second array should have 3 elements");
    }
}
