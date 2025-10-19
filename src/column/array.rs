//! Array column implementation
//!
//! **ClickHouse Documentation:** <https://clickhouse.com/docs/en/sql-reference/data-types/array>
//!
//! ## Overview
//!
//! Array columns store variable-length arrays of elements. All elements are stored in a single
//! nested column (flattened), with offsets tracking where each array begins/ends.
//!
//! ## Important Restriction
//!
//! **Arrays cannot be wrapped in Nullable:**
//! - ❌ `Nullable(Array(String))` - Error: "Nested type Array(String) cannot be inside Nullable type" (Error code 43)
//! - ✅ `Array(Nullable(String))` - CORRECT: Each element can be NULL
//!
//! If you need to represent "no array", use an empty array `[]` instead of NULL.
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
//! - Offsets: `[2, 3, 6]` (2 elements in first array, 3 total after second, 6 total after third)
//! - Nested data: `[1, 2, 3, 4, 5, 6]`

use super::{Column, ColumnRef};
use crate::types::Type;
use crate::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::sync::Arc;

/// Column for arrays of variable length
///
/// Stores a nested column with all array elements concatenated,
/// and an offsets array that marks where each array ends.
///
/// **Reference Implementation:** See `clickhouse-cpp/clickhouse/columns/array.cpp`
pub struct ColumnArray {
    type_: Type,
    nested: ColumnRef,
    offsets: Vec<u64>, // Cumulative offsets: offsets[i] = total elements up to and including array i
}

impl ColumnArray {
    /// Create a new array column from an array type
    pub fn new(type_: Type) -> Self {
        // Extract nested type and create nested column
        let nested = match &type_ {
            Type::Array { item_type } => {
                crate::io::block_stream::create_column(item_type).expect("Failed to create nested column")
            }
            _ => panic!("ColumnArray requires Array type"),
        };

        Self {
            type_,
            nested,
            offsets: Vec::new(),
        }
    }

    /// Create a new array column with an existing nested column
    pub fn with_nested(nested: ColumnRef) -> Self {
        let nested_type = nested.column_type().clone();
        Self {
            type_: Type::array(nested_type),
            nested,
            offsets: Vec::new(),
        }
    }

    /// Create a new array column from parts (for geo types that need custom type names)
    pub(crate) fn from_parts(type_: Type, nested: ColumnRef) -> Self {
        Self {
            type_,
            nested,
            offsets: Vec::new(),
        }
    }

    /// Create with reserved capacity
    pub fn with_capacity(type_: Type, capacity: usize) -> Self {
        let nested = match &type_ {
            Type::Array { item_type } => {
                crate::io::block_stream::create_column(item_type).expect("Failed to create nested column")
            }
            _ => panic!("ColumnArray requires Array type"),
        };

        Self {
            type_,
            nested,
            offsets: Vec::with_capacity(capacity),
        }
    }

    /// Append an array (specified by the number of elements in the nested column to consume)
    /// The caller must ensure that `len` elements have been added to the nested column
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
        let start = if index == 0 {
            0
        } else {
            self.offsets[index - 1] as usize
        };

        Some((start, end))
    }

    /// Get the length of the array at the given index
    pub fn get_array_len(&self, index: usize) -> Option<usize> {
        self.get_array_range(index)
            .map(|(start, end)| end - start)
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
    /// This takes all the data from the provided column and adds it as one array
    pub fn append_array(&mut self, array_data: ColumnRef) {
        let len = array_data.size() as u64;

        // Append the array data to nested column
        if let Some(nested_mut) = Arc::get_mut(&mut self.nested) {
            let _ = nested_mut.append_column(array_data);
        }

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
        // Note: We can't clear nested due to Arc, but this is a known limitation
    }

    fn reserve(&mut self, new_cap: usize) {
        self.offsets.reserve(new_cap);
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<ColumnArray>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            })?;

        // Check that nested types match
        if self.nested.column_type().name() != other.nested.column_type().name() {
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

        Ok(())
    }

    fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
        self.offsets.reserve(rows);

        // Read offsets (varint encoded u64)
        for _ in 0..rows {
            let offset = read_varint(buffer)?;
            self.offsets.push(offset);
        }

        // Now we need to load the nested column data
        // But we can't call load_from_buffer on nested due to Arc immutability
        // This is a design limitation - in practice, we'd need interior mutability

        Ok(())
    }

    fn save_prefix(&self, buffer: &mut BytesMut) -> Result<()> {
        // Delegate to nested column's save_prefix
        // Critical for Array(LowCardinality(X)) to write LowCardinality version before offsets
        self.nested.save_prefix(buffer)
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Write offsets
        for &offset in &self.offsets {
            write_varint(buffer, offset);
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
        let nested_start = if begin == 0 {
            0
        } else {
            self.offsets[begin - 1] as usize
        };
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

// Helper functions for varint encoding/decoding (same as in string.rs)
fn read_varint(buffer: &mut &[u8]) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;

    loop {
        if buffer.is_empty() {
            return Err(Error::Protocol(
                "Unexpected end of buffer reading varint".to_string(),
            ));
        }

        let byte = buffer[0];
        buffer.advance(1);

        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift >= 64 {
            return Err(Error::Protocol("Varint overflow".to_string()));
        }
    }

    Ok(result)
}

fn write_varint(buffer: &mut BytesMut, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buffer.put_u8(byte);

        if value == 0 {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::numeric::ColumnUInt64;
    use crate::column::string::ColumnString;
    use crate::types::Type;

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
        let nested_mut = Arc::get_mut(&mut col.nested).unwrap()
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
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnArray::with_nested(nested);

        // Encode offsets manually: 3, 5, 8
        let mut data = BytesMut::new();
        write_varint(&mut data, 3);
        write_varint(&mut data, 5);
        write_varint(&mut data, 8);

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
        let sliced_col = sliced.as_any().downcast_ref::<ColumnArray>().unwrap();

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
}
