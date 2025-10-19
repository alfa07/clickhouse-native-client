//! Nullable column implementation
//!
//! **ClickHouse Documentation:** <https://clickhouse.com/docs/en/sql-reference/data-types/nullable>
//!
//! ## Important Nesting Restrictions
//!
//! ClickHouse does NOT allow wrapping certain types in Nullable:
//! - ❌ `Nullable(Array(...))` - NOT allowed (Error code 43)
//! - ❌ `Nullable(LowCardinality(...))` - NOT allowed
//!
//! **Correct usage:**
//! - ✅ `Array(Nullable(...))` - Nullable elements inside array
//! - ✅ `LowCardinality(Nullable(...))` - Nullable values with dictionary
//!   encoding
//!
//! See: <https://github.com/ClickHouse/ClickHouse/issues/1062>

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
use std::sync::Arc;

/// Column for nullable values
///
/// Stores a nested column and a bitmap of null flags (1 = null, 0 = not null).
///
/// **Wire Format:**
/// ```text
/// [null_bitmap: UInt8 * num_rows][nested_column_data]
/// ```
///
/// **ClickHouse Reference:**
/// - Documentation: <https://clickhouse.com/docs/en/sql-reference/data-types/nullable>
/// - Best Practices: <https://clickhouse.com/docs/en/cloud/bestpractices/avoid-nullable-columns>
pub struct ColumnNullable {
    type_: Type,
    nested: ColumnRef,
    nulls: Vec<u8>, // Bitmap: 1 = null, 0 = not null
}

impl ColumnNullable {
    /// Create a new nullable column from a nullable type
    pub fn new(type_: Type) -> Self {
        // Extract nested type and create nested column
        let nested = match &type_ {
            Type::Nullable { nested_type } => {
                crate::io::block_stream::create_column(nested_type)
                    .expect("Failed to create nested column")
            }
            _ => panic!("ColumnNullable requires Nullable type"),
        };

        Self { type_, nested, nulls: Vec::new() }
    }

    /// Create a new nullable column wrapping an existing nested column
    pub fn with_nested(nested: ColumnRef) -> Self {
        let nested_type = nested.column_type().clone();
        Self { type_: Type::nullable(nested_type), nested, nulls: Vec::new() }
    }

    /// Create with reserved capacity
    pub fn with_capacity(type_: Type, capacity: usize) -> Self {
        let nested = match &type_ {
            Type::Nullable { nested_type } => {
                crate::io::block_stream::create_column(nested_type)
                    .expect("Failed to create nested column")
            }
            _ => panic!("ColumnNullable requires Nullable type"),
        };

        Self { type_, nested, nulls: Vec::with_capacity(capacity) }
    }

    /// Append a null value
    pub fn append_null(&mut self) {
        self.nulls.push(1);
    }

    /// Append a non-null value (the nested column should be updated
    /// separately)
    pub fn append_non_null(&mut self) {
        self.nulls.push(0);
    }

    /// Check if value at index is null
    pub fn is_null(&self, index: usize) -> bool {
        index < self.nulls.len() && self.nulls[index] != 0
    }

    /// Get the nested column
    pub fn nested(&self) -> &ColumnRef {
        &self.nested
    }

    /// Get mutable access to the nested column
    pub fn nested_mut(&mut self) -> &mut ColumnRef {
        &mut self.nested
    }

    /// Get the nulls bitmap
    pub fn nulls(&self) -> &[u8] {
        &self.nulls
    }

    /// Append a nullable UInt32 value (convenience method for tests)
    pub fn append_nullable(&mut self, value: Option<u32>) {
        use crate::column::numeric::ColumnUInt32;

        match value {
            None => {
                self.append_null();
                // Still need to add a placeholder to nested column to keep
                // indices aligned
                let nested_mut = Arc::get_mut(&mut self.nested).expect(
                    "Cannot append to shared nullable column - column has multiple references",
                );
                let col = nested_mut
                    .as_any_mut()
                    .downcast_mut::<ColumnUInt32>()
                    .expect("Nullable nested column is not UInt32");
                col.append(0); // Placeholder value (ignored due to null flag)
            }
            Some(val) => {
                self.append_non_null();
                let nested_mut = Arc::get_mut(&mut self.nested).expect(
                    "Cannot append to shared nullable column - column has multiple references",
                );
                let col = nested_mut
                    .as_any_mut()
                    .downcast_mut::<ColumnUInt32>()
                    .expect("Nullable nested column is not UInt32");
                col.append(val);
            }
        }
    }

    /// Check if value at index is null (alias for is_null)
    pub fn is_null_at(&self, index: usize) -> bool {
        self.is_null(index)
    }

    /// Get a reference to the value at the given index
    /// Returns the nested column for accessing the value (check is_null
    /// first!)
    pub fn at(&self, _index: usize) -> ColumnRef {
        self.nested.clone()
    }

    /// Get the number of elements (alias for size())
    pub fn len(&self) -> usize {
        self.nulls.len()
    }

    /// Check if the nullable column is empty
    pub fn is_empty(&self) -> bool {
        self.nulls.is_empty()
    }
}

impl Column for ColumnNullable {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.nulls.len()
    }

    fn clear(&mut self) {
        self.nulls.clear();
        // CRITICAL: Must also clear nested data to maintain consistency
        // If we clear null bitmap but not nested data, the column is in a
        // corrupt state
        let nested_mut = Arc::get_mut(&mut self.nested)
            .expect("Cannot clear shared nullable column - column has multiple references");
        nested_mut.clear();
    }

    fn reserve(&mut self, new_cap: usize) {
        self.nulls.reserve(new_cap);
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<ColumnNullable>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
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

        // Append null bitmap
        self.nulls.extend_from_slice(&other.nulls);

        // CRITICAL: Must also append the nested data!
        // Without this, null flags are correct but values are missing → DATA
        // LOSS
        let nested_mut = Arc::get_mut(&mut self.nested).ok_or_else(|| {
            Error::Protocol(
                "Cannot append to shared nullable column - column has multiple references"
                    .to_string(),
            )
        })?;
        nested_mut.append_column(other.nested.clone())?;

        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        // Read null bitmap (one byte per row)
        if buffer.len() < rows {
            return Err(Error::Protocol(format!(
                "Not enough data for null bitmap: need {}, have {}",
                rows,
                buffer.len()
            )));
        }

        self.nulls.extend_from_slice(&buffer[..rows]);
        buffer.advance(rows);

        // CRITICAL: Must also load the nested column data
        // The nested column has exactly `rows` elements (including
        // placeholders for nulls)
        if rows > 0 {
            let nested_mut = Arc::get_mut(&mut self.nested).ok_or_else(|| {
                Error::Protocol(
                    "Cannot load into shared nullable column - column has multiple references"
                        .to_string(),
                )
            })?;
            nested_mut.load_from_buffer(buffer, rows)?;
        }

        Ok(())
    }

    fn save_prefix(&self, buffer: &mut BytesMut) -> Result<()> {
        // Delegate to nested column's save_prefix
        // This is critical for nested types like LowCardinality that write
        // version info
        self.nested.save_prefix(buffer)
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Write null bitmap
        buffer.put_slice(&self.nulls);

        // Write nested column data
        self.nested.save_to_buffer(buffer)?;

        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnNullable::with_nested(self.nested.clone_empty()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        if begin + len > self.nulls.len() {
            return Err(Error::InvalidArgument(format!(
                "Slice out of bounds: begin={}, len={}, size={}",
                begin,
                len,
                self.nulls.len()
            )));
        }

        let sliced_nulls = self.nulls[begin..begin + len].to_vec();
        let sliced_nested = self.nested.slice(begin, len)?;

        let mut result = ColumnNullable::with_nested(sliced_nested);
        result.nulls = sliced_nulls;

        Ok(Arc::new(result))
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
    use crate::{
        column::{
            numeric::ColumnUInt64,
            string::ColumnString,
        },
        types::Type,
    };

    #[test]
    fn test_nullable_creation() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let col = ColumnNullable::with_nested(nested);
        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_nullable_append() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnNullable::with_nested(nested);

        col.append_non_null();
        col.append_null();
        col.append_non_null();

        assert_eq!(col.size(), 3);
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_nullable_nulls_bitmap() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnNullable::with_nested(nested);

        col.append_non_null();
        col.append_null();
        col.append_null();
        col.append_non_null();

        let nulls = col.nulls();
        assert_eq!(nulls, &[0, 1, 1, 0]);
    }

    #[test]
    fn test_nullable_save_load() {
        let mut nested = ColumnUInt64::new(Type::uint64());
        nested.append(10);
        nested.append(20);
        nested.append(30);

        let mut col = ColumnNullable::with_nested(Arc::new(nested));
        col.append_non_null();
        col.append_null();
        col.append_non_null();

        let mut buffer = BytesMut::new();
        col.save_to_buffer(&mut buffer).unwrap();

        // Verify buffer contains null bitmap + nested data
        let nulls_len = 3; // 3 rows
        assert!(buffer.len() >= nulls_len);
        assert_eq!(&buffer[..nulls_len], &[0, 1, 0]);
    }

    #[test]
    fn test_nullable_load_null_bitmap() {
        use bytes::{
            BufMut,
            BytesMut,
        };

        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnNullable::with_nested(nested);

        // Null bitmap: [1, 0, 1, 0, 1] (5 bytes)
        let mut data = BytesMut::new();
        data.extend_from_slice(&[1u8, 0, 1, 0, 1]);

        // Must also include nested data (5 UInt64 values)
        for i in 0..5u64 {
            data.put_u64_le(i);
        }

        let mut reader = &data[..];
        col.load_from_buffer(&mut reader, 5).unwrap();

        assert_eq!(col.size(), 5);
        assert!(col.is_null(0));
        assert!(!col.is_null(1));
        assert!(col.is_null(2));
        assert!(!col.is_null(3));
        assert!(col.is_null(4));
    }

    #[test]
    fn test_nullable_slice() {
        let mut nested = ColumnUInt64::new(Type::uint64());
        // Add data to the nested column
        for i in 0..10 {
            nested.append(i);
        }
        let mut col = ColumnNullable::with_nested(Arc::new(nested));

        for i in 0..10 {
            if i % 2 == 0 {
                col.append_null();
            } else {
                col.append_non_null();
            }
        }

        let sliced = col.slice(2, 5).unwrap();
        let sliced_col =
            sliced.as_any().downcast_ref::<ColumnNullable>().unwrap();

        assert_eq!(sliced_col.size(), 5);
        assert!(sliced_col.is_null(0)); // index 2 in original
        assert!(!sliced_col.is_null(1)); // index 3 in original
        assert!(sliced_col.is_null(2)); // index 4 in original
    }

    #[test]
    fn test_nullable_with_string() {
        let nested = Arc::new(ColumnString::new(Type::string()));
        let mut col = ColumnNullable::with_nested(nested);

        col.append_non_null();
        col.append_null();
        col.append_non_null();

        assert_eq!(col.size(), 3);
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));
    }

    #[test]
    fn test_nullable_type_mismatch() {
        let nested1 = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col1 = ColumnNullable::with_nested(nested1);

        let nested2 = Arc::new(ColumnString::new(Type::string()));
        let col2 = ColumnNullable::with_nested(nested2);

        let result = col1.append_column(Arc::new(col2));
        assert!(result.is_err());
    }

    #[test]
    fn test_nullable_out_of_bounds() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnNullable::with_nested(nested);

        col.append_null();
        col.append_non_null();

        // Out of bounds should return false (not null)
        assert!(!col.is_null(100));
    }

    #[test]
    fn test_nullable_append_column() {
        use crate::column::numeric::ColumnUInt32;

        // Create first nullable column: [Some(1), None, Some(3)]
        let mut col1 = ColumnNullable::with_nested(Arc::new(
            ColumnUInt32::new(Type::uint32()),
        ));
        col1.append_nullable(Some(1));
        col1.append_nullable(None);
        col1.append_nullable(Some(3));

        // Create second nullable column: [None, Some(5)]
        let mut col2 = ColumnNullable::with_nested(Arc::new(
            ColumnUInt32::new(Type::uint32()),
        ));
        col2.append_nullable(None);
        col2.append_nullable(Some(5));

        // Append col2 to col1
        col1.append_column(Arc::new(col2))
            .expect("append_column should succeed");

        // Verify we have 5 elements total
        assert_eq!(col1.size(), 5, "Should have 5 elements after append");

        // Verify null flags are correct
        assert!(!col1.is_null(0), "Element 0 should not be null (value=1)");
        assert!(col1.is_null(1), "Element 1 should be null");
        assert!(!col1.is_null(2), "Element 2 should not be null (value=3)");
        assert!(col1.is_null(3), "Element 3 should be null");
        assert!(!col1.is_null(4), "Element 4 should not be null (value=5)");

        // CRITICAL: Verify nested data was actually appended
        // The nested column should have 5 elements (including placeholders for
        // nulls)
        let nested =
            col1.nested.as_any().downcast_ref::<ColumnUInt32>().unwrap();
        assert_eq!(
            nested.size(),
            5,
            "Nested column should have 5 total elements"
        );

        // Verify null bitmap is correct: [0, 1, 0, 1, 0]
        assert_eq!(
            col1.nulls(),
            &[0, 1, 0, 1, 0],
            "Null bitmap should be [0, 1, 0, 1, 0]"
        );
    }

    #[test]
    #[should_panic(
        expected = "Cannot clear shared nullable column - column has multiple references"
    )]
    fn test_nullable_clear_panics_on_shared_nested() {
        use crate::column::numeric::ColumnUInt32;

        // Create a nullable column and add data BEFORE sharing the nested
        // column
        let mut col = ColumnNullable::with_nested(Arc::new(
            ColumnUInt32::new(Type::uint32()),
        ));
        col.append_nullable(Some(1));
        col.append_nullable(None);
        col.append_nullable(Some(3));

        // Create a second reference to the nested column (share it)
        let _shared_ref = col.nested.clone();

        // Now nested has multiple Arc references, so clear() MUST panic
        // to prevent data corruption (clearing null bitmap but not nested
        // data)
        col.clear();
    }

    #[test]
    fn test_nullable_roundtrip_nested_data() {
        use crate::column::numeric::ColumnUInt32;
        use bytes::BytesMut;

        // Create nullable column with data: [Some(1), None, Some(3)]
        let mut col = ColumnNullable::with_nested(Arc::new(
            ColumnUInt32::new(Type::uint32()),
        ));
        col.append_nullable(Some(1));
        col.append_nullable(None);
        col.append_nullable(Some(3));

        assert_eq!(col.size(), 3, "Original should have 3 elements");

        // Save to buffer
        let mut buffer = BytesMut::new();
        col.save_to_buffer(&mut buffer).expect("save should succeed");

        // Load into new nullable column
        let nested_empty = Arc::new(ColumnUInt32::new(Type::uint32()));
        let mut col_loaded = ColumnNullable::with_nested(nested_empty);

        let mut buf_slice = &buffer[..];
        col_loaded
            .load_from_buffer(&mut buf_slice, 3)
            .expect("load should succeed");

        // Verify structure
        assert_eq!(col_loaded.size(), 3, "Loaded should have 3 elements");

        // Verify null flags
        assert!(!col_loaded.is_null(0), "Element 0 should not be null");
        assert!(col_loaded.is_null(1), "Element 1 should be null");
        assert!(!col_loaded.is_null(2), "Element 2 should not be null");

        // CRITICAL: Verify nested data was actually loaded
        let nested_loaded =
            col_loaded.nested.as_any().downcast_ref::<ColumnUInt32>().unwrap();
        assert_eq!(
            nested_loaded.size(),
            3,
            "Nested should have 3 elements after load"
        );

        // Note: We can't easily verify the actual values without a getter
        // method, but the size check proves the data was loaded
    }
}
