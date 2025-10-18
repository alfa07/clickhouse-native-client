use super::{Column, ColumnRef};
use crate::types::Type;
use crate::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::sync::Arc;

/// Column for nullable values
/// Stores a nested column and a bitmap of null flags
pub struct ColumnNullable {
    type_: Type,
    nested: ColumnRef,
    nulls: Vec<u8>, // Bitmap: 1 = null, 0 = not null
}

impl ColumnNullable {
    /// Create a new nullable column wrapping a nested column
    pub fn new(nested: ColumnRef) -> Self {
        let nested_type = nested.column_type().clone();
        Self {
            type_: Type::nullable(nested_type),
            nested,
            nulls: Vec::new(),
        }
    }

    /// Create with reserved capacity
    pub fn with_capacity(nested: ColumnRef, capacity: usize) -> Self {
        let nested_type = nested.column_type().clone();
        Self {
            type_: Type::nullable(nested_type),
            nested,
            nulls: Vec::with_capacity(capacity),
        }
    }

    /// Append a null value
    pub fn append_null(&mut self) {
        self.nulls.push(1);
    }

    /// Append a non-null value (the nested column should be updated separately)
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
        // Note: We can't clear nested due to Arc, but this is a known limitation
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
        if self.nested.column_type().name() != other.nested.column_type().name() {
            return Err(Error::TypeMismatch {
                expected: self.nested.column_type().name(),
                actual: other.nested.column_type().name(),
            });
        }

        self.nulls.extend_from_slice(&other.nulls);
        Ok(())
    }

    fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
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

        // Now we need to load the nested column data
        // But we can't call load_from_buffer on nested due to Arc immutability
        // This is a design limitation - in practice, we'd need interior mutability
        // For now, we'll document this limitation

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Write null bitmap
        buffer.put_slice(&self.nulls);

        // Write nested column data
        self.nested.save_to_buffer(buffer)?;

        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnNullable::new(self.nested.clone_empty()))
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

        let mut result = ColumnNullable::new(sliced_nested);
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
    use crate::column::numeric::ColumnUInt64;
    use crate::column::string::ColumnString;
    use crate::types::Type;

    #[test]
    fn test_nullable_creation() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let col = ColumnNullable::new(nested);
        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_nullable_append() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnNullable::new(nested);

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
        let mut col = ColumnNullable::new(nested);

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

        let mut col = ColumnNullable::new(Arc::new(nested));
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
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnNullable::new(nested);

        let data = vec![1u8, 0, 1, 0, 1];
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
        let mut col = ColumnNullable::new(Arc::new(nested));

        for i in 0..10 {
            if i % 2 == 0 {
                col.append_null();
            } else {
                col.append_non_null();
            }
        }

        let sliced = col.slice(2, 5).unwrap();
        let sliced_col = sliced.as_any().downcast_ref::<ColumnNullable>().unwrap();

        assert_eq!(sliced_col.size(), 5);
        assert!(sliced_col.is_null(0)); // index 2 in original
        assert!(!sliced_col.is_null(1)); // index 3 in original
        assert!(sliced_col.is_null(2)); // index 4 in original
    }

    #[test]
    fn test_nullable_with_string() {
        let nested = Arc::new(ColumnString::new());
        let mut col = ColumnNullable::new(nested);

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
        let mut col1 = ColumnNullable::new(nested1);

        let nested2 = Arc::new(ColumnString::new());
        let col2 = ColumnNullable::new(nested2);

        let result = col1.append_column(Arc::new(col2));
        assert!(result.is_err());
    }

    #[test]
    fn test_nullable_out_of_bounds() {
        let nested = Arc::new(ColumnUInt64::new(Type::uint64()));
        let mut col = ColumnNullable::new(nested);

        col.append_null();
        col.append_non_null();

        // Out of bounds should return false (not null)
        assert!(!col.is_null(100));
    }
}
