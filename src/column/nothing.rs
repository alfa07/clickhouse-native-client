//! Nothing/Void column implementation.
//!
//! This is a dummy column that tracks size without storing actual data.
//! Used for `NULL`-only columns or as a placeholder.

use super::{
    Column,
    ColumnRef,
};
use crate::{
    types::Type,
    Error,
    Result,
};
use bytes::BytesMut;
use std::sync::Arc;

/// Column for Nothing/Void type. Tracks row count without storing data.
pub struct ColumnNothing {
    type_: Type,
    size: usize,
}

impl ColumnNothing {
    /// Create a new empty Nothing column.
    pub fn new(type_: Type) -> Self {
        Self { type_, size: 0 }
    }

    /// Set the initial size (number of nothing/null entries).
    pub fn with_size(mut self, size: usize) -> Self {
        self.size = size;
        self
    }

    /// Append a NULL/nothing value (just increments size)
    pub fn append(&mut self) {
        self.size += 1;
    }

    /// Get value at index (always returns None)
    pub fn at(&self, _index: usize) -> Option<()> {
        None
    }

    /// Returns the number of entries in this column.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Returns `true` if the column contains no entries.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

impl Column for ColumnNothing {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.size
    }

    fn clear(&mut self) {
        self.size = 0;
    }

    fn reserve(&mut self, _new_cap: usize) {
        // Nothing to reserve
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<ColumnNothing>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            })?;

        self.size += other.size;
        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        // Nothing type doesn't actually consume any data
        // But we need to skip the appropriate bytes (1 byte per row of
        // "nothing")
        if buffer.len() < rows {
            return Err(Error::Protocol(
                "Not enough data for Nothing".to_string(),
            ));
        }
        *buffer = &buffer[rows..];
        self.size += rows;
        Ok(())
    }

    fn save_to_buffer(&self, _buffer: &mut BytesMut) -> Result<()> {
        // Nothing type doesn't serialize any data
        // According to C++ implementation, SaveBody should not be supported
        Err(Error::Protocol(
            "SaveBody is not supported for Nothing column".to_string(),
        ))
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnNothing::new(self.type_.clone()))
    }

    fn slice(&self, _begin: usize, len: usize) -> Result<ColumnRef> {
        // Slice just creates a new Nothing column with the specified length
        Ok(Arc::new(ColumnNothing::new(self.type_.clone()).with_size(len)))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::types::TypeCode;

    fn void_type() -> Type {
        Type::Simple(TypeCode::Void)
    }

    #[test]
    fn test_nothing_append() {
        let mut col = ColumnNothing::new(void_type());
        col.append();
        col.append();
        col.append();

        assert_eq!(col.len(), 3);
        assert_eq!(col.at(0), None);
        assert_eq!(col.at(1), None);
        assert_eq!(col.at(2), None);
    }

    #[test]
    fn test_nothing_with_size() {
        let col = ColumnNothing::new(void_type()).with_size(10);
        assert_eq!(col.len(), 10);
    }

    #[test]
    fn test_nothing_slice() {
        let col = ColumnNothing::new(void_type()).with_size(10);
        let sliced = col.slice(2, 5).unwrap();
        let sliced_col =
            sliced.as_any().downcast_ref::<ColumnNothing>().unwrap();

        assert_eq!(sliced_col.len(), 5);
    }

    #[test]
    fn test_nothing_append_column() {
        let mut col1 = ColumnNothing::new(void_type()).with_size(5);
        let col2 = Arc::new(ColumnNothing::new(void_type()).with_size(3));

        col1.append_column(col2).unwrap();
        assert_eq!(col1.len(), 8);
    }

    #[test]
    fn test_nothing_clear() {
        let mut col = ColumnNothing::new(void_type()).with_size(10);
        assert_eq!(col.len(), 10);

        col.clear();
        assert_eq!(col.len(), 0);
        assert!(col.is_empty());
    }
}
