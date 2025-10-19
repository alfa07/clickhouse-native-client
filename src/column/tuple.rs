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

/// Column for tuple types (fixed number of heterogeneous columns)
pub struct ColumnTuple {
    type_: Type,
    columns: Vec<ColumnRef>,
}

impl ColumnTuple {
    pub fn new(type_: Type, columns: Vec<ColumnRef>) -> Self {
        Self { type_, columns }
    }

    /// Get the number of columns in the tuple
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get a reference to a specific column in the tuple
    pub fn column_at(&self, index: usize) -> ColumnRef {
        self.columns[index].clone()
    }

    /// Get mutable reference to a specific column (for appending)
    pub fn column_at_mut(&mut self, index: usize) -> &mut dyn Column {
        Arc::get_mut(&mut self.columns[index])
            .expect("Cannot get mutable reference to shared column")
    }

    /// Get the number of elements (rows) - all columns should have the same
    /// size
    pub fn len(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].size()
        }
    }

    /// Check if the tuple column is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Column for ColumnTuple {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.len()
    }

    fn clear(&mut self) {
        for col in &mut self.columns {
            let col_mut = Arc::get_mut(col)
                .expect("Cannot clear shared tuple column - column has multiple references");
            col_mut.clear();
        }
    }

    fn reserve(&mut self, new_cap: usize) {
        for col in &mut self.columns {
            let col_mut = Arc::get_mut(col)
                .expect("Cannot reserve on shared tuple column - column has multiple references");
            col_mut.reserve(new_cap);
        }
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let other =
            other.as_any().downcast_ref::<ColumnTuple>().ok_or_else(|| {
                Error::TypeMismatch {
                    expected: self.type_.name(),
                    actual: other.column_type().name(),
                }
            })?;

        if self.columns.len() != other.columns.len() {
            return Err(Error::TypeMismatch {
                expected: format!("Tuple with {} columns", self.columns.len()),
                actual: format!("Tuple with {} columns", other.columns.len()),
            });
        }

        for (i, col) in self.columns.iter_mut().enumerate() {
            let col_mut = Arc::get_mut(col).ok_or_else(|| {
                Error::Protocol(
                    "Cannot append to shared tuple column - column has multiple references"
                        .to_string(),
                )
            })?;
            col_mut.append_column(other.columns[i].clone())?;
        }

        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        for col in &mut self.columns {
            let col_mut = Arc::get_mut(col).ok_or_else(|| {
                Error::Protocol(
                    "Cannot load into shared tuple column - column has multiple references"
                        .to_string(),
                )
            })?;
            col_mut.load_from_buffer(buffer, rows)?;
        }
        Ok(())
    }

    fn save_prefix(&self, buffer: &mut BytesMut) -> Result<()> {
        // Call save_prefix on all tuple element columns
        for col in &self.columns {
            col.save_prefix(buffer)?;
        }
        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for col in &self.columns {
            col.save_to_buffer(buffer)?;
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        let empty_cols: Vec<ColumnRef> =
            self.columns.iter().map(|c| c.clone_empty()).collect();
        Arc::new(ColumnTuple::new(self.type_.clone(), empty_cols))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        if begin + len > self.size() {
            return Err(Error::InvalidArgument(format!(
                "Slice out of bounds: begin={}, len={}, size={}",
                begin,
                len,
                self.size()
            )));
        }

        let sliced_cols: Result<Vec<ColumnRef>> =
            self.columns.iter().map(|col| col.slice(begin, len)).collect();

        Ok(Arc::new(ColumnTuple::new(self.type_.clone(), sliced_cols?)))
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
            ColumnString,
            ColumnUInt64,
        },
        types::Type,
    };

    #[test]
    fn test_tuple_creation() {
        let types = vec![Type::uint64(), Type::string()];
        let tuple_type = Type::tuple(types);

        let col1 = Arc::new(ColumnUInt64::new(Type::uint64())) as ColumnRef;
        let col2 = Arc::new(ColumnString::new(Type::string())) as ColumnRef;

        let tuple = ColumnTuple::new(tuple_type, vec![col1, col2]);

        assert_eq!(tuple.column_count(), 2);
        assert_eq!(tuple.size(), 0);
    }

    #[test]
    fn test_tuple_slice() {
        let types = vec![Type::uint64(), Type::string()];
        let tuple_type = Type::tuple(types);

        let mut col1 = ColumnUInt64::new(Type::uint64());
        col1.append(1);
        col1.append(2);
        col1.append(3);

        let mut col2 = ColumnString::new(Type::string());
        col2.append("a");
        col2.append("b");
        col2.append("c");

        let tuple = ColumnTuple::new(
            tuple_type,
            vec![Arc::new(col1) as ColumnRef, Arc::new(col2) as ColumnRef],
        );

        let sliced = tuple.slice(1, 2).unwrap();
        assert_eq!(sliced.size(), 2);

        let sliced_tuple =
            sliced.as_any().downcast_ref::<ColumnTuple>().unwrap();
        let col_ref = sliced_tuple.column_at(0);
        let sliced_col1 =
            col_ref.as_any().downcast_ref::<ColumnUInt64>().unwrap();
        assert_eq!(sliced_col1.at(0), 2);
        assert_eq!(sliced_col1.at(1), 3);
    }
}
