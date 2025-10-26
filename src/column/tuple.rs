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
use std::{
    marker::PhantomData,
    sync::Arc,
};

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
            let col_mut = Arc::get_mut(col)
                .ok_or_else(|| Error::Protocol(
                    "Cannot append to shared tuple column - column has multiple references".to_string()
                ))?;
            col_mut.append_column(other.columns[i].clone())?;
        }

        Ok(())
    }

    fn load_prefix(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
        // Call load_prefix on all tuple element columns
        for col in &mut self.columns {
            let col_mut = Arc::get_mut(col).ok_or_else(|| {
                Error::Protocol(
                    "Cannot load prefix for shared tuple column".to_string(),
                )
            })?;
            col_mut.load_prefix(buffer, rows)?;
        }
        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        for col in &mut self.columns {
            let col_mut = Arc::get_mut(col)
                .ok_or_else(|| Error::Protocol(
                    "Cannot load into shared tuple column - column has multiple references".to_string()
                ))?;
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

/// Typed wrapper for ColumnTuple that provides type-safe access to tuple columns
///
/// This is analogous to `ColumnTupleT<...Columns>` in clickhouse-cpp, providing
/// compile-time type safety for tuple operations.
///
/// Due to Rust's lack of variadic generics, we provide implementations for
/// tuples up to size 12 using a macro.
///
/// **Reference Implementation:** See `clickhouse-cpp/clickhouse/columns/tuple.h`

// Macro to implement ColumnTupleT for different tuple sizes
macro_rules! impl_column_tuple_t {
    ($($T:ident),+; $($idx:tt),+) => {
        #[allow(non_snake_case)]
        pub struct ColumnTupleT<$($T),+>
        where
            $($T: Column + 'static),+
        {
            inner: ColumnTuple,
            _phantom: PhantomData<fn() -> ($($T,)+)>,
        }

        #[allow(non_snake_case)]
        impl<$($T),+> ColumnTupleT<$($T),+>
        where
            $($T: Column + 'static),+
        {
            /// Create a new typed tuple column from individual columns
            ///
            /// This constructor uses type inference to determine the Tuple type from
            /// the provided columns.
            ///
            /// # Example
            /// ```ignore
            /// let col1 = Arc::new(ColumnUInt64::new());
            /// let col2 = Arc::new(ColumnString::new(Type::string()));
            /// let tuple = ColumnTupleT::from_columns((col1, col2));
            /// // Type is automatically inferred as Tuple(UInt64, String)
            /// ```
            pub fn from_columns(columns: ($(Arc<$T>,)+)) -> Self {
                let ($($T,)+) = columns;

                let types = vec![$($T.column_type().clone(),)+];
                let tuple_type = Type::Tuple { item_types: types };

                let columns_vec: Vec<ColumnRef> = vec![$($T as ColumnRef,)+];

                let inner = ColumnTuple::new(tuple_type, columns_vec);
                Self { inner, _phantom: PhantomData }
            }

            /// Create a new typed tuple column from an existing ColumnTuple
            ///
            /// Returns an error if the underlying structure doesn't match
            /// the expected column types.
            pub fn try_from_tuple(tuple: ColumnTuple) -> Result<Self> {
                // Count the columns
                let expected_count = [$($idx),+].len();
                if tuple.column_count() != expected_count {
                    return Err(Error::InvalidArgument(format!(
                        "Tuple column count mismatch: expected {}, found {}",
                        expected_count,
                        tuple.column_count()
                    )));
                }

                // Verify each column type
                $(
                    let col = tuple.column_at($idx);
                    let _ = col.as_any().downcast_ref::<$T>().ok_or_else(|| {
                        Error::InvalidArgument(format!(
                            "Column {} type mismatch: expected {}, found {}",
                            $idx,
                            std::any::type_name::<$T>(),
                            col.column_type().name()
                        ))
                    })?;
                )+

                Ok(Self { inner: tuple, _phantom: PhantomData })
            }

            /// Get typed references to the tuple columns
            ///
            /// Returns a tuple of references to the concrete column types.
            pub fn columns(&self) -> Result<($(&$T,)+)> {
                Ok(($(
                    self.inner
                        .column_at($idx)
                        .as_any()
                        .downcast_ref::<$T>()
                        .ok_or_else(|| {
                            Error::InvalidArgument(format!(
                                "Failed to downcast column {} to {}",
                                $idx,
                                std::any::type_name::<$T>()
                            ))
                        })?,
                )+))
            }

            /// Get the number of rows
            pub fn len(&self) -> usize {
                self.inner.len()
            }

            /// Check if the column is empty
            pub fn is_empty(&self) -> bool {
                self.inner.is_empty()
            }

            /// Get reference to inner ColumnTuple
            pub fn inner(&self) -> &ColumnTuple {
                &self.inner
            }

            /// Get mutable reference to inner ColumnTuple
            pub fn inner_mut(&mut self) -> &mut ColumnTuple {
                &mut self.inner
            }

            /// Convert into inner ColumnTuple
            pub fn into_inner(self) -> ColumnTuple {
                self.inner
            }
        }

        #[allow(non_snake_case)]
        impl<$($T),+> Column for ColumnTupleT<$($T),+>
        where
            $($T: Column + 'static),+
        {
            fn column_type(&self) -> &Type {
                self.inner.column_type()
            }

            fn size(&self) -> usize {
                self.inner.size()
            }

            fn clear(&mut self) {
                self.inner.clear()
            }

            fn reserve(&mut self, new_cap: usize) {
                self.inner.reserve(new_cap)
            }

            fn append_column(&mut self, other: ColumnRef) -> Result<()> {
                self.inner.append_column(other)
            }

            fn load_from_buffer(
                &mut self,
                buffer: &mut &[u8],
                rows: usize,
            ) -> Result<()> {
                self.inner.load_from_buffer(buffer, rows)
            }

            fn load_prefix(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
                self.inner.load_prefix(buffer, rows)
            }

            fn save_prefix(&self, buffer: &mut BytesMut) -> Result<()> {
                self.inner.save_prefix(buffer)
            }

            fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
                self.inner.save_to_buffer(buffer)
            }

            fn clone_empty(&self) -> ColumnRef {
                let cloned = self.inner.clone_empty();
                Arc::new(ColumnTupleT::<$($T),+> {
                    inner: match cloned.as_any().downcast_ref::<ColumnTuple>() {
                        Some(tuple) => ColumnTuple::new(
                            tuple.column_type().clone(),
                            (0..tuple.column_count()).map(|i| tuple.column_at(i)).collect(),
                        ),
                        None => {
                            // Fallback: create a new empty tuple with same type
                            ColumnTuple::new(
                                self.inner.column_type().clone(),
                                vec![]
                            )
                        }
                    },
                    _phantom: PhantomData,
                })
            }

            fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
                let sliced_inner = self.inner.slice(begin, len)?;
                let sliced_tuple = sliced_inner
                    .as_any()
                    .downcast_ref::<ColumnTuple>()
                    .ok_or_else(|| {
                        Error::InvalidArgument(
                            "Failed to downcast sliced column".to_string(),
                        )
                    })?;

                Ok(Arc::new(ColumnTupleT::<$($T),+> {
                    inner: ColumnTuple::new(
                        sliced_tuple.column_type().clone(),
                        (0..sliced_tuple.column_count())
                            .map(|i| sliced_tuple.column_at(i))
                            .collect(),
                    ),
                    _phantom: PhantomData,
                }))
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }
        }
    };
}

// Generate implementations for tuples of size 1-12
impl_column_tuple_t!(T0; 0);
impl_column_tuple_t!(T0, T1; 0, 1);
impl_column_tuple_t!(T0, T1, T2; 0, 1, 2);
impl_column_tuple_t!(T0, T1, T2, T3; 0, 1, 2, 3);
impl_column_tuple_t!(T0, T1, T2, T3, T4; 0, 1, 2, 3, 4);
impl_column_tuple_t!(T0, T1, T2, T3, T4, T5; 0, 1, 2, 3, 4, 5);
impl_column_tuple_t!(T0, T1, T2, T3, T4, T5, T6; 0, 1, 2, 3, 4, 5, 6);
impl_column_tuple_t!(T0, T1, T2, T3, T4, T5, T6, T7; 0, 1, 2, 3, 4, 5, 6, 7);
impl_column_tuple_t!(T0, T1, T2, T3, T4, T5, T6, T7, T8; 0, 1, 2, 3, 4, 5, 6, 7, 8);
impl_column_tuple_t!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9; 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
impl_column_tuple_t!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10; 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
impl_column_tuple_t!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11; 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
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

        let col1 = Arc::new(ColumnUInt64::new()) as ColumnRef;
        let col2 = Arc::new(ColumnString::new(Type::string())) as ColumnRef;

        let tuple = ColumnTuple::new(tuple_type, vec![col1, col2]);

        assert_eq!(tuple.column_count(), 2);
        assert_eq!(tuple.size(), 0);
    }

    #[test]
    fn test_tuple_slice() {
        let types = vec![Type::uint64(), Type::string()];
        let tuple_type = Type::tuple(types);

        let mut col1 = ColumnUInt64::new();
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

    // ColumnTupleT tests
    #[test]
    fn test_tuple_t_2_creation() {
        let col1 = Arc::new(ColumnUInt64::new());
        let col2 = Arc::new(ColumnString::new(Type::string()));

        let tuple = ColumnTupleT::from_columns((col1, col2));

        assert_eq!(tuple.len(), 0);
        assert!(tuple.is_empty());
    }

    #[test]
    fn test_tuple_t_2_columns() {
        let col1 = Arc::new(ColumnUInt64::new());
        let col2 = Arc::new(ColumnString::new(Type::string()));

        let tuple = ColumnTupleT::from_columns((col1.clone(), col2.clone()));

        // Verify we can access typed columns
        let (c1, c2) = tuple.columns().expect("should get columns");

        assert_eq!(c1.size(), 0);
        assert_eq!(c2.size(), 0);
    }

    #[test]
    fn test_tuple_t_3_creation() {
        let col1 = Arc::new(ColumnUInt64::new());
        let col2 = Arc::new(ColumnString::new(Type::string()));
        let col3 = Arc::new(ColumnUInt32::new());

        let tuple = ColumnTupleT::from_columns((col1, col2, col3));

        assert_eq!(tuple.len(), 0);
        assert_eq!(tuple.inner().column_count(), 3);
    }

    #[test]
    fn test_tuple_t_try_from_tuple() {
        let types = vec![Type::uint64(), Type::string()];
        let tuple_type = Type::tuple(types);

        let col1 = Arc::new(ColumnUInt64::new()) as ColumnRef;
        let col2 = Arc::new(ColumnString::new(Type::string())) as ColumnRef;

        let tuple = ColumnTuple::new(tuple_type, vec![col1, col2]);

        // Convert to typed tuple
        let typed_tuple =
            ColumnTupleT::<ColumnUInt64, ColumnString>::try_from_tuple(tuple)
                .expect("should convert to typed tuple");

        assert_eq!(typed_tuple.len(), 0);
    }

    #[test]
    fn test_tuple_t_type_mismatch() {
        let types = vec![Type::uint64(), Type::string()];
        let tuple_type = Type::tuple(types);

        let col1 = Arc::new(ColumnUInt64::new()) as ColumnRef;
        let col2 = Arc::new(ColumnString::new(Type::string())) as ColumnRef;

        let tuple = ColumnTuple::new(tuple_type, vec![col1, col2]);

        // Try to convert to wrong type - should fail (expecting UInt32 instead of
        // UInt64)
        let result =
            ColumnTupleT::<ColumnUInt32, ColumnString>::try_from_tuple(tuple);

        assert!(result.is_err());
    }

    #[test]
    fn test_tuple_t_count_mismatch() {
        let types = vec![Type::uint64(), Type::string()];
        let tuple_type = Type::tuple(types);

        let col1 = Arc::new(ColumnUInt64::new()) as ColumnRef;
        let col2 = Arc::new(ColumnString::new(Type::string())) as ColumnRef;

        let tuple = ColumnTuple::new(tuple_type, vec![col1, col2]);

        // Try to convert to 3-element tuple - should fail
        let result = ColumnTupleT::<
            ColumnUInt64,
            ColumnString,
            ColumnUInt32,
        >::try_from_tuple(tuple);

        assert!(result.is_err());
    }
}
