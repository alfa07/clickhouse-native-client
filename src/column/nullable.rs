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
    numeric::ColumnUInt8,
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

/// Column for nullable values
///
/// Stores a nested column and a ColumnUInt8 for null flags (1 = null, 0 = not
/// null).
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
    nulls: ColumnRef, // ColumnUInt8
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

        let nulls = Arc::new(ColumnUInt8::new());
        Self { type_, nested, nulls }
    }

    /// Create a new nullable column wrapping an existing nested column
    pub fn with_nested(nested: ColumnRef) -> Self {
        let nested_type = nested.column_type().clone();
        let nulls = Arc::new(ColumnUInt8::new());
        Self { type_: Type::nullable(nested_type), nested, nulls }
    }

    /// Create with both nested and nulls columns
    pub fn from_parts(nested: ColumnRef, nulls: ColumnRef) -> Result<Self> {
        // Validate nulls is ColumnUInt8
        if nulls.column_type().name() != "UInt8" {
            return Err(Error::InvalidArgument(
                "nulls column must be UInt8".to_string(),
            ));
        }

        // Validate same size
        if nested.size() != nulls.size() {
            return Err(Error::InvalidArgument(format!(
                "nested and nulls must have same size: nested={}, nulls={}",
                nested.size(),
                nulls.size()
            )));
        }

        let nested_type = nested.column_type().clone();
        Ok(Self { type_: Type::nullable(nested_type), nested, nulls })
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

        let mut nulls = ColumnUInt8::new();
        nulls.reserve(capacity);
        Self { type_, nested, nulls: Arc::new(nulls) }
    }

    /// Append a null flag (matches C++ API)
    pub fn append(&mut self, isnull: bool) {
        let nulls_mut = Arc::get_mut(&mut self.nulls)
            .expect("Cannot append to shared nulls column")
            .as_any_mut()
            .downcast_mut::<ColumnUInt8>()
            .expect("nulls must be ColumnUInt8");
        nulls_mut.append(if isnull { 1 } else { 0 });
    }

    /// Append a null value
    pub fn append_null(&mut self) {
        self.append(true);
    }

    /// Append a non-null value (the nested column should be updated
    /// separately)
    pub fn append_non_null(&mut self) {
        self.append(false);
    }

    /// Check if value at index is null (matches C++ IsNull)
    pub fn is_null(&self, index: usize) -> bool {
        if index >= self.nulls.size() {
            return false;
        }
        let nulls_col = self
            .nulls
            .as_any()
            .downcast_ref::<ColumnUInt8>()
            .expect("nulls must be ColumnUInt8");
        nulls_col.at(index) != 0
    }

    /// Get a reference to the nested column as a specific type
    ///
    /// # Example
    /// ```
    /// let col: ColumnNullable = ...;
    /// let nested: &ColumnUInt32 = col.nested();
    /// ```
    pub fn nested<T: Column + 'static>(&self) -> &T {
        self.nested
            .as_any()
            .downcast_ref::<T>()
            .expect("Failed to downcast nested column to requested type")
    }

    /// Get mutable reference to the nested column as a specific type
    ///
    /// # Example
    /// ```
    /// let mut col: ColumnNullable = ...;
    /// let nested_mut: &mut ColumnUInt32 = col.nested_mut();
    /// ```
    pub fn nested_mut<T: Column + 'static>(&mut self) -> &mut T {
        Arc::get_mut(&mut self.nested)
            .expect("Cannot get mutable reference to shared nested column")
            .as_any_mut()
            .downcast_mut::<T>()
            .expect("Failed to downcast nested column to requested type")
    }

    /// Get the nested column as a ColumnRef (Arc<dyn Column>)
    pub fn nested_ref(&self) -> ColumnRef {
        self.nested.clone()
    }

    /// Get mutable access to the nested ColumnRef for dynamic dispatch
    /// scenarios
    ///
    /// This is useful when you need to modify the nested column but don't know
    /// its concrete type at compile time.
    pub fn nested_ref_mut(&mut self) -> &mut ColumnRef {
        &mut self.nested
    }

    /// Get the nulls column (matches C++ Nulls)
    pub fn nulls(&self) -> ColumnRef {
        self.nulls.clone()
    }

    /// Append a nullable UInt32 value (convenience method for tests)
    pub fn append_nullable(&mut self, value: Option<u32>) {
        use crate::column::numeric::ColumnUInt32;

        match value {
            None => {
                self.append_null();
                // Still need to add a placeholder to nested column to keep
                // indices aligned
                let nested_mut = Arc::get_mut(&mut self.nested)
                    .expect("Cannot append to shared nullable column - column has multiple references");
                let col = nested_mut
                    .as_any_mut()
                    .downcast_mut::<ColumnUInt32>()
                    .expect("Nullable nested column is not UInt32");
                col.append(0); // Placeholder value (ignored due to null flag)
            }
            Some(val) => {
                self.append_non_null();
                let nested_mut = Arc::get_mut(&mut self.nested)
                    .expect("Cannot append to shared nullable column - column has multiple references");
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
        self.nested_ref()
    }

    /// Get the number of elements (alias for size())
    pub fn len(&self) -> usize {
        self.nulls.size()
    }

    /// Check if the nullable column is empty
    pub fn is_empty(&self) -> bool {
        self.nulls.size() == 0
    }
}

impl Column for ColumnNullable {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.nulls.size()
    }

    fn clear(&mut self) {
        // Clear both columns
        let nulls_mut = Arc::get_mut(&mut self.nulls)
            .expect("Cannot clear shared nulls column");
        nulls_mut.clear();

        let nested_mut = Arc::get_mut(&mut self.nested)
            .expect("Cannot clear shared nested column");
        nested_mut.clear();
    }

    fn reserve(&mut self, new_cap: usize) {
        let nulls_mut = Arc::get_mut(&mut self.nulls)
            .expect("Cannot reserve in shared nulls column");
        nulls_mut.reserve(new_cap);

        let nested_mut = Arc::get_mut(&mut self.nested)
            .expect("Cannot reserve in shared nested column");
        nested_mut.reserve(new_cap);
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

        // Append nulls column
        let nulls_mut = Arc::get_mut(&mut self.nulls).ok_or_else(|| {
            Error::Protocol("Cannot append to shared nulls column".to_string())
        })?;
        nulls_mut.append_column(other.nulls.clone())?;

        // Append nested data
        let nested_mut = Arc::get_mut(&mut self.nested).ok_or_else(|| {
            Error::Protocol(
                "Cannot append to shared nested column".to_string(),
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
        // Load null bitmap
        if rows > 0 {
            let nulls_mut =
                Arc::get_mut(&mut self.nulls).ok_or_else(|| {
                    Error::Protocol(
                        "Cannot load into shared nulls column".to_string(),
                    )
                })?;
            nulls_mut.load_from_buffer(buffer, rows)?;

            // Load nested column data
            let nested_mut =
                Arc::get_mut(&mut self.nested).ok_or_else(|| {
                    Error::Protocol(
                        "Cannot load into shared nested column".to_string(),
                    )
                })?;
            nested_mut.load_from_buffer(buffer, rows)?;
        }

        Ok(())
    }

    fn save_prefix(&self, buffer: &mut BytesMut) -> Result<()> {
        // Delegate to nested column's save_prefix
        self.nested.save_prefix(buffer)
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Write null bitmap
        self.nulls.save_to_buffer(buffer)?;

        // Write nested column data
        self.nested.save_to_buffer(buffer)?;

        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(
            ColumnNullable::from_parts(
                self.nested.clone_empty(),
                self.nulls.clone_empty(),
            )
            .expect("clone_empty should succeed"),
        )
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

        let sliced_nulls = self.nulls.slice(begin, len)?;
        let sliced_nested = self.nested.slice(begin, len)?;

        Ok(Arc::new(
            ColumnNullable::from_parts(sliced_nested, sliced_nulls)
                .expect("slice should create valid ColumnNullable"),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Typed nullable column wrapper (matches C++ ColumnNullableT)
///
/// Provides typed access to nullable columns with methods that work
/// with `Option<T>` instead of raw column operations.
pub struct ColumnNullableT<T: Column> {
    inner: ColumnNullable,
    _phantom: PhantomData<T>,
}

impl<T: Column + 'static> ColumnNullableT<T> {
    /// Create a new typed nullable column from parts
    pub fn from_parts(nested: Arc<T>, nulls: ColumnRef) -> Result<Self> {
        let inner = ColumnNullable::from_parts(nested, nulls)?;
        Ok(Self { inner, _phantom: PhantomData })
    }

    /// Create from nested column only (all non-null initially)
    pub fn from_nested(nested: Arc<T>) -> Self {
        let size = nested.size();
        let mut nulls = ColumnUInt8::new();
        for _ in 0..size {
            nulls.append(0);
        }
        Self {
            inner: ColumnNullable::from_parts(nested, Arc::new(nulls))
                .expect("from_nested should succeed"),
            _phantom: PhantomData,
        }
    }

    /// Create with type
    pub fn new(type_: Type) -> Self {
        let inner = ColumnNullable::new(type_);
        Self { inner, _phantom: PhantomData }
    }

    /// Wrap a ColumnNullable (matches C++ Wrap)
    pub fn wrap(col: ColumnNullable) -> Self {
        Self { inner: col, _phantom: PhantomData }
    }

    /// Wrap from ColumnRef
    pub fn wrap_ref(col: ColumnRef) -> Result<Self> {
        let nullable = col
            .as_any()
            .downcast_ref::<ColumnNullable>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: "ColumnNullable".to_string(),
                actual: "unknown".to_string(),
            })?;

        // Clone the inner data to create owned ColumnNullable
        Ok(Self::wrap(ColumnNullable::from_parts(
            nullable.nested_ref(),
            nullable.nulls(),
        )?))
    }

    /// Get the typed nested column
    pub fn typed_nested(&self) -> Result<Arc<T>> {
        self.inner
            .nested_ref()
            .as_any()
            .downcast_ref::<T>()
            .map(|_| {
                // We need to clone the Arc with the right type
                // This is safe because we just verified the type
                unsafe {
                    let ptr = Arc::into_raw(self.inner.nested_ref());
                    let typed_ptr = ptr as *const T;
                    Arc::from_raw(typed_ptr)
                }
            })
            .ok_or_else(|| Error::TypeMismatch {
                expected: std::any::type_name::<T>().to_string(),
                actual: "unknown".to_string(),
            })
    }

    /// Check if value at index is null
    pub fn is_null(&self, index: usize) -> bool {
        self.inner.is_null(index)
    }

    /// Get the inner ColumnNullable
    pub fn inner(&self) -> &ColumnNullable {
        &self.inner
    }

    /// Get mutable inner ColumnNullable
    pub fn inner_mut(&mut self) -> &mut ColumnNullable {
        &mut self.inner
    }

    /// Get size
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T: Column + 'static> Column for ColumnNullableT<T> {
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

    fn save_prefix(&self, buffer: &mut BytesMut) -> Result<()> {
        self.inner.save_prefix(buffer)
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        self.inner.save_to_buffer(buffer)
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(Self::wrap(
            self.inner
                .clone_empty()
                .as_any()
                .downcast_ref::<ColumnNullable>()
                .expect("clone_empty must return ColumnNullable")
                .clone(),
        ))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        let sliced = self.inner.slice(begin, len)?;
        Ok(Arc::new(Self::wrap(
            sliced
                .as_any()
                .downcast_ref::<ColumnNullable>()
                .expect("slice must return ColumnNullable")
                .clone(),
        )))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// Implement Clone for ColumnNullable
impl Clone for ColumnNullable {
    fn clone(&self) -> Self {
        Self {
            type_: self.type_.clone(),
            nested: self.nested.clone(),
            nulls: self.nulls.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        column::{
            numeric::{
                ColumnUInt32,
                ColumnUInt64,
            },
            string::ColumnString,
        },
        types::Type,
    };

    #[test]
    fn test_nullable_creation() {
        let nested = Arc::new(ColumnUInt64::new());
        let col = ColumnNullable::with_nested(nested);
        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_nullable_append() {
        let nested = Arc::new(ColumnUInt64::new());
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
        let nested = Arc::new(ColumnUInt64::new());
        let mut col = ColumnNullable::with_nested(nested);

        col.append_non_null();
        col.append_null();
        col.append_null();
        col.append_non_null();

        let nulls_ref = col.nulls();
        let nulls_col =
            nulls_ref.as_any().downcast_ref::<ColumnUInt8>().unwrap();
        assert_eq!(nulls_col.at(0), 0);
        assert_eq!(nulls_col.at(1), 1);
        assert_eq!(nulls_col.at(2), 1);
        assert_eq!(nulls_col.at(3), 0);
    }

    #[test]
    fn test_nullable_save_load() {
        let mut nested = ColumnUInt64::new();
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

        let nested = Arc::new(ColumnUInt64::new());
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
        let mut nested = ColumnUInt64::new();
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
        let nested1 = Arc::new(ColumnUInt64::new());
        let mut col1 = ColumnNullable::with_nested(nested1);

        let nested2 = Arc::new(ColumnString::new(Type::string()));
        let col2 = ColumnNullable::with_nested(nested2);

        let result = col1.append_column(Arc::new(col2));
        assert!(result.is_err());
    }

    #[test]
    fn test_nullable_out_of_bounds() {
        let nested = Arc::new(ColumnUInt64::new());
        let mut col = ColumnNullable::with_nested(nested);

        col.append_null();
        col.append_non_null();

        // Out of bounds should return false (not null)
        assert!(!col.is_null(100));
    }

    #[test]
    fn test_nullable_append_column() {
        // Create first nullable column: [Some(1), None, Some(3)]
        let mut col1 =
            ColumnNullable::with_nested(Arc::new(ColumnUInt32::new()));
        col1.append_nullable(Some(1));
        col1.append_nullable(None);
        col1.append_nullable(Some(3));

        // Create second nullable column: [None, Some(5)]
        let mut col2 =
            ColumnNullable::with_nested(Arc::new(ColumnUInt32::new()));
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

        // Verify nested data was actually appended
        let nested: &ColumnUInt32 = col1.nested();
        assert_eq!(
            nested.size(),
            5,
            "Nested column should have 5 total elements"
        );
    }

    #[test]
    #[should_panic(expected = "Cannot clear shared nulls column")]
    fn test_nullable_clear_panics_on_shared_nulls() {
        use crate::column::numeric::ColumnUInt32;

        // Create a nullable column and add data
        let mut col =
            ColumnNullable::with_nested(Arc::new(ColumnUInt32::new()));
        col.append_nullable(Some(1));
        col.append_nullable(None);
        col.append_nullable(Some(3));

        // Create a second reference to the nulls column (share it)
        let _shared_ref = col.nulls();

        // Now nulls has multiple Arc references, so clear() MUST panic
        col.clear();
    }

    #[test]
    fn test_nullable_roundtrip_nested_data() {
        use bytes::BytesMut;

        // Create nullable column with data: [Some(1), None, Some(3)]
        let mut col =
            ColumnNullable::with_nested(Arc::new(ColumnUInt32::new()));
        col.append_nullable(Some(1));
        col.append_nullable(None);
        col.append_nullable(Some(3));

        assert_eq!(col.size(), 3, "Original should have 3 elements");

        // Save to buffer
        let mut buffer = BytesMut::new();
        col.save_to_buffer(&mut buffer).expect("save should succeed");

        // Load into new nullable column
        let nested_empty = Arc::new(ColumnUInt32::new());
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

        // Verify nested data was actually loaded
        let nested_loaded: &ColumnUInt32 = col_loaded.nested();
        assert_eq!(
            nested_loaded.size(),
            3,
            "Nested should have 3 elements after load"
        );
    }

    #[test]
    fn test_nullable_t_creation() {
        let nested = Arc::new(ColumnUInt64::new());
        let col = ColumnNullableT::<ColumnUInt64>::from_nested(nested);
        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_nullable_t_wrap() {
        let nested = Arc::new(ColumnUInt64::new());
        let nullable = ColumnNullable::with_nested(nested);
        let col_t = ColumnNullableT::<ColumnUInt64>::wrap(nullable);
        assert_eq!(col_t.size(), 0);
    }

    #[test]
    fn test_nullable_t_typed_nested() {
        let mut nested = ColumnUInt64::new();
        nested.append(42);
        let col =
            ColumnNullableT::<ColumnUInt64>::from_nested(Arc::new(nested));

        let typed = col.typed_nested().unwrap();
        assert_eq!(typed.size(), 1);
        assert_eq!(typed.at(0), 42);
    }
}
