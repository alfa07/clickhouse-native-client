use super::{
    Column,
    ColumnArray,
    ColumnArrayT,
    ColumnRef,
    ColumnTuple,
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

/// Column for Map(K, V) type
/// Maps are stored internally as Array(Tuple(K, V))
pub struct ColumnMap {
    type_: Type,
    data: ColumnRef, // Array of Tuple(key, value)
}

impl ColumnMap {
    pub fn new(type_: Type) -> Self {
        // Extract key and value types from Map type
        let (key_type, value_type) = match &type_ {
            Type::Map { key_type, value_type } => {
                (key_type.as_ref().clone(), value_type.as_ref().clone())
            }
            _ => panic!("ColumnMap requires Map type"),
        };

        // Create the underlying Array(Tuple(K, V)) type
        let tuple_type =
            Type::Tuple { item_types: vec![key_type, value_type] };
        let array_type = Type::Array { item_type: Box::new(tuple_type) };

        // Create the array column with the correct type
        let data: ColumnRef = Arc::new(ColumnArray::new(array_type));

        Self { type_, data }
    }

    /// Create from existing array data
    pub fn from_array(type_: Type, data: ColumnRef) -> Self {
        Self { type_, data }
    }

    /// Get a reference to the data column as a specific type
    ///
    /// # Example
    /// ```
    /// let col: ColumnMap = ...;
    /// let data: &ColumnArray = col.data();
    /// ```
    pub fn data<T: Column + 'static>(&self) -> &T {
        self.data
            .as_any()
            .downcast_ref::<T>()
            .expect("Failed to downcast data column to requested type")
    }

    /// Get mutable reference to the data column as a specific type
    ///
    /// # Example
    /// ```
    /// let mut col: ColumnMap = ...;
    /// let data_mut: &mut ColumnArray = col.data_mut();
    /// ```
    pub fn data_mut<T: Column + 'static>(&mut self) -> &mut T {
        Arc::get_mut(&mut self.data)
            .expect("Cannot get mutable reference to shared data column")
            .as_any_mut()
            .downcast_mut::<T>()
            .expect("Failed to downcast data column to requested type")
    }

    /// Get the data column as a ColumnRef (Arc<dyn Column>)
    pub fn data_ref(&self) -> ColumnRef {
        self.data.clone()
    }

    /// Get the underlying array column as ColumnArray if possible
    pub fn as_array(&self) -> Option<&ColumnArray> {
        self.data.as_any().downcast_ref::<ColumnArray>()
    }

    /// Get map at index as a column reference
    /// The returned column is a Tuple(K, V) array
    pub fn at(&self, index: usize) -> Result<ColumnRef> {
        // Delegate to the array's slice functionality
        self.data.slice(index, 1)
    }

    pub fn len(&self) -> usize {
        self.data.size()
    }

    pub fn is_empty(&self) -> bool {
        self.data.size() == 0
    }
}

impl Column for ColumnMap {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.data.size()
    }

    fn clear(&mut self) {
        // Create a new empty column
        let new_col = ColumnMap::new(self.type_.clone());
        self.data = new_col.data;
    }

    fn reserve(&mut self, _new_cap: usize) {
        // Reserve not supported through ColumnRef without downcasting
        // This is a limitation of the wrapper approach
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let _other =
            other.as_any().downcast_ref::<ColumnMap>().ok_or_else(|| {
                Error::TypeMismatch {
                    expected: self.type_.name(),
                    actual: other.column_type().name(),
                }
            })?;

        // Append not easily supported through ColumnRef
        Err(Error::Protocol(
            "append_column not fully supported for Map".to_string(),
        ))
    }

    fn load_prefix(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
        // Delegate to underlying array's load_prefix
        // CRITICAL: This ensures nested LowCardinality columns in Map values
        // have their key_version read before load_from_buffer is called
        let data_mut = Arc::get_mut(&mut self.data).ok_or_else(|| {
            Error::Protocol(
                "Cannot load prefix for shared map column".to_string(),
            )
        })?;
        data_mut.load_prefix(buffer, rows)
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        // Create a new column with correct array type and load into it
        let mut new_col = ColumnMap::new(self.type_.clone());

        // Get mutable access to the underlying array
        if let Some(array) = Arc::get_mut(&mut new_col.data) {
            if let Some(array_mut) =
                array.as_any_mut().downcast_mut::<ColumnArray>()
            {
                array_mut.load_from_buffer(buffer, rows)?;
                self.data = new_col.data;
                return Ok(());
            }
        }

        Err(Error::Protocol(
            "Failed to load Map column from buffer".to_string(),
        ))
    }

    fn save_prefix(&self, buffer: &mut BytesMut) -> Result<()> {
        // Delegate to underlying array's save_prefix
        // CRITICAL: This ensures nested LowCardinality columns in Map values
        // have their key_version written before save_to_buffer is called
        self.data.save_prefix(buffer)
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        self.data.save_to_buffer(buffer)
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnMap::new(self.type_.clone()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        // Get the sliced array
        let sliced_data = self.data.slice(begin, len)?;

        // ColumnMap wraps the sliced array directly
        // We store it as ColumnRef in a new ColumnMap structure
        Ok(Arc::new(ColumnMap {
            type_: self.type_.clone(),
            data: sliced_data,
        }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// Note: ColumnArray doesn't implement Clone, so we need to work around this
impl Clone for ColumnMap {
    fn clone(&self) -> Self {
        Self { type_: self.type_.clone(), data: self.data.clone() }
    }
}

/// Typed wrapper for ColumnMap that provides type-safe access to key and value columns
///
/// This is analogous to `ColumnMapT<K, V>` in clickhouse-cpp, providing
/// compile-time type safety for map operations.
///
/// Maps are internally stored as Array(Tuple(K, V)), and this wrapper provides
/// convenient typed access to the underlying structure.
///
/// **Reference Implementation:** See `clickhouse-cpp/clickhouse/columns/map.h`
pub struct ColumnMapT<K, V>
where
    K: Column + 'static,
    V: Column + 'static,
{
    inner: ColumnMap,
    _phantom: PhantomData<fn() -> (K, V)>,
}

impl<K, V> ColumnMapT<K, V>
where
    K: Column + 'static,
    V: Column + 'static,
{
    /// Create a new typed map column from key and value columns
    ///
    /// This constructor uses type inference to determine the Map type from
    /// the provided key and value columns.
    ///
    /// # Example
    /// ```ignore
    /// let keys = Arc::new(ColumnString::new(Type::string()));
    /// let values = Arc::new(ColumnUInt32::new());
    /// let map = ColumnMapT::from_keys_values(keys, values);
    /// // Type is automatically inferred as Map(String, UInt32)
    /// ```
    pub fn from_keys_values(keys: Arc<K>, values: Arc<V>) -> Self {
        // Create the tuple column from keys and values
        let tuple_type = Type::Tuple {
            item_types: vec![
                keys.column_type().clone(),
                values.column_type().clone(),
            ],
        };
        let tuple = Arc::new(ColumnTuple::new(
            tuple_type.clone(),
            vec![keys as ColumnRef, values as ColumnRef],
        ));

        // Create the array column wrapping the tuple
        let array_type = Type::Array { item_type: Box::new(tuple_type) };
        let array = Arc::new(ColumnArray::from_parts(array_type, tuple));

        // Create the map type
        let map_type = Type::Map {
            key_type: Box::new(keys.column_type().clone()),
            value_type: Box::new(values.column_type().clone()),
        };

        let inner = ColumnMap::from_array(map_type, array);
        Self { inner, _phantom: PhantomData }
    }

    /// Create a new typed map column from an existing ColumnMap
    ///
    /// Returns an error if the underlying data structure doesn't match
    /// the expected key/value types.
    pub fn try_from_map(map: ColumnMap) -> Result<Self> {
        // Verify the underlying array structure
        let array = map.as_array().ok_or_else(|| {
            Error::InvalidArgument("Map data is not an array".to_string())
        })?;

        // Verify the array's nested column is a tuple
        let tuple = array
            .nested_ref()
            .as_any()
            .downcast_ref::<ColumnTuple>()
            .ok_or_else(|| {
                Error::InvalidArgument(
                    "Map data is not backed by a tuple".to_string(),
                )
            })?;

        // Verify tuple has exactly 2 columns
        if tuple.column_count() != 2 {
            return Err(Error::InvalidArgument(format!(
                "Map tuple must have 2 columns, found {}",
                tuple.column_count()
            )));
        }

        // Verify key and value column types
        let key_col = tuple.column_at(0);
        let val_col = tuple.column_at(1);

        let _ = key_col.as_any().downcast_ref::<K>().ok_or_else(|| {
            Error::InvalidArgument(format!(
                "Key column type mismatch: expected {}, found {}",
                std::any::type_name::<K>(),
                key_col.column_type().name()
            ))
        })?;

        let _ = val_col.as_any().downcast_ref::<V>().ok_or_else(|| {
            Error::InvalidArgument(format!(
                "Value column type mismatch: expected {}, found {}",
                std::any::type_name::<V>(),
                val_col.column_type().name()
            ))
        })?;

        Ok(Self { inner: map, _phantom: PhantomData })
    }

    /// Get typed reference to the underlying array column
    fn typed_array(&self) -> Result<&ColumnArrayT<ColumnTuple>> {
        let array = self.inner.as_array().ok_or_else(|| {
            Error::InvalidArgument("Map data is not an array".to_string())
        })?;

        // We know it's a ColumnArray, we just need to provide typed access
        // This is safe because we verified the structure in construction
        Ok(unsafe {
            // SAFETY: We verified the structure matches ColumnArrayT<ColumnTuple>
            // during construction in from_keys_values or try_from_map
            &*(array as *const ColumnArray as *const ColumnArrayT<ColumnTuple>)
        })
    }

    /// Get mutable typed reference to the underlying array column
    fn typed_array_mut(&mut self) -> Result<&mut ColumnArrayT<ColumnTuple>> {
        let array = Arc::get_mut(&mut self.inner.data)
            .ok_or_else(|| {
                Error::Protocol(
                    "Cannot get mutable reference to shared map data".to_string(),
                )
            })?
            .as_any_mut()
            .downcast_mut::<ColumnArray>()
            .ok_or_else(|| {
                Error::InvalidArgument("Map data is not an array".to_string())
            })?;

        Ok(unsafe {
            // SAFETY: We verified the structure matches ColumnArrayT<ColumnTuple>
            // during construction
            &mut *(array as *mut ColumnArray
                as *mut ColumnArrayT<ColumnTuple>)
        })
    }

    /// Get typed reference to the key column
    ///
    /// Returns a reference to the key column with its concrete type.
    pub fn keys(&self) -> Result<&K> {
        let array = self.inner.as_array().ok_or_else(|| {
            Error::InvalidArgument("Map data is not an array".to_string())
        })?;

        let tuple: &ColumnTuple = array.nested();
        let key_col = tuple.column_at(0);

        key_col.as_any().downcast_ref::<K>().ok_or_else(|| {
            Error::InvalidArgument(format!(
                "Failed to downcast key column to {}",
                std::any::type_name::<K>()
            ))
        })
    }

    /// Get typed reference to the value column
    ///
    /// Returns a reference to the value column with its concrete type.
    pub fn values(&self) -> Result<&V> {
        let array = self.inner.as_array().ok_or_else(|| {
            Error::InvalidArgument("Map data is not an array".to_string())
        })?;

        let tuple: &ColumnTuple = array.nested();
        let val_col = tuple.column_at(1);

        val_col.as_any().downcast_ref::<V>().ok_or_else(|| {
            Error::InvalidArgument(format!(
                "Failed to downcast value column to {}",
                std::any::type_name::<V>()
            ))
        })
    }

    /// Get map at index
    ///
    /// Returns a slice of the underlying data representing the map at the given index.
    pub fn at(&self, index: usize) -> Result<ColumnRef> {
        self.inner.at(index)
    }

    /// Get the number of maps in this column
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Check if the column is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Get reference to inner ColumnMap
    pub fn inner(&self) -> &ColumnMap {
        &self.inner
    }

    /// Get mutable reference to inner ColumnMap
    pub fn inner_mut(&mut self) -> &mut ColumnMap {
        &mut self.inner
    }

    /// Convert into inner ColumnMap
    pub fn into_inner(self) -> ColumnMap {
        self.inner
    }
}

impl<K, V> Column for ColumnMapT<K, V>
where
    K: Column + 'static,
    V: Column + 'static,
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
        Arc::new(ColumnMapT::<K, V> {
            inner: match self.inner.clone_empty().as_any().downcast_ref::<ColumnMap>() {
                Some(map) => map.clone(),
                None => ColumnMap::new(self.inner.column_type().clone()),
            },
            _phantom: PhantomData,
        })
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        let sliced_inner = self.inner.slice(begin, len)?;
        let sliced_map = sliced_inner
            .as_any()
            .downcast_ref::<ColumnMap>()
            .ok_or_else(|| {
                Error::InvalidArgument(
                    "Failed to downcast sliced column".to_string(),
                )
            })?;

        Ok(Arc::new(ColumnMapT::<K, V> {
            inner: sliced_map.clone(),
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

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::types::TypeCode;

    #[test]
    fn test_map_creation() {
        // Map(String, UInt32)
        let map_type = Type::Map {
            key_type: Box::new(Type::Simple(TypeCode::String)),
            value_type: Box::new(Type::Simple(TypeCode::UInt32)),
        };

        let col = ColumnMap::new(map_type);
        assert_eq!(col.len(), 0);
        assert!(col.is_empty());
    }

    #[test]
    fn test_map_underlying_array() {
        let map_type = Type::Map {
            key_type: Box::new(Type::Simple(TypeCode::String)),
            value_type: Box::new(Type::Simple(TypeCode::UInt32)),
        };

        let col = ColumnMap::new(map_type);
        let array = col.as_array();

        // The underlying array should be empty
        assert!(array.is_some());
        assert_eq!(array.unwrap().size(), 0);
    }

    #[test]
    fn test_map_clone() {
        let map_type = Type::Map {
            key_type: Box::new(Type::Simple(TypeCode::String)),
            value_type: Box::new(Type::Simple(TypeCode::UInt32)),
        };

        let col1 = ColumnMap::new(map_type);
        let col2 = col1.clone();

        assert_eq!(col1.len(), col2.len());
    }

    // ColumnMapT tests
    #[test]
    fn test_map_t_creation() {
        use crate::column::{
            ColumnString,
            ColumnUInt32,
        };

        let keys = Arc::new(ColumnString::new(Type::string()));
        let values = Arc::new(ColumnUInt32::new());

        let map = ColumnMapT::from_keys_values(keys, values);

        assert_eq!(map.len(), 0);
        assert!(map.is_empty());
    }

    #[test]
    fn test_map_t_keys_values() {
        use crate::column::{
            ColumnString,
            ColumnUInt32,
        };

        let keys = Arc::new(ColumnString::new(Type::string()));
        let values = Arc::new(ColumnUInt32::new());

        let map = ColumnMapT::from_keys_values(keys.clone(), values.clone());

        // Verify we can access typed keys and values
        let keys_ref = map.keys().expect("should get keys");
        let values_ref = map.values().expect("should get values");

        assert_eq!(keys_ref.size(), 0);
        assert_eq!(values_ref.size(), 0);
    }

    #[test]
    fn test_map_t_try_from_map() {
        use crate::column::{
            ColumnString,
            ColumnUInt32,
        };

        let map_type = Type::Map {
            key_type: Box::new(Type::Simple(TypeCode::String)),
            value_type: Box::new(Type::Simple(TypeCode::UInt32)),
        };

        let map = ColumnMap::new(map_type);

        // Convert to typed map
        let typed_map =
            ColumnMapT::<ColumnString, ColumnUInt32>::try_from_map(map)
                .expect("should convert to typed map");

        assert_eq!(typed_map.len(), 0);
    }

    #[test]
    fn test_map_t_type_mismatch() {
        use crate::column::{
            ColumnString,
            ColumnUInt32,
            ColumnUInt64,
        };

        let map_type = Type::Map {
            key_type: Box::new(Type::Simple(TypeCode::String)),
            value_type: Box::new(Type::Simple(TypeCode::UInt32)),
        };

        let map = ColumnMap::new(map_type);

        // Try to convert to wrong type - should fail
        let result =
            ColumnMapT::<ColumnString, ColumnUInt64>::try_from_map(map);

        assert!(result.is_err());
    }
}
