use super::{
    Column,
    ColumnArray,
    ColumnRef,
};
use crate::{
    types::Type,
    Error,
    Result,
};
use bytes::BytesMut;
use std::sync::Arc;

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

    /// Get the underlying array column as ColumnRef
    pub fn data(&self) -> &ColumnRef {
        &self.data
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

#[cfg(test)]
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
}
