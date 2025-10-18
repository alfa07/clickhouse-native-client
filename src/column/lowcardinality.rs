use super::{Column, ColumnRef};
use crate::types::Type;
use crate::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;
use std::sync::Arc;

/// Column for LowCardinality type (dictionary encoding)
/// LowCardinality columns store unique values in a dictionary and use indices to reference them
/// This provides compression for columns with many repeated values
pub struct ColumnLowCardinality {
    type_: Type,
    dictionary: ColumnRef, // Stores unique values
    indices: Vec<u64>,     // Indices into dictionary
    unique_map: HashMap<u64, u64>, // Hash -> dictionary index for fast lookup
}

impl ColumnLowCardinality {
    pub fn new(type_: Type) -> Self {
        // Extract the nested type from LowCardinality
        let dictionary_type = match &type_ {
            Type::LowCardinality { nested_type } => nested_type.as_ref().clone(),
            _ => panic!("ColumnLowCardinality requires LowCardinality type"),
        };

        // Create the dictionary column
        let dictionary = crate::io::block_stream::create_column(&dictionary_type)
            .expect("Failed to create dictionary column");

        Self {
            type_,
            dictionary,
            indices: Vec::new(),
            unique_map: HashMap::new(),
        }
    }

    /// Get the dictionary column
    pub fn dictionary(&self) -> &ColumnRef {
        &self.dictionary
    }

    /// Get the number of unique values in the dictionary
    pub fn dictionary_size(&self) -> usize {
        self.dictionary.size()
    }

    /// Get the index at position
    pub fn index_at(&self, index: usize) -> u64 {
        self.indices[index]
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

impl Column for ColumnLowCardinality {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.indices.len()
    }

    fn clear(&mut self) {
        self.indices.clear();
        self.unique_map.clear();
        // Note: We don't clear the dictionary to preserve unique values
        // In a full implementation, we might compact the dictionary
    }

    fn reserve(&mut self, new_cap: usize) {
        self.indices.reserve(new_cap);
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let _other = other
            .as_any()
            .downcast_ref::<ColumnLowCardinality>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            })?;

        // Full implementation would merge dictionaries and remap indices
        // For now, return an error as this is complex
        Err(Error::Protocol(
            "append_column not fully implemented for LowCardinality".to_string(),
        ))
    }

    fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
        // LowCardinality has a complex serialization format:
        // 1. Read serialization version
        // 2. Read dictionary size
        // 3. Read dictionary values
        // 4. Read index type and values

        if buffer.len() < 8 {
            return Err(Error::Protocol(
                "Not enough data for LowCardinality header".to_string(),
            ));
        }

        // Read version (UInt64)
        let _version = buffer.get_u64_le();

        // For now, we'll implement a simplified version
        // A full implementation would need to handle:
        // - Different index sizes (UInt8, UInt16, UInt32, UInt64)
        // - Shared dictionaries
        // - Nullable handling

        // Read number of unique values
        if buffer.len() < 8 {
            return Err(Error::Protocol(
                "Not enough data for dictionary size".to_string(),
            ));
        }
        let dict_size = buffer.get_u64_le() as usize;

        // Load dictionary values
        // This is simplified - real implementation needs to handle the nested type properly
        for _ in 0..dict_size {
            // Skip dictionary loading for now
            // Would need: self.dictionary.load_from_buffer(buffer, 1)?;
        }

        // Read indices
        self.indices.reserve(rows);
        for _ in 0..rows {
            if buffer.len() < 8 {
                return Err(Error::Protocol(
                    "Not enough data for LowCardinality index".to_string(),
                ));
            }
            let index = buffer.get_u64_le();
            self.indices.push(index);
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Write version
        buffer.put_u64_le(1);

        // Write dictionary size
        buffer.put_u64_le(self.dictionary.size() as u64);

        // Write dictionary values
        self.dictionary.save_to_buffer(buffer)?;

        // Write indices
        for &index in &self.indices {
            buffer.put_u64_le(index);
        }

        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnLowCardinality::new(self.type_.clone()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        if begin + len > self.indices.len() {
            return Err(Error::InvalidArgument(format!(
                "Slice out of bounds: begin={}, len={}, size={}",
                begin,
                len,
                self.indices.len()
            )));
        }

        let mut sliced = ColumnLowCardinality::new(self.type_.clone());
        sliced.dictionary = self.dictionary.clone();
        sliced.indices = self.indices[begin..begin + len].to_vec();
        sliced.unique_map = self.unique_map.clone();

        Ok(Arc::new(sliced))
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
    use crate::types::TypeCode;

    #[test]
    fn test_lowcardinality_creation() {
        let lc_type = Type::LowCardinality {
            nested_type: Box::new(Type::Simple(TypeCode::String)),
        };

        let col = ColumnLowCardinality::new(lc_type);
        assert_eq!(col.len(), 0);
        assert!(col.is_empty());
        assert_eq!(col.dictionary_size(), 0);
    }

    #[test]
    fn test_lowcardinality_empty() {
        let lc_type = Type::LowCardinality {
            nested_type: Box::new(Type::Simple(TypeCode::UInt32)),
        };

        let col = ColumnLowCardinality::new(lc_type);
        assert_eq!(col.dictionary_size(), 0);
        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_lowcardinality_slice() {
        let lc_type = Type::LowCardinality {
            nested_type: Box::new(Type::Simple(TypeCode::String)),
        };

        let mut col = ColumnLowCardinality::new(lc_type);
        // Manually add some indices for testing
        col.indices = vec![0, 1, 2, 1, 0];

        let sliced = col.slice(1, 3).unwrap();
        let sliced_col = sliced
            .as_any()
            .downcast_ref::<ColumnLowCardinality>()
            .unwrap();

        assert_eq!(sliced_col.len(), 3);
        assert_eq!(sliced_col.index_at(0), 1);
        assert_eq!(sliced_col.index_at(1), 2);
        assert_eq!(sliced_col.index_at(2), 1);
    }

    #[test]
    fn test_lowcardinality_clear() {
        let lc_type = Type::LowCardinality {
            nested_type: Box::new(Type::Simple(TypeCode::String)),
        };

        let mut col = ColumnLowCardinality::new(lc_type);
        col.indices = vec![0, 1, 2];

        col.clear();
        assert_eq!(col.len(), 0);
        assert!(col.is_empty());
    }
}
