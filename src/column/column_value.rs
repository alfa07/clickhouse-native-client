//! ColumnValue - A value extracted from or to be inserted into a column
//!
//! This is similar to C++ clickhouse-cpp's ItemView, providing a type-tagged
//! byte representation of column values.

use crate::{
    types::TypeCode,
    Error,
    Result,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{
    Hash,
    Hasher,
};
use std::sync::Arc;

/// A value from a column, stored as bytes with type information
/// Similar to C++ ItemView but owned
#[derive(Clone, Debug)]
pub struct ColumnValue {
    pub type_code: TypeCode,
    pub data: Vec<u8>,
}

impl ColumnValue {
    /// Create from primitive types
    pub fn from_u8(value: u8) -> Self {
        Self {
            type_code: TypeCode::UInt8,
            data: value.to_le_bytes().to_vec(),
        }
    }

    pub fn from_u16(value: u16) -> Self {
        Self {
            type_code: TypeCode::UInt16,
            data: value.to_le_bytes().to_vec(),
        }
    }

    pub fn from_u32(value: u32) -> Self {
        Self {
            type_code: TypeCode::UInt32,
            data: value.to_le_bytes().to_vec(),
        }
    }

    pub fn from_u64(value: u64) -> Self {
        Self {
            type_code: TypeCode::UInt64,
            data: value.to_le_bytes().to_vec(),
        }
    }

    pub fn from_i8(value: i8) -> Self {
        Self {
            type_code: TypeCode::Int8,
            data: value.to_le_bytes().to_vec(),
        }
    }

    pub fn from_i16(value: i16) -> Self {
        Self {
            type_code: TypeCode::Int16,
            data: value.to_le_bytes().to_vec(),
        }
    }

    pub fn from_i32(value: i32) -> Self {
        Self {
            type_code: TypeCode::Int32,
            data: value.to_le_bytes().to_vec(),
        }
    }

    pub fn from_i64(value: i64) -> Self {
        Self {
            type_code: TypeCode::Int64,
            data: value.to_le_bytes().to_vec(),
        }
    }

    pub fn from_f32(value: f32) -> Self {
        Self {
            type_code: TypeCode::Float32,
            data: value.to_le_bytes().to_vec(),
        }
    }

    pub fn from_f64(value: f64) -> Self {
        Self {
            type_code: TypeCode::Float64,
            data: value.to_le_bytes().to_vec(),
        }
    }

    pub fn from_string(value: &str) -> Self {
        Self {
            type_code: TypeCode::String,
            data: value.as_bytes().to_vec(),
        }
    }

    pub fn void() -> Self {
        Self {
            type_code: TypeCode::Void,
            data: Vec::new(),
        }
    }

    /// Get as string (for String type)
    pub fn as_string(&self) -> Result<&str> {
        if self.type_code != TypeCode::String {
            return Err(Error::TypeMismatch {
                expected: "String".to_string(),
                actual: format!("{:?}", self.type_code),
            });
        }
        std::str::from_utf8(&self.data).map_err(|e| {
            Error::Protocol(format!("Invalid UTF-8 in string: {}", e))
        })
    }

    /// Get raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

/// Hash computation for LowCardinality deduplication
/// Matches C++ computeHashKey using dual hashing
pub fn compute_hash_key(value: &ColumnValue) -> (u64, u64) {
    // Void type gets special (0, 0) hash to distinguish NULL from empty string
    if value.type_code == TypeCode::Void {
        return (0, 0);
    }

    // Hash 1: std::hash equivalent
    let mut hasher = DefaultHasher::new();
    value.data.hash(&mut hasher);
    let hash1 = hasher.finish();

    // Hash 2: CityHash64 equivalent (using simple FNV-1a for now)
    let hash2 = fnv1a_64(&value.data);

    (hash1, hash2)
}

/// Simple FNV-1a hash (64-bit)
/// This is a placeholder - ideally we'd use actual CityHash64
/// FNV-1a is simple, fast, and has good distribution
fn fnv1a_64(data: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Helper functions to extract ColumnValue from specific column types
use super::{
    nullable::ColumnNullable,
    numeric::*,
    string::ColumnString,
    Column,
};

/// Get item from a column by index
/// Returns ColumnValue representation
pub fn get_column_item(column: &dyn Column, index: usize) -> Result<ColumnValue> {
    use crate::types::Type;

    if index >= column.size() {
        return Err(Error::InvalidArgument(format!(
            "Index {} out of bounds (size: {})",
            index,
            column.size()
        )));
    }

    match column.column_type() {
        Type::Simple(type_code) => {
            match type_code {
                TypeCode::UInt8 => {
                    if let Some(col) = column.as_any().downcast_ref::<ColumnUInt8>() {
                        Ok(ColumnValue::from_u8(col.at(index)))
                    } else {
                        Err(Error::Protocol("Failed to downcast UInt8 column".to_string()))
                    }
                }
                TypeCode::UInt16 => {
                    if let Some(col) = column.as_any().downcast_ref::<ColumnUInt16>() {
                        Ok(ColumnValue::from_u16(col.at(index)))
                    } else {
                        Err(Error::Protocol("Failed to downcast UInt16 column".to_string()))
                    }
                }
                TypeCode::UInt32 => {
                    if let Some(col) = column.as_any().downcast_ref::<ColumnUInt32>() {
                        Ok(ColumnValue::from_u32(col.at(index)))
                    } else {
                        Err(Error::Protocol("Failed to downcast UInt32 column".to_string()))
                    }
                }
                TypeCode::UInt64 => {
                    if let Some(col) = column.as_any().downcast_ref::<ColumnUInt64>() {
                        Ok(ColumnValue::from_u64(col.at(index)))
                    } else {
                        Err(Error::Protocol("Failed to downcast UInt64 column".to_string()))
                    }
                }
                TypeCode::Int8 => {
                    if let Some(col) = column.as_any().downcast_ref::<ColumnInt8>() {
                        Ok(ColumnValue::from_i8(col.at(index)))
                    } else {
                        Err(Error::Protocol("Failed to downcast Int8 column".to_string()))
                    }
                }
                TypeCode::Int16 => {
                    if let Some(col) = column.as_any().downcast_ref::<ColumnInt16>() {
                        Ok(ColumnValue::from_i16(col.at(index)))
                    } else {
                        Err(Error::Protocol("Failed to downcast Int16 column".to_string()))
                    }
                }
                TypeCode::Int32 => {
                    if let Some(col) = column.as_any().downcast_ref::<ColumnInt32>() {
                        Ok(ColumnValue::from_i32(col.at(index)))
                    } else {
                        Err(Error::Protocol("Failed to downcast Int32 column".to_string()))
                    }
                }
                TypeCode::Int64 => {
                    if let Some(col) = column.as_any().downcast_ref::<ColumnInt64>() {
                        Ok(ColumnValue::from_i64(col.at(index)))
                    } else {
                        Err(Error::Protocol("Failed to downcast Int64 column".to_string()))
                    }
                }
                TypeCode::Float32 => {
                    if let Some(col) = column.as_any().downcast_ref::<ColumnFloat32>() {
                        Ok(ColumnValue::from_f32(col.at(index)))
                    } else {
                        Err(Error::Protocol("Failed to downcast Float32 column".to_string()))
                    }
                }
                TypeCode::Float64 => {
                    if let Some(col) = column.as_any().downcast_ref::<ColumnFloat64>() {
                        Ok(ColumnValue::from_f64(col.at(index)))
                    } else {
                        Err(Error::Protocol("Failed to downcast Float64 column".to_string()))
                    }
                }
                TypeCode::String => {
                    if let Some(col) = column.as_any().downcast_ref::<ColumnString>() {
                        Ok(ColumnValue::from_string(&col.at(index)))
                    } else {
                        Err(Error::Protocol("Failed to downcast String column".to_string()))
                    }
                }
                _ => Err(Error::Protocol(format!(
                    "get_column_item not implemented for type {:?}",
                    type_code
                ))),
            }
        }
        Type::Nullable { nested_type: _ } => {
            if let Some(col) = column.as_any().downcast_ref::<ColumnNullable>() {
                if col.is_null(index) {
                    Ok(ColumnValue::void())
                } else {
                    get_column_item(col.nested().as_ref(), index)
                }
            } else {
                Err(Error::Protocol("Failed to downcast Nullable column".to_string()))
            }
        }
        _ => Err(Error::Protocol(format!(
            "get_column_item not implemented for type {}",
            column.column_type().name()
        ))),
    }
}

/// Append item to a column
pub fn append_column_item(column: &mut dyn Column, value: &ColumnValue) -> Result<()> {
    use crate::types::Type;

    match column.column_type() {
        Type::Simple(type_code) => {
            if *type_code != value.type_code {
                return Err(Error::TypeMismatch {
                    expected: format!("{:?}", type_code),
                    actual: format!("{:?}", value.type_code),
                });
            }

            match type_code {
                TypeCode::String => {
                    if let Some(col) = column.as_any_mut().downcast_mut::<ColumnString>() {
                        col.append(value.as_string()?);
                        Ok(())
                    } else {
                        Err(Error::Protocol("Failed to downcast String column".to_string()))
                    }
                }
                TypeCode::UInt8 => {
                    if let Some(col) = column.as_any_mut().downcast_mut::<ColumnUInt8>() {
                        let val = u8::from_le_bytes(value.data.as_slice().try_into().map_err(|_| {
                            Error::Protocol("Invalid UInt8 data".to_string())
                        })?);
                        col.append(val);
                        Ok(())
                    } else {
                        Err(Error::Protocol("Failed to downcast UInt8 column".to_string()))
                    }
                }
                TypeCode::UInt64 => {
                    if let Some(col) = column.as_any_mut().downcast_mut::<ColumnUInt64>() {
                        let val = u64::from_le_bytes(value.data.as_slice().try_into().map_err(|_| {
                            Error::Protocol("Invalid UInt64 data".to_string())
                        })?);
                        col.append(val);
                        Ok(())
                    } else {
                        Err(Error::Protocol("Failed to downcast UInt64 column".to_string()))
                    }
                }
                // Add more types as needed
                _ => Err(Error::Protocol(format!(
                    "append_column_item not implemented for type {:?}",
                    type_code
                ))),
            }
        }
        Type::Nullable { .. } => {
            if let Some(col) = column.as_any_mut().downcast_mut::<ColumnNullable>() {
                if value.type_code == TypeCode::Void {
                    col.append_null();
                    Ok(())
                } else {
                    // Get mutable access to the nested Arc<dyn Column>
                    let nested_arc = col.nested_mut();
                    let nested_mut = Arc::get_mut(nested_arc).ok_or_else(|| {
                        Error::Protocol(
                            "Cannot append to shared nullable column - column has multiple references"
                                .to_string(),
                        )
                    })?;
                    append_column_item(nested_mut, value)?;
                    col.append_non_null();
                    Ok(())
                }
            } else {
                Err(Error::Protocol("Failed to downcast Nullable column".to_string()))
            }
        }
        _ => Err(Error::Protocol(format!(
            "append_column_item not implemented for type {}",
            column.column_type().name()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_value_primitives() {
        let v = ColumnValue::from_u64(42);
        assert_eq!(v.type_code, TypeCode::UInt64);
        assert_eq!(v.data, 42u64.to_le_bytes());

        let s = ColumnValue::from_string("hello");
        assert_eq!(s.type_code, TypeCode::String);
        assert_eq!(s.as_string().unwrap(), "hello");
    }

    #[test]
    fn test_hash_computation() {
        let v1 = ColumnValue::from_string("test");
        let v2 = ColumnValue::from_string("test");
        let v3 = ColumnValue::from_string("different");

        let h1 = compute_hash_key(&v1);
        let h2 = compute_hash_key(&v2);
        let h3 = compute_hash_key(&v3);

        // Same values should have same hash
        assert_eq!(h1, h2);
        // Different values should (likely) have different hash
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_void_hash() {
        let void = ColumnValue::void();
        let hash = compute_hash_key(&void);
        assert_eq!(hash, (0, 0));
    }
}
