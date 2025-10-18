use super::{Column, ColumnRef, ColumnTyped};
use crate::types::Type;
use crate::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::sync::Arc;

/// Trait for types that can be read/written as fixed-size values (synchronous version for columns)
pub trait FixedSize: Sized + Clone + Send + Sync + 'static {
    fn read_from(buffer: &mut &[u8]) -> Result<Self>;
    fn write_to(&self, buffer: &mut BytesMut);
}

// Implement FixedSize for primitive types
macro_rules! impl_fixed_size {
    ($type:ty, $get:ident, $put:ident) => {
        impl FixedSize for $type {
            fn read_from(buffer: &mut &[u8]) -> Result<Self> {
                if buffer.len() < std::mem::size_of::<$type>() {
                    return Err(Error::Protocol("Buffer underflow".to_string()));
                }
                Ok(buffer.$get())
            }

            fn write_to(&self, buffer: &mut BytesMut) {
                buffer.$put(*self);
            }
        }
    };
}

impl_fixed_size!(u8, get_u8, put_u8);
impl_fixed_size!(u16, get_u16_le, put_u16_le);
impl_fixed_size!(u32, get_u32_le, put_u32_le);
impl_fixed_size!(u64, get_u64_le, put_u64_le);
impl_fixed_size!(i8, get_i8, put_i8);
impl_fixed_size!(i16, get_i16_le, put_i16_le);
impl_fixed_size!(i32, get_i32_le, put_i32_le);
impl_fixed_size!(i64, get_i64_le, put_i64_le);
impl_fixed_size!(f32, get_f32_le, put_f32_le);
impl_fixed_size!(f64, get_f64_le, put_f64_le);

impl FixedSize for i128 {
    fn read_from(buffer: &mut &[u8]) -> Result<Self> {
        if buffer.len() < 16 {
            return Err(Error::Protocol("Buffer underflow".to_string()));
        }
        Ok(buffer.get_i128_le())
    }

    fn write_to(&self, buffer: &mut BytesMut) {
        buffer.put_i128_le(*self);
    }
}

impl FixedSize for u128 {
    fn read_from(buffer: &mut &[u8]) -> Result<Self> {
        if buffer.len() < 16 {
            return Err(Error::Protocol("Buffer underflow".to_string()));
        }
        Ok(buffer.get_u128_le())
    }

    fn write_to(&self, buffer: &mut BytesMut) {
        buffer.put_u128_le(*self);
    }
}

/// Generic column for numeric types
pub struct ColumnVector<T: FixedSize> {
    type_: Type,
    data: Vec<T>,
}

impl<T: FixedSize + Clone + Send + Sync + 'static> ColumnVector<T> {
    pub fn new(type_: Type) -> Self {
        Self {
            type_,
            data: Vec::new(),
        }
    }

    pub fn with_capacity(type_: Type, capacity: usize) -> Self {
        Self {
            type_,
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn from_vec(type_: Type, data: Vec<T>) -> Self {
        Self { type_, data }
    }

    /// Create a column with initial data (builder pattern)
    pub fn with_data(mut self, data: Vec<T>) -> Self {
        self.data = data;
        self
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index)
    }

    /// Get value at index (panics if out of bounds - for tests)
    pub fn at(&self, index: usize) -> T {
        self.data[index].clone()
    }

    /// Get the number of elements (alias for size())
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the column is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn append(&mut self, value: T) {
        self.data.push(value);
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.data.iter()
    }

    pub fn data(&self) -> &[T] {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut Vec<T> {
        &mut self.data
    }
}

impl<T: FixedSize> Column for ColumnVector<T> {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.data.len()
    }

    fn clear(&mut self) {
        self.data.clear()
    }

    fn reserve(&mut self, new_cap: usize) {
        self.data.reserve(new_cap);
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<ColumnVector<T>>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            })?;

        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
        self.data.reserve(rows);
        for _ in 0..rows {
            let value = T::read_from(buffer)?;
            self.data.push(value);
        }
        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for value in &self.data {
            value.write_to(buffer);
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnVector::<T>::new(self.type_.clone()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        if begin + len > self.data.len() {
            return Err(Error::InvalidArgument(format!(
                "Slice range out of bounds: begin={}, len={}, size={}",
                begin,
                len,
                self.data.len()
            )));
        }

        let sliced_data = self.data[begin..begin + len].to_vec();
        Ok(Arc::new(ColumnVector::<T>::from_vec(
            self.type_.clone(),
            sliced_data,
        )))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl<T: FixedSize + Clone + Send + Sync + 'static> ColumnTyped<T> for ColumnVector<T> {
    fn get(&self, index: usize) -> Option<T> {
        self.data.get(index).cloned()
    }

    fn append(&mut self, value: T) {
        self.data.push(value);
    }
}

// Type aliases for common numeric columns
pub type ColumnUInt8 = ColumnVector<u8>;
pub type ColumnUInt16 = ColumnVector<u16>;
pub type ColumnUInt32 = ColumnVector<u32>;
pub type ColumnUInt64 = ColumnVector<u64>;
pub type ColumnUInt128 = ColumnVector<u128>;

pub type ColumnInt8 = ColumnVector<i8>;
pub type ColumnInt16 = ColumnVector<i16>;
pub type ColumnInt32 = ColumnVector<i32>;
pub type ColumnInt64 = ColumnVector<i64>;
pub type ColumnInt128 = ColumnVector<i128>;

pub type ColumnFloat32 = ColumnVector<f32>;
pub type ColumnFloat64 = ColumnVector<f64>;

// Date is stored as UInt16 (days since epoch)
pub type ColumnDate = ColumnVector<u16>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Type;

    #[test]
    fn test_column_creation() {
        let col = ColumnUInt32::new(Type::uint32());
        assert_eq!(col.size(), 0);
        assert_eq!(col.column_type().name(), "UInt32");
    }

    #[test]
    fn test_column_append() {
        let mut col = ColumnUInt32::new(Type::uint32());
        col.append(42);
        col.append(100);

        assert_eq!(col.size(), 2);
        assert_eq!(col.get(0), Some(&42));
        assert_eq!(col.get(1), Some(&100));
    }

    #[test]
    fn test_column_clear() {
        let mut col = ColumnInt64::new(Type::int64());
        col.append(-123);
        col.append(456);
        assert_eq!(col.size(), 2);

        col.clear();
        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_column_slice() {
        let mut col = ColumnUInt64::new(Type::uint64());
        for i in 0..10 {
            col.append(i);
        }

        let sliced = col.slice(2, 5).unwrap();
        assert_eq!(sliced.size(), 5);

        let sliced_concrete = sliced.as_any().downcast_ref::<ColumnUInt64>().unwrap();
        assert_eq!(sliced_concrete.get(0), Some(&2));
        assert_eq!(sliced_concrete.get(4), Some(&6));
    }

    #[test]
    fn test_column_save_load() {
        let mut col = ColumnInt32::new(Type::int32());
        col.append(1);
        col.append(-2);
        col.append(3);

        // Save to buffer
        let mut buf = BytesMut::new();
        col.save_to_buffer(&mut buf).unwrap();

        // Load from buffer
        let mut col2 = ColumnInt32::new(Type::int32());
        let mut reader = &buf[..];
        col2.load_from_buffer(&mut reader, 3).unwrap();

        assert_eq!(col2.size(), 3);
        assert_eq!(col2.get(0), Some(&1));
        assert_eq!(col2.get(1), Some(&-2));
        assert_eq!(col2.get(2), Some(&3));
    }

    #[test]
    fn test_column_append_column() {
        let mut col1 = ColumnFloat64::new(Type::float64());
        col1.append(1.5);
        col1.append(2.5);

        let mut col2 = ColumnFloat64::new(Type::float64());
        col2.append(3.5);
        col2.append(4.5);

        col1.append_column(Arc::new(col2)).unwrap();

        assert_eq!(col1.size(), 4);
        assert_eq!(col1.get(0), Some(&1.5));
        assert_eq!(col1.get(3), Some(&4.5));
    }
}
