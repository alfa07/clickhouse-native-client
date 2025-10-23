//! Numeric column implementations
//!
//! **ClickHouse Documentation:**
//! - [Integer Types](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint)
//!   - Int8/16/32/64/128, UInt8/16/32/64/128
//! - [Floating-Point Types](https://clickhouse.com/docs/en/sql-reference/data-types/float)
//!   - Float32, Float64
//! - [Decimal Types](https://clickhouse.com/docs/en/sql-reference/data-types/decimal)
//!   - Fixed-point numbers
//!
//! ## Integer Types
//!
//! All integer types are stored in **little-endian** format:
//!
//! | Type | Rust Type | Storage | Min | Max |
//! |------|-----------|---------|-----|-----|
//! | `Int8` | `i8` | 1 byte | -128 | 127 |
//! | `Int16` | `i16` | 2 bytes | -32,768 | 32,767 |
//! | `Int32` | `i32` | 4 bytes | -2³¹ | 2³¹-1 |
//! | `Int64` | `i64` | 8 bytes | -2⁶³ | 2⁶³-1 |
//! | `Int128` | `i128` | 16 bytes | -2¹²⁷ | 2¹²⁷-1 |
//! | `UInt8` | `u8` | 1 byte | 0 | 255 |
//! | `UInt16` | `u16` | 2 bytes | 0 | 65,535 |
//! | `UInt32` | `u32` | 4 bytes | 0 | 2³²-1 |
//! | `UInt64` | `u64` | 8 bytes | 0 | 2⁶⁴-1 |
//! | `UInt128` | `u128` | 16 bytes | 0 | 2¹²⁸-1 |
//!
//! ## Floating-Point Types
//!
//! IEEE 754 floating-point numbers, stored in little-endian:
//! - `Float32` - Single precision (32-bit)
//! - `Float64` - Double precision (64-bit)
//!
//! ## Bool Type
//!
//! `Bool` is an alias for `UInt8` where 0 = false, 1 = true.

use super::{
    Column,
    ColumnRef,
    ColumnTyped,
};
use crate::{
    types::{
        ToType,
        Type,
    },
    Error,
    Result,
};
use bytes::{
    Buf,
    BufMut,
    BytesMut,
};
use std::sync::Arc;

/// Trait for types that can be read/written as fixed-size values (synchronous
/// version for columns)
///
/// All numeric types are stored in **little-endian** format in ClickHouse wire
/// protocol.
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
                    return Err(Error::Protocol(
                        "Buffer underflow".to_string(),
                    ));
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
    /// Create a new column with explicit type (backward compatible)
    pub fn with_type(type_: Type) -> Self {
        Self { type_, data: Vec::new() }
    }

    /// Create a new column with explicit type and capacity (backward
    /// compatible)
    pub fn with_type_and_capacity(type_: Type, capacity: usize) -> Self {
        Self { type_, data: Vec::with_capacity(capacity) }
    }

    pub fn from_vec(type_: Type, data: Vec<T>) -> Self {
        Self { type_, data }
    }

    /// Create a column with initial data (builder pattern)
    pub fn with_data(mut self, data: Vec<T>) -> Self {
        self.data = data;
        self
    }

    /// Reserve capacity for additional elements (for
    /// benchmarking/optimization)
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    /// Clear the column while preserving capacity (for
    /// benchmarking/optimization)
    pub fn clear(&mut self) {
        self.data.clear();
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

/// Type-inferred constructors for ColumnVector
/// Implements the type map pattern from C++ Type::CreateSimple<T>()
impl<T: FixedSize + ToType + Clone + Send + Sync + 'static> ColumnVector<T> {
    /// Create a new column with type inferred from T
    /// Equivalent to C++ pattern where type is determined from template
    /// parameter
    ///
    /// # Examples
    ///
    /// ```
    /// use clickhouse_client::column::ColumnVector;
    /// use clickhouse_client::types::Type;
    ///
    /// let col = ColumnVector::<i32>::new();
    /// assert_eq!(col.column_type(), &Type::int32());
    /// ```
    pub fn new() -> Self {
        Self { type_: T::to_type(), data: Vec::new() }
    }

    /// Create a new column with type inferred from T and specified capacity
    ///
    /// # Examples
    ///
    /// ```
    /// use clickhouse_client::column::ColumnVector;
    ///
    /// let col = ColumnVector::<u64>::with_capacity(100);
    /// assert_eq!(col.len(), 0);
    /// ```
    pub fn with_capacity(capacity: usize) -> Self {
        Self { type_: T::to_type(), data: Vec::with_capacity(capacity) }
    }
}

impl<T: FixedSize + ToType + Clone + Send + Sync + 'static> Default
    for ColumnVector<T>
{
    fn default() -> Self {
        Self::new()
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

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        // Optimize: Use bulk read instead of loop for massive performance gain
        // C++ does: WireFormat::ReadBytes(*input, data_.data(), rows *
        // sizeof(T))
        let bytes_needed = rows * std::mem::size_of::<T>();

        if buffer.len() < bytes_needed {
            return Err(Error::Protocol(format!(
                "Buffer underflow: need {} bytes, have {}",
                bytes_needed,
                buffer.len()
            )));
        }

        // Pre-allocate and read directly into Vec's memory
        self.data.clear();
        self.data.reserve(rows);

        unsafe {
            // Read bytes directly into Vec's uninitialized memory
            let dest_ptr = self.data.as_mut_ptr() as *mut u8;
            std::ptr::copy_nonoverlapping(
                buffer.as_ptr(),
                dest_ptr,
                bytes_needed,
            );
            self.data.set_len(rows);
        }

        // Advance buffer
        *buffer = &buffer[bytes_needed..];
        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Optimize: Use bulk write instead of loop for massive performance
        // gain C++ does: WireFormat::WriteBytes(*output, data_.data(),
        // data_.size() * sizeof(T)) This achieves the same with
        // extend_from_slice on the raw bytes
        if !self.data.is_empty() {
            let byte_slice = unsafe {
                std::slice::from_raw_parts(
                    self.data.as_ptr() as *const u8,
                    self.data.len() * std::mem::size_of::<T>(),
                )
            };
            buffer.extend_from_slice(byte_slice);
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnVector::<T>::with_type(self.type_.clone()))
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

impl<T: FixedSize + Clone + Send + Sync + 'static> ColumnTyped<T>
    for ColumnVector<T>
{
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
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::types::Type;

    #[test]
    fn test_column_creation() {
        // Test type-inferred constructor
        let col = ColumnUInt32::new();
        assert_eq!(col.size(), 0);
        assert_eq!(col.column_type().name(), "UInt32");

        // Test explicit type constructor
        let col2 = ColumnUInt32::with_type(Type::uint32());
        assert_eq!(col2.size(), 0);
        assert_eq!(col2.column_type().name(), "UInt32");
    }

    #[test]
    fn test_column_append() {
        let mut col = ColumnUInt32::new();
        col.append(42);
        col.append(100);

        assert_eq!(col.size(), 2);
        assert_eq!(col.get(0), Some(&42));
        assert_eq!(col.get(1), Some(&100));
    }

    #[test]
    fn test_column_clear() {
        let mut col = ColumnInt64::new();
        col.append(-123);
        col.append(456);
        assert_eq!(col.size(), 2);

        col.clear();
        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_column_slice() {
        let mut col = ColumnUInt64::new();
        for i in 0..10 {
            col.append(i);
        }

        let sliced = col.slice(2, 5).unwrap();
        assert_eq!(sliced.size(), 5);

        let sliced_concrete =
            sliced.as_any().downcast_ref::<ColumnUInt64>().unwrap();
        assert_eq!(sliced_concrete.get(0), Some(&2));
        assert_eq!(sliced_concrete.get(4), Some(&6));
    }

    #[test]
    fn test_column_save_load() {
        let mut col = ColumnInt32::new();
        col.append(1);
        col.append(-2);
        col.append(3);

        // Save to buffer
        let mut buf = BytesMut::new();
        col.save_to_buffer(&mut buf).unwrap();

        // Load from buffer
        let mut col2 = ColumnInt32::new();
        let mut reader = &buf[..];
        col2.load_from_buffer(&mut reader, 3).unwrap();

        assert_eq!(col2.size(), 3);
        assert_eq!(col2.get(0), Some(&1));
        assert_eq!(col2.get(1), Some(&-2));
        assert_eq!(col2.get(2), Some(&3));
    }

    #[test]
    fn test_column_append_column() {
        let mut col1 = ColumnFloat64::new();
        col1.append(1.5);
        col1.append(2.5);

        let mut col2 = ColumnFloat64::new();
        col2.append(3.5);
        col2.append(4.5);

        col1.append_column(Arc::new(col2)).unwrap();

        assert_eq!(col1.size(), 4);
        assert_eq!(col1.get(0), Some(&1.5));
        assert_eq!(col1.get(3), Some(&4.5));
    }

    // Bulk copy tests - verify set_len safety
    #[test]
    fn test_bulk_load_large_dataset() {
        // Test with 10,000 elements to ensure bulk copy works correctly
        let mut col = ColumnUInt64::new();
        let data: Vec<u64> = (0..10_000).collect();

        // Save to buffer
        let mut buf = BytesMut::new();
        for &val in &data {
            buf.put_u64_le(val);
        }

        // Load from buffer using bulk copy (internally uses set_len)
        let mut reader = &buf[..];
        col.load_from_buffer(&mut reader, 10_000).unwrap();

        assert_eq!(col.size(), 10_000);
        assert_eq!(col.get(0), Some(&0));
        assert_eq!(col.get(5_000), Some(&5_000));
        assert_eq!(col.get(9_999), Some(&9_999));
    }

    #[test]
    fn test_bulk_load_multiple_sequential() {
        // Test multiple sequential bulk loads
        let mut col = ColumnInt32::new();

        // First bulk load
        let mut buf1 = BytesMut::new();
        for i in 0..1_000 {
            buf1.put_i32_le(i);
        }
        let mut reader1 = &buf1[..];
        col.load_from_buffer(&mut reader1, 1_000).unwrap();

        assert_eq!(col.size(), 1_000);
        assert_eq!(col.get(0), Some(&0));
        assert_eq!(col.get(999), Some(&999));

        // Second bulk load (should append)
        let mut buf2 = BytesMut::new();
        for i in 1_000..2_000 {
            buf2.put_i32_le(i);
        }
        let mut reader2 = &buf2[..];
        col.load_from_buffer(&mut reader2, 1_000).unwrap();

        assert_eq!(col.size(), 1_000); // Note: load_from_buffer clears first
        assert_eq!(col.get(0), Some(&1_000));
        assert_eq!(col.get(999), Some(&1_999));
    }

    #[test]
    fn test_bulk_load_empty() {
        // Test edge case: empty load
        let mut col = ColumnUInt32::new();
        let buf = BytesMut::new();
        let mut reader = &buf[..];
        col.load_from_buffer(&mut reader, 0).unwrap();

        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_bulk_load_single_element() {
        // Test edge case: single element
        let mut col = ColumnInt64::new();
        let mut buf = BytesMut::new();
        buf.put_i64_le(42);

        let mut reader = &buf[..];
        col.load_from_buffer(&mut reader, 1).unwrap();

        assert_eq!(col.size(), 1);
        assert_eq!(col.get(0), Some(&42));
    }

    #[test]
    fn test_bulk_load_roundtrip_large() {
        // Test save/load round-trip with large data
        let mut col1 = ColumnFloat32::new();
        for i in 0..5_000 {
            col1.append(i as f32 * 1.5);
        }

        // Save to buffer
        let mut buf = BytesMut::new();
        col1.save_to_buffer(&mut buf).unwrap();

        // Load from buffer
        let mut col2 = ColumnFloat32::new();
        let mut reader = &buf[..];
        col2.load_from_buffer(&mut reader, 5_000).unwrap();

        assert_eq!(col2.size(), 5_000);
        for i in 0..5_000 {
            assert_eq!(col2.get(i), Some(&(i as f32 * 1.5)));
        }
    }

    #[test]
    fn test_bulk_load_all_numeric_types() {
        // Test bulk load for all numeric types to ensure set_len works
        // correctly

        // UInt8
        let mut col_u8 = ColumnUInt8::new();
        let mut buf = BytesMut::new();
        for i in 0..255u8 {
            buf.put_u8(i);
        }
        let mut reader = &buf[..];
        col_u8.load_from_buffer(&mut reader, 255).unwrap();
        assert_eq!(col_u8.size(), 255);

        // UInt16
        let mut col_u16 = ColumnUInt16::new();
        let mut buf = BytesMut::new();
        for i in 0..1000u16 {
            buf.put_u16_le(i);
        }
        let mut reader = &buf[..];
        col_u16.load_from_buffer(&mut reader, 1000).unwrap();
        assert_eq!(col_u16.size(), 1000);

        // Int8
        let mut col_i8 = ColumnInt8::new();
        let mut buf = BytesMut::new();
        for i in -127..127i8 {
            buf.put_i8(i);
        }
        let mut reader = &buf[..];
        col_i8.load_from_buffer(&mut reader, 254).unwrap();
        assert_eq!(col_i8.size(), 254);

        // Int16
        let mut col_i16 = ColumnInt16::new();
        let mut buf = BytesMut::new();
        for i in 0..1000i16 {
            buf.put_i16_le(i);
        }
        let mut reader = &buf[..];
        col_i16.load_from_buffer(&mut reader, 1000).unwrap();
        assert_eq!(col_i16.size(), 1000);

        // i128 and u128
        let mut col_i128 = ColumnInt128::new();
        let mut buf = BytesMut::new();
        for i in 0..100i128 {
            buf.put_i128_le(i);
        }
        let mut reader = &buf[..];
        col_i128.load_from_buffer(&mut reader, 100).unwrap();
        assert_eq!(col_i128.size(), 100);

        let mut col_u128 = ColumnUInt128::new();
        let mut buf = BytesMut::new();
        for i in 0..100u128 {
            buf.put_u128_le(i);
        }
        let mut reader = &buf[..];
        col_u128.load_from_buffer(&mut reader, 100).unwrap();
        assert_eq!(col_u128.size(), 100);
    }

    #[test]
    fn test_bulk_load_memory_safety() {
        // This test specifically validates that set_len is called AFTER memory
        // initialization If set_len was called before
        // copy_nonoverlapping, this would fail or cause UB
        let mut col = ColumnInt32::new();

        // Create test data with specific pattern
        let mut buf = BytesMut::new();
        let test_values: Vec<i32> =
            vec![i32::MIN, -1_000_000, -1, 0, 1, 1_000_000, i32::MAX];
        for &val in &test_values {
            buf.put_i32_le(val);
        }

        // Load using bulk copy
        let mut reader = &buf[..];
        col.load_from_buffer(&mut reader, test_values.len()).unwrap();

        // Verify all values are correctly initialized
        assert_eq!(col.size(), test_values.len());
        for (i, &expected) in test_values.iter().enumerate() {
            assert_eq!(
                col.get(i),
                Some(&expected),
                "Value mismatch at index {}",
                i
            );
        }
    }
}
