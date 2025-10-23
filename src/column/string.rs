//! String column implementations
//!
//! **ClickHouse Documentation:**
//! - [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)
//!   - Variable-length UTF-8 strings
//! - [FixedString](https://clickhouse.com/docs/en/sql-reference/data-types/fixedstring)
//!   - Fixed-length binary strings
//!
//! ## String Type
//!
//! Variable-length UTF-8 strings. Each string is prefixed with its length
//! (varint encoded).
//!
//! **Wire Format:**
//! ```text
//! For each string: [length:varint][bytes:UInt8 * length]
//! ```
//!
//! ## FixedString Type
//!
//! Fixed-length binary strings, zero-padded if shorter than the specified
//! size. Useful for storing UUIDs, hashes, or other fixed-size binary data.
//!
//! **Wire Format:**
//! ```text
//! [bytes:UInt8 * N]  // N is the FixedString size
//! ```

use super::{
    Column,
    ColumnRef,
};
use crate::{
    io::buffer_utils,
    types::Type,
    Error,
    Result,
};
use bytes::{
    Buf,
    BufMut,
    BytesMut,
};
use std::sync::Arc;

/// Column for fixed-length strings (all strings padded to same length)
///
/// Stores binary data of exactly `N` bytes per element, zero-padded if needed.
///
/// **ClickHouse Reference:** <https://clickhouse.com/docs/en/sql-reference/data-types/fixedstring>
pub struct ColumnFixedString {
    type_: Type,
    string_size: usize,
    data: Vec<u8>,
}

impl ColumnFixedString {
    pub fn new(type_: Type) -> Self {
        let string_size = match &type_ {
            Type::FixedString { size } => *size,
            _ => panic!("Expected FixedString type"),
        };

        Self { type_, string_size, data: Vec::new() }
    }

    pub fn with_capacity(type_: Type, capacity: usize) -> Self {
        let string_size = match &type_ {
            Type::FixedString { size } => *size,
            _ => panic!("Expected FixedString type"),
        };

        Self {
            type_,
            string_size,
            data: Vec::with_capacity(string_size * capacity),
        }
    }

    /// Create a column with initial data (builder pattern)
    pub fn with_data(mut self, data: Vec<String>) -> Self {
        for s in data {
            self.append(s);
        }
        self
    }

    pub fn append(&mut self, s: String) {
        let bytes = s.as_bytes();

        if bytes.len() > self.string_size {
            panic!(
                "String too long for FixedString({}): got {} bytes",
                self.string_size,
                bytes.len()
            );
        }

        // Append the string data
        self.data.extend_from_slice(bytes);

        // Pad with zeros if needed
        if bytes.len() < self.string_size {
            self.data
                .resize(self.data.len() + (self.string_size - bytes.len()), 0);
        }
    }

    pub fn get(&self, index: usize) -> Option<String> {
        if index >= self.size() {
            return None;
        }

        let start = index * self.string_size;
        let end = start + self.string_size;
        let bytes = &self.data[start..end];

        // Trim null bytes from the end
        let trimmed =
            bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        Some(String::from_utf8_lossy(&bytes[..trimmed]).to_string())
    }

    /// Get value at index (for tests)
    pub fn at(&self, index: usize) -> String {
        self.get(index).unwrap()
    }

    /// Get the number of elements (alias for size())
    pub fn len(&self) -> usize {
        self.size()
    }

    /// Check if the column is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn fixed_size(&self) -> usize {
        self.string_size
    }
}

impl Column for ColumnFixedString {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.data.len() / self.string_size
    }

    fn clear(&mut self) {
        self.data.clear();
    }

    fn reserve(&mut self, new_cap: usize) {
        self.data.reserve(self.string_size * new_cap);
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<ColumnFixedString>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            })?;

        if self.string_size != other.string_size {
            return Err(Error::TypeMismatch {
                expected: format!("FixedString({})", self.string_size),
                actual: format!("FixedString({})", other.string_size),
            });
        }

        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        let total_bytes = self.string_size * rows;

        if buffer.len() < total_bytes {
            return Err(Error::Protocol(format!(
                "Not enough data for {} FixedString({}) values: need {}, have {}",
                rows, self.string_size, total_bytes, buffer.len()
            )));
        }

        self.data.extend_from_slice(&buffer[..total_bytes]);
        buffer.advance(total_bytes);
        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        buffer.put_slice(&self.data);
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnFixedString::new(self.type_.clone()))
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

        let start = begin * self.string_size;
        let end = start + len * self.string_size;

        let mut result = ColumnFixedString::new(self.type_.clone());
        result.data = self.data[start..end].to_vec();

        Ok(Arc::new(result))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Column for variable-length strings
pub struct ColumnString {
    type_: Type,
    data: Vec<String>,
}

impl ColumnString {
    pub fn new(type_: Type) -> Self {
        Self { type_, data: Vec::new() }
    }

    pub fn with_capacity(type_: Type, capacity: usize) -> Self {
        Self { type_, data: Vec::with_capacity(capacity) }
    }

    pub fn from_vec(type_: Type, data: Vec<String>) -> Self {
        Self { type_, data }
    }

    /// Create a column with initial data (builder pattern)
    pub fn with_data(mut self, data: Vec<String>) -> Self {
        self.data = data;
        self
    }

    pub fn append(&mut self, s: impl Into<String>) {
        self.data.push(s.into());
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        self.data.get(index).map(|s| s.as_str())
    }

    /// Get value at index (for tests)
    pub fn at(&self, index: usize) -> String {
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

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.data.iter().map(|s| s.as_str())
    }
}

impl Default for ColumnString {
    fn default() -> Self {
        Self::new(Type::string())
    }
}

impl Column for ColumnString {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.data.len()
    }

    fn clear(&mut self) {
        self.data.clear();
    }

    fn reserve(&mut self, new_cap: usize) {
        self.data.reserve(new_cap);
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let other = other.as_any().downcast_ref::<ColumnString>().ok_or_else(
            || Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            },
        )?;

        self.data.extend(other.data.iter().cloned());
        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        self.data.reserve(rows);

        for _ in 0..rows {
            // Read varint length
            let len = buffer_utils::read_varint(buffer)? as usize;

            if buffer.len() < len {
                return Err(Error::Protocol(format!(
                    "Not enough data for string: need {}, have {}",
                    len,
                    buffer.len()
                )));
            }

            // Read string data
            let string_data = &buffer[..len];
            let s = String::from_utf8(string_data.to_vec()).map_err(|e| {
                Error::Protocol(format!("Invalid UTF-8 in string: {}", e))
            })?;

            self.data.push(s);
            buffer.advance(len);
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for s in &self.data {
            // Write varint length
            buffer_utils::write_varint(buffer, s.len() as u64);
            // Write string data
            buffer.put_slice(s.as_bytes());
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnString::new(self.type_.clone()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        if begin + len > self.data.len() {
            return Err(Error::InvalidArgument(format!(
                "Slice out of bounds: begin={}, len={}, size={}",
                begin,
                len,
                self.data.len()
            )));
        }

        let sliced = self.data[begin..begin + len].to_vec();
        Ok(Arc::new(ColumnString::from_vec(self.type_.clone(), sliced)))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// Helper functions removed - using buffer_utils module

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_string_creation() {
        let col = ColumnFixedString::new(Type::fixed_string(10));
        assert_eq!(col.size(), 0);
        assert_eq!(col.fixed_size(), 10);
    }

    #[test]
    fn test_fixed_string_append() {
        let mut col = ColumnFixedString::new(Type::fixed_string(10));
        col.append("hello".to_string());
        col.append("world".to_string());

        assert_eq!(col.size(), 2);
        assert_eq!(col.get(0), Some("hello".to_string()));
        assert_eq!(col.get(1), Some("world".to_string()));
    }

    #[test]
    fn test_fixed_string_padding() {
        let mut col = ColumnFixedString::new(Type::fixed_string(10));
        col.append("hi".to_string());

        // Should be padded to 10 bytes
        assert_eq!(col.data.len(), 10);
        assert_eq!(col.get(0), Some("hi".to_string()));
    }

    #[test]
    #[should_panic(expected = "String too long")]
    fn test_fixed_string_too_long() {
        let mut col = ColumnFixedString::new(Type::fixed_string(5));
        col.append("too long string".to_string());
    }

    #[test]
    fn test_fixed_string_save_load() {
        let mut col = ColumnFixedString::new(Type::fixed_string(8));
        col.append("hello".to_string());
        col.append("world".to_string());

        let mut buffer = BytesMut::new();
        col.save_to_buffer(&mut buffer).unwrap();

        let mut col2 = ColumnFixedString::new(Type::fixed_string(8));
        let mut reader = &buffer[..];
        col2.load_from_buffer(&mut reader, 2).unwrap();

        assert_eq!(col2.size(), 2);
        assert_eq!(col2.get(0), Some("hello".to_string()));
        assert_eq!(col2.get(1), Some("world".to_string()));
    }

    #[test]
    fn test_string_creation() {
        let col = ColumnString::new(Type::string());
        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_string_append() {
        let mut col = ColumnString::new(Type::string());
        col.append("hello");
        col.append("world");
        col.append(String::from("rust"));

        assert_eq!(col.size(), 3);
        assert_eq!(col.get(0), Some("hello"));
        assert_eq!(col.get(1), Some("world"));
        assert_eq!(col.get(2), Some("rust"));
    }

    #[test]
    fn test_string_save_load() {
        let mut col = ColumnString::new(Type::string());
        col.append("hello");
        col.append("мир"); // Unicode
        col.append("🦀"); // Emoji

        let mut buffer = BytesMut::new();
        col.save_to_buffer(&mut buffer).unwrap();

        let mut col2 = ColumnString::new(Type::string());
        let mut reader = &buffer[..];
        col2.load_from_buffer(&mut reader, 3).unwrap();

        assert_eq!(col2.size(), 3);
        assert_eq!(col2.get(0), Some("hello"));
        assert_eq!(col2.get(1), Some("мир"));
        assert_eq!(col2.get(2), Some("🦀"));
    }

    #[test]
    fn test_string_slice() {
        let mut col = ColumnString::new(Type::string());
        for i in 0..10 {
            col.append(format!("str_{}", i));
        }

        let sliced = col.slice(2, 5).unwrap();
        let sliced_col =
            sliced.as_any().downcast_ref::<ColumnString>().unwrap();

        assert_eq!(sliced_col.size(), 5);
        assert_eq!(sliced_col.get(0), Some("str_2"));
        assert_eq!(sliced_col.get(4), Some("str_6"));
    }

    #[test]
    fn test_varint_encode_decode() {
        let test_values = vec![0u64, 1, 127, 128, 255, 256, 65535, u64::MAX];

        for value in test_values {
            let mut buffer = BytesMut::new();
            buffer_utils::write_varint(&mut buffer, value);

            let mut reader = &buffer[..];
            let decoded = buffer_utils::read_varint(&mut reader).unwrap();

            assert_eq!(value, decoded);
        }
    }
}
