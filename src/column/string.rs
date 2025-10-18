use super::{Column, ColumnRef, ColumnTyped};
use crate::types::Type;
use crate::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::sync::Arc;

/// Column for fixed-length strings (all strings padded to same length)
pub struct ColumnFixedString {
    type_: Type,
    string_size: usize,
    data: Vec<u8>,
}

impl ColumnFixedString {
    pub fn new(size: usize) -> Self {
        Self {
            type_: Type::fixed_string(size),
            string_size: size,
            data: Vec::new(),
        }
    }

    pub fn with_capacity(size: usize, capacity: usize) -> Self {
        Self {
            type_: Type::fixed_string(size),
            string_size: size,
            data: Vec::with_capacity(size * capacity),
        }
    }

    pub fn append(&mut self, s: &str) {
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
            self.data.resize(self.data.len() + (self.string_size - bytes.len()), 0);
        }
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        if index >= self.size() {
            return None;
        }

        let start = index * self.string_size;
        let end = start + self.string_size;
        let bytes = &self.data[start..end];

        // Find actual string length (trim trailing zeros)
        let actual_len = bytes.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);

        std::str::from_utf8(&bytes[..actual_len]).ok()
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

    fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
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
        Arc::new(ColumnFixedString::new(self.string_size))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        if begin + len > self.size() {
            return Err(Error::InvalidArgument(format!(
                "Slice out of bounds: begin={}, len={}, size={}",
                begin, len, self.size()
            )));
        }

        let start = begin * self.string_size;
        let end = start + len * self.string_size;

        let mut result = ColumnFixedString::new(self.string_size);
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
    pub fn new() -> Self {
        Self {
            type_: Type::string(),
            data: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            type_: Type::string(),
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn from_vec(data: Vec<String>) -> Self {
        Self {
            type_: Type::string(),
            data,
        }
    }

    pub fn append(&mut self, s: impl Into<String>) {
        self.data.push(s.into());
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        self.data.get(index).map(|s| s.as_str())
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.data.iter().map(|s| s.as_str())
    }
}

impl Default for ColumnString {
    fn default() -> Self {
        Self::new()
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
        let other = other
            .as_any()
            .downcast_ref::<ColumnString>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            })?;

        self.data.extend(other.data.iter().cloned());
        Ok(())
    }

    fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
        self.data.reserve(rows);

        for _ in 0..rows {
            // Read varint length
            let len = read_varint(buffer)? as usize;

            if buffer.len() < len {
                return Err(Error::Protocol(format!(
                    "Not enough data for string: need {}, have {}",
                    len, buffer.len()
                )));
            }

            // Read string data
            let string_data = &buffer[..len];
            let s = String::from_utf8(string_data.to_vec())
                .map_err(|e| Error::Protocol(format!("Invalid UTF-8 in string: {}", e)))?;

            self.data.push(s);
            buffer.advance(len);
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for s in &self.data {
            // Write varint length
            write_varint(buffer, s.len() as u64);
            // Write string data
            buffer.put_slice(s.as_bytes());
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnString::new())
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        if begin + len > self.data.len() {
            return Err(Error::InvalidArgument(format!(
                "Slice out of bounds: begin={}, len={}, size={}",
                begin, len, self.data.len()
            )));
        }

        let sliced = self.data[begin..begin + len].to_vec();
        Ok(Arc::new(ColumnString::from_vec(sliced)))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

// Helper functions for varint encoding/decoding in sync context
fn read_varint(buffer: &mut &[u8]) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0;

    loop {
        if buffer.is_empty() {
            return Err(Error::Protocol("Unexpected end of buffer reading varint".to_string()));
        }

        let byte = buffer[0];
        buffer.advance(1);

        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;
        if shift >= 64 {
            return Err(Error::Protocol("Varint overflow".to_string()));
        }
    }

    Ok(result)
}

fn write_varint(buffer: &mut BytesMut, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buffer.put_u8(byte);

        if value == 0 {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_string_creation() {
        let col = ColumnFixedString::new(10);
        assert_eq!(col.size(), 0);
        assert_eq!(col.fixed_size(), 10);
    }

    #[test]
    fn test_fixed_string_append() {
        let mut col = ColumnFixedString::new(10);
        col.append("hello");
        col.append("world");

        assert_eq!(col.size(), 2);
        assert_eq!(col.get(0), Some("hello"));
        assert_eq!(col.get(1), Some("world"));
    }

    #[test]
    fn test_fixed_string_padding() {
        let mut col = ColumnFixedString::new(10);
        col.append("hi");

        // Should be padded to 10 bytes
        assert_eq!(col.data.len(), 10);
        assert_eq!(col.get(0), Some("hi"));
    }

    #[test]
    #[should_panic(expected = "String too long")]
    fn test_fixed_string_too_long() {
        let mut col = ColumnFixedString::new(5);
        col.append("too long string");
    }

    #[test]
    fn test_fixed_string_save_load() {
        let mut col = ColumnFixedString::new(8);
        col.append("hello");
        col.append("world");

        let mut buffer = BytesMut::new();
        col.save_to_buffer(&mut buffer).unwrap();

        let mut col2 = ColumnFixedString::new(8);
        let mut reader = &buffer[..];
        col2.load_from_buffer(&mut reader, 2).unwrap();

        assert_eq!(col2.size(), 2);
        assert_eq!(col2.get(0), Some("hello"));
        assert_eq!(col2.get(1), Some("world"));
    }

    #[test]
    fn test_string_creation() {
        let col = ColumnString::new();
        assert_eq!(col.size(), 0);
    }

    #[test]
    fn test_string_append() {
        let mut col = ColumnString::new();
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
        let mut col = ColumnString::new();
        col.append("hello");
        col.append("мир"); // Unicode
        col.append("🦀"); // Emoji

        let mut buffer = BytesMut::new();
        col.save_to_buffer(&mut buffer).unwrap();

        let mut col2 = ColumnString::new();
        let mut reader = &buffer[..];
        col2.load_from_buffer(&mut reader, 3).unwrap();

        assert_eq!(col2.size(), 3);
        assert_eq!(col2.get(0), Some("hello"));
        assert_eq!(col2.get(1), Some("мир"));
        assert_eq!(col2.get(2), Some("🦀"));
    }

    #[test]
    fn test_string_slice() {
        let mut col = ColumnString::new();
        for i in 0..10 {
            col.append(format!("str_{}", i));
        }

        let sliced = col.slice(2, 5).unwrap();
        let sliced_col = sliced.as_any().downcast_ref::<ColumnString>().unwrap();

        assert_eq!(sliced_col.size(), 5);
        assert_eq!(sliced_col.get(0), Some("str_2"));
        assert_eq!(sliced_col.get(4), Some("str_6"));
    }

    #[test]
    fn test_varint_encode_decode() {
        let test_values = vec![0u64, 1, 127, 128, 255, 256, 65535, u64::MAX];

        for value in test_values {
            let mut buffer = BytesMut::new();
            write_varint(&mut buffer, value);

            let mut reader = &buffer[..];
            let decoded = read_varint(&mut reader).unwrap();

            assert_eq!(value, decoded);
        }
    }
}
