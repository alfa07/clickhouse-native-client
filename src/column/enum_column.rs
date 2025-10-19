use super::{
    Column,
    ColumnRef,
};
use crate::{
    types::Type,
    Error,
    Result,
};
use bytes::{
    BufMut,
    BytesMut,
};
use std::sync::Arc;

/// Column for Enum8 type (stored as Int8 with name-value mapping in Type)
pub struct ColumnEnum8 {
    type_: Type,
    data: Vec<i8>,
}

impl ColumnEnum8 {
    pub fn new(type_: Type) -> Self {
        match &type_ {
            Type::Enum8 { .. } => Self { type_, data: Vec::new() },
            _ => panic!("ColumnEnum8 requires Enum8 type"),
        }
    }

    pub fn with_data(mut self, data: Vec<i8>) -> Self {
        self.data = data;
        self
    }

    /// Append enum by numeric value
    pub fn append_value(&mut self, value: i8) {
        self.data.push(value);
    }

    /// Append enum by name (looks up value in Type)
    pub fn append_name(&mut self, name: &str) -> Result<()> {
        let value = self.type_.get_enum_value(name).ok_or_else(|| {
            Error::Protocol(format!("Unknown enum name: {}", name))
        })?;

        self.data.push(value as i8);
        Ok(())
    }

    /// Get numeric value at index
    pub fn at(&self, index: usize) -> i8 {
        self.data[index]
    }

    /// Get enum name at index (looks up in Type)
    pub fn name_at(&self, index: usize) -> Option<&str> {
        let value = self.data[index] as i16;
        self.type_.get_enum_name(value)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Column for ColumnEnum8 {
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
        let other =
            other.as_any().downcast_ref::<ColumnEnum8>().ok_or_else(|| {
                Error::TypeMismatch {
                    expected: self.type_.name(),
                    actual: other.column_type().name(),
                }
            })?;

        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        use bytes::Buf;

        self.data.reserve(rows);

        for _ in 0..rows {
            if buffer.is_empty() {
                return Err(Error::Protocol(
                    "Not enough data for Enum8".to_string(),
                ));
            }
            let value = buffer.get_i8();
            self.data.push(value);
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for &value in &self.data {
            buffer.put_i8(value);
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnEnum8::new(self.type_.clone()))
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

        let sliced_data = self.data[begin..begin + len].to_vec();
        Ok(Arc::new(
            ColumnEnum8::new(self.type_.clone()).with_data(sliced_data),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Column for Enum16 type (stored as Int16 with name-value mapping in Type)
pub struct ColumnEnum16 {
    type_: Type,
    data: Vec<i16>,
}

impl ColumnEnum16 {
    pub fn new(type_: Type) -> Self {
        match &type_ {
            Type::Enum16 { .. } => Self { type_, data: Vec::new() },
            _ => panic!("ColumnEnum16 requires Enum16 type"),
        }
    }

    pub fn with_data(mut self, data: Vec<i16>) -> Self {
        self.data = data;
        self
    }

    /// Append enum by numeric value
    pub fn append_value(&mut self, value: i16) {
        self.data.push(value);
    }

    /// Append enum by name (looks up value in Type)
    pub fn append_name(&mut self, name: &str) -> Result<()> {
        let value = self.type_.get_enum_value(name).ok_or_else(|| {
            Error::Protocol(format!("Unknown enum name: {}", name))
        })?;

        self.data.push(value);
        Ok(())
    }

    /// Get numeric value at index
    pub fn at(&self, index: usize) -> i16 {
        self.data[index]
    }

    /// Get enum name at index (looks up in Type)
    pub fn name_at(&self, index: usize) -> Option<&str> {
        let value = self.data[index];
        self.type_.get_enum_name(value)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Column for ColumnEnum16 {
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
        let other = other.as_any().downcast_ref::<ColumnEnum16>().ok_or_else(
            || Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            },
        )?;

        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        use bytes::Buf;

        self.data.reserve(rows);

        for _ in 0..rows {
            if buffer.len() < 2 {
                return Err(Error::Protocol(
                    "Not enough data for Enum16".to_string(),
                ));
            }
            let value = buffer.get_i16_le();
            self.data.push(value);
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for &value in &self.data {
            buffer.put_i16_le(value);
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnEnum16::new(self.type_.clone()))
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

        let sliced_data = self.data[begin..begin + len].to_vec();
        Ok(Arc::new(
            ColumnEnum16::new(self.type_.clone()).with_data(sliced_data),
        ))
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
    use crate::types::EnumItem;

    #[test]
    fn test_enum8_append_value() {
        let items = vec![
            EnumItem { name: "Red".to_string(), value: 1 },
            EnumItem { name: "Green".to_string(), value: 2 },
        ];
        let mut col = ColumnEnum8::new(Type::enum8(items));

        col.append_value(1);
        col.append_value(2);

        assert_eq!(col.len(), 2);
        assert_eq!(col.at(0), 1);
        assert_eq!(col.at(1), 2);
    }

    #[test]
    fn test_enum8_append_name() {
        let items = vec![
            EnumItem { name: "Red".to_string(), value: 1 },
            EnumItem { name: "Green".to_string(), value: 2 },
        ];
        let mut col = ColumnEnum8::new(Type::enum8(items));

        col.append_name("Red").unwrap();
        col.append_name("Green").unwrap();

        assert_eq!(col.len(), 2);
        assert_eq!(col.at(0), 1);
        assert_eq!(col.at(1), 2);
        assert_eq!(col.name_at(0), Some("Red"));
        assert_eq!(col.name_at(1), Some("Green"));
    }

    #[test]
    fn test_enum16() {
        let items = vec![
            EnumItem { name: "Small".to_string(), value: 100 },
            EnumItem { name: "Large".to_string(), value: 1000 },
        ];
        let mut col = ColumnEnum16::new(Type::enum16(items));

        col.append_value(100);
        col.append_name("Large").unwrap();

        assert_eq!(col.len(), 2);
        assert_eq!(col.at(0), 100);
        assert_eq!(col.at(1), 1000);
        assert_eq!(col.name_at(0), Some("Small"));
        assert_eq!(col.name_at(1), Some("Large"));
    }
}
