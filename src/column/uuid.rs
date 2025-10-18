use super::{Column, ColumnRef};
use crate::types::Type;
use crate::{Error, Result};
use bytes::{BufMut, BytesMut};
use std::sync::Arc;

/// UUID value stored as 128 bits (2x u64)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Uuid {
    pub high: u64,
    pub low: u64,
}

impl Uuid {
    pub fn new(high: u64, low: u64) -> Self {
        Self { high, low }
    }

    /// Parse UUID from string format: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.replace("-", "");
        if s.len() != 32 {
            return Err(Error::Protocol(format!("Invalid UUID format: {}", s)));
        }

        let high = u64::from_str_radix(&s[0..16], 16)
            .map_err(|e| Error::Protocol(format!("Invalid UUID hex: {}", e)))?;
        let low = u64::from_str_radix(&s[16..32], 16)
            .map_err(|e| Error::Protocol(format!("Invalid UUID hex: {}", e)))?;

        Ok(Self { high, low })
    }

    /// Format UUID as string: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    pub fn to_string(&self) -> String {
        format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            (self.high >> 32) as u32,
            ((self.high >> 16) & 0xFFFF) as u16,
            (self.high & 0xFFFF) as u16,
            (self.low >> 48) as u16,
            (self.low & 0xFFFFFFFFFFFF) as u64,
        )
    }
}

/// Column for UUID type (stored as 2x UInt64)
pub struct ColumnUuid {
    type_: Type,
    data: Vec<Uuid>,
}

impl ColumnUuid {
    pub fn new(type_: Type) -> Self {
        Self {
            type_,
            data: Vec::new(),
        }
    }

    pub fn with_data(mut self, data: Vec<Uuid>) -> Self {
        self.data = data;
        self
    }

    pub fn append(&mut self, value: Uuid) {
        self.data.push(value);
    }

    pub fn append_from_string(&mut self, s: &str) -> Result<()> {
        let uuid = Uuid::parse(s)?;
        self.data.push(uuid);
        Ok(())
    }

    pub fn at(&self, index: usize) -> Uuid {
        self.data[index]
    }

    pub fn as_string(&self, index: usize) -> String {
        self.data[index].to_string()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Column for ColumnUuid {
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
            .downcast_ref::<ColumnUuid>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            })?;

        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
        use bytes::Buf;

        self.data.reserve(rows);

        for _ in 0..rows {
            if buffer.len() < 16 {
                return Err(Error::Protocol("Not enough data for UUID".to_string()));
            }

            let high = buffer.get_u64_le();
            let low = buffer.get_u64_le();
            self.data.push(Uuid { high, low });
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for uuid in &self.data {
            buffer.put_u64_le(uuid.high);
            buffer.put_u64_le(uuid.low);
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnUuid::new(self.type_.clone()))
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
            ColumnUuid::new(self.type_.clone()).with_data(sliced_data),
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

    #[test]
    fn test_uuid_parse() {
        let uuid = Uuid::parse("550e8400-e29b-41d4-a716-446655440000").unwrap();
        assert_eq!(uuid.high, 0x550e8400e29b41d4);
        assert_eq!(uuid.low, 0xa716446655440000);
    }

    #[test]
    fn test_uuid_to_string() {
        let uuid = Uuid::new(0x550e8400e29b41d4, 0xa716446655440000);
        assert_eq!(
            uuid.to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_uuid_column_append() {
        let mut col = ColumnUuid::new(Type::uuid());
        col.append(Uuid::new(0x123456789abcdef0, 0xfedcba9876543210));
        col.append(Uuid::new(0, 0));

        assert_eq!(col.len(), 2);
        assert_eq!(col.at(0), Uuid::new(0x123456789abcdef0, 0xfedcba9876543210));
        assert_eq!(col.at(1), Uuid::new(0, 0));
    }

    #[test]
    fn test_uuid_column_from_string() {
        let mut col = ColumnUuid::new(Type::uuid());
        col.append_from_string("550e8400-e29b-41d4-a716-446655440000")
            .unwrap();

        assert_eq!(col.len(), 1);
        assert_eq!(
            col.as_string(0),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }
}
