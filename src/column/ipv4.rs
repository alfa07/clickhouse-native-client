use super::{Column, ColumnRef};
use crate::types::Type;
use crate::{Error, Result};
use bytes::{BufMut, BytesMut};
use std::sync::Arc;

/// Column for IPv4 addresses (stored as UInt32)
pub struct ColumnIpv4 {
    type_: Type,
    data: Vec<u32>, // IPv4 addresses stored as 32-bit integers (network byte order)
}

impl ColumnIpv4 {
    pub fn new(type_: Type) -> Self {
        Self {
            type_,
            data: Vec::new(),
        }
    }

    pub fn with_data(mut self, data: Vec<u32>) -> Self {
        self.data = data;
        self
    }

    /// Append IPv4 from dotted decimal string "192.168.1.1"
    pub fn append_from_string(&mut self, s: &str) -> Result<()> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 4 {
            return Err(Error::Protocol(format!("Invalid IPv4 format: {}", s)));
        }

        let mut ip: u32 = 0;
        for (i, part) in parts.iter().enumerate() {
            let octet = part
                .parse::<u8>()
                .map_err(|e| Error::Protocol(format!("Invalid IPv4 octet: {}", e)))?;
            // Network byte order: most significant byte first
            ip |= (octet as u32) << (24 - i * 8);
        }

        self.data.push(ip);
        Ok(())
    }

    /// Append IPv4 from u32 value
    pub fn append(&mut self, value: u32) {
        self.data.push(value);
    }

    /// Get IPv4 at index as u32
    pub fn at(&self, index: usize) -> u32 {
        self.data[index]
    }

    /// Format IPv4 at index as dotted decimal string
    pub fn as_string(&self, index: usize) -> String {
        let ip = self.data[index];
        format!(
            "{}.{}.{}.{}",
            (ip >> 24) & 0xFF,
            (ip >> 16) & 0xFF,
            (ip >> 8) & 0xFF,
            ip & 0xFF
        )
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Column for ColumnIpv4 {
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
            .downcast_ref::<ColumnIpv4>()
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
            if buffer.len() < 4 {
                return Err(Error::Protocol("Not enough data for IPv4".to_string()));
            }

            let ip = buffer.get_u32_le();
            self.data.push(ip);
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for &ip in &self.data {
            buffer.put_u32_le(ip);
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnIpv4::new(self.type_.clone()))
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
            ColumnIpv4::new(self.type_.clone()).with_data(sliced_data),
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
    fn test_ipv4_from_string() {
        let mut col = ColumnIpv4::new(Type::ipv4());
        col.append_from_string("192.168.1.1").unwrap();
        col.append_from_string("10.0.0.1").unwrap();
        col.append_from_string("0.0.0.0").unwrap();

        assert_eq!(col.len(), 3);
        assert_eq!(col.as_string(0), "192.168.1.1");
        assert_eq!(col.as_string(1), "10.0.0.1");
        assert_eq!(col.as_string(2), "0.0.0.0");
    }

    #[test]
    fn test_ipv4_from_u32() {
        let mut col = ColumnIpv4::new(Type::ipv4());
        col.append(0xC0A80101); // 192.168.1.1
        col.append(0x0A000001); // 10.0.0.1
        col.append(0); // 0.0.0.0

        assert_eq!(col.len(), 3);
        assert_eq!(col.at(0), 0xC0A80101);
        assert_eq!(col.at(1), 0x0A000001);
        assert_eq!(col.at(2), 0);
    }

    #[test]
    fn test_ipv4_edge_cases() {
        let mut col = ColumnIpv4::new(Type::ipv4());
        col.append_from_string("255.255.255.255").unwrap();
        col.append_from_string("127.0.0.1").unwrap();

        assert_eq!(col.as_string(0), "255.255.255.255");
        assert_eq!(col.as_string(1), "127.0.0.1");
    }
}
