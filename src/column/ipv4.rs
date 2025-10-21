use super::{
    Column,
    ColumnRef,
};
use crate::{
    types::Type,
    Error,
    Result,
};
use bytes::BytesMut;
use std::sync::Arc;

/// Column for IPv4 addresses (stored as UInt32)
pub struct ColumnIpv4 {
    type_: Type,
    data: Vec<u32>, /* IPv4 addresses stored as 32-bit integers (network
                     * byte order) */
}

impl ColumnIpv4 {
    pub fn new(type_: Type) -> Self {
        Self { type_, data: Vec::new() }
    }

    pub fn with_data(mut self, data: Vec<u32>) -> Self {
        self.data = data;
        self
    }

    /// Append IPv4 from dotted decimal string "192.168.1.1"
    pub fn append_from_string(&mut self, s: &str) -> Result<()> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 4 {
            return Err(Error::Protocol(format!(
                "Invalid IPv4 format: {}",
                s
            )));
        }

        let mut ip: u32 = 0;
        for (i, part) in parts.iter().enumerate() {
            let octet = part.parse::<u8>().map_err(|e| {
                Error::Protocol(format!("Invalid IPv4 octet: {}", e))
            })?;
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
        let other =
            other.as_any().downcast_ref::<ColumnIpv4>().ok_or_else(|| {
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
        let bytes_needed = rows * 4;
        if buffer.len() < bytes_needed {
            return Err(Error::Protocol(format!(
                "Buffer underflow: need {} bytes for IPv4, have {}",
                bytes_needed,
                buffer.len()
            )));
        }

        // Use bulk copy for performance
        let current_len = self.data.len();
        unsafe {
            let dest_ptr =
                (self.data.as_mut_ptr() as *mut u8).add(current_len * 4);
            std::ptr::copy_nonoverlapping(
                buffer.as_ptr(),
                dest_ptr,
                bytes_needed,
            );
            self.data.set_len(current_len + rows);
        }

        use bytes::Buf;
        buffer.advance(bytes_needed);
        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        if !self.data.is_empty() {
            let byte_slice = unsafe {
                std::slice::from_raw_parts(
                    self.data.as_ptr() as *const u8,
                    self.data.len() * 4,
                )
            };
            buffer.extend_from_slice(byte_slice);
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
