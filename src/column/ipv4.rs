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
///
/// **C++ Implementation Pattern:**
/// Uses delegation to `ColumnUInt32` for storage, matching the C++
/// clickhouse-cpp reference implementation's `std::shared_ptr<ColumnUInt32>
/// data_` pattern.
pub struct ColumnIpv4 {
    type_: Type,
    data: Arc<super::ColumnUInt32>, /* Delegates to ColumnUInt32, matches
                                     * C++ pattern */
}

impl ColumnIpv4 {
    pub fn new(type_: Type) -> Self {
        Self { type_, data: Arc::new(super::ColumnUInt32::new()) }
    }

    pub fn with_data(mut self, data: Vec<u32>) -> Self {
        self.data =
            Arc::new(super::ColumnUInt32::from_vec(Type::uint32(), data));
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

        self.append(ip);
        Ok(())
    }

    /// Append IPv4 from u32 value
    pub fn append(&mut self, value: u32) {
        Arc::get_mut(&mut self.data)
            .expect("Cannot append to shared column")
            .append(value);
    }

    /// Get IPv4 at index as u32
    pub fn at(&self, index: usize) -> u32 {
        self.data.at(index)
    }

    /// Format IPv4 at index as dotted decimal string
    pub fn as_string(&self, index: usize) -> String {
        let ip = self.data.at(index);
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

    /// Get reference to underlying data column (for advanced use)
    pub fn data(&self) -> &super::ColumnUInt32 {
        &self.data
    }
}

impl Column for ColumnIpv4 {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.data.size()
    }

    fn clear(&mut self) {
        Arc::get_mut(&mut self.data)
            .expect("Cannot clear shared column")
            .clear();
    }

    fn reserve(&mut self, new_cap: usize) {
        Arc::get_mut(&mut self.data)
            .expect("Cannot reserve on shared column")
            .reserve(new_cap);
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let other =
            other.as_any().downcast_ref::<ColumnIpv4>().ok_or_else(|| {
                Error::TypeMismatch {
                    expected: self.type_.name(),
                    actual: other.column_type().name(),
                }
            })?;

        // Delegate to underlying ColumnUInt32
        Arc::get_mut(&mut self.data)
            .expect("Cannot append to shared column")
            .append_column(other.data.clone() as ColumnRef)?;
        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        // Delegate to ColumnUInt32 which has bulk copy optimization
        Arc::get_mut(&mut self.data)
            .expect("Cannot load into shared column")
            .load_from_buffer(buffer, rows)
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Delegate to ColumnUInt32 which has bulk copy optimization
        self.data.save_to_buffer(buffer)
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnIpv4::new(self.type_.clone()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        // Delegate to underlying column and wrap result
        let sliced_data = self.data.slice(begin, len)?;

        Ok(Arc::new(ColumnIpv4 {
            type_: self.type_.clone(),
            data: sliced_data
                .as_any()
                .downcast_ref::<super::ColumnUInt32>()
                .map(|col| {
                    // Create new Arc from the sliced data
                    Arc::new(super::ColumnUInt32::from_vec(
                        Type::uint32(),
                        col.data().to_vec(),
                    ))
                })
                .expect("Slice should return ColumnUInt32"),
        }))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
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
