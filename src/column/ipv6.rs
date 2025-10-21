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

/// Column for IPv6 addresses (stored as FixedString(16) - 16 bytes)
///
/// **Implementation Note:**
/// Unlike C++, this does NOT delegate to `ColumnFixedString` because IPv6 data
/// is pure binary (not UTF-8 text). Rust's `ColumnFixedString` uses `String`
/// which requires valid UTF-8 and trims null bytes, corrupting binary IPv6
/// data. Direct `Vec<[u8; 16]>` storage is more appropriate and preserves bulk
/// copy performance.
pub struct ColumnIpv6 {
    type_: Type,
    data: Vec<[u8; 16]>, // IPv6 addresses stored as 16 bytes
}

impl ColumnIpv6 {
    pub fn new(type_: Type) -> Self {
        Self { type_, data: Vec::new() }
    }

    pub fn with_data(mut self, data: Vec<[u8; 16]>) -> Self {
        self.data = data;
        self
    }

    /// Append IPv6 from string (supports compressed format)
    /// Examples: "2001:0db8:85a3:0000:0000:8a2e:0370:7334", "::1", "fe80::1"
    pub fn append_from_string(&mut self, s: &str) -> Result<()> {
        let bytes = parse_ipv6(s)?;
        self.data.push(bytes);
        Ok(())
    }

    /// Append IPv6 from 16-byte array
    pub fn append(&mut self, bytes: [u8; 16]) {
        self.data.push(bytes);
    }

    /// Get IPv6 at index as 16-byte array
    pub fn at(&self, index: usize) -> [u8; 16] {
        self.data[index]
    }

    /// Format IPv6 at index as string
    pub fn as_string(&self, index: usize) -> String {
        format_ipv6(&self.data[index])
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Column for ColumnIpv6 {
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
            other.as_any().downcast_ref::<ColumnIpv6>().ok_or_else(|| {
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
        let bytes_needed = rows * 16;
        if buffer.len() < bytes_needed {
            return Err(Error::Protocol(format!(
                "Buffer underflow: need {} bytes for IPv6, have {}",
                bytes_needed,
                buffer.len()
            )));
        }

        // Use bulk copy for performance
        self.data.reserve(rows);
        let current_len = self.data.len();
        unsafe {
            // Set length first to claim ownership of the memory
            self.data.set_len(current_len + rows);
            let dest_ptr =
                (self.data.as_mut_ptr() as *mut u8).add(current_len * 16);
            std::ptr::copy_nonoverlapping(
                buffer.as_ptr(),
                dest_ptr,
                bytes_needed,
            );
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
                    self.data.len() * 16,
                )
            };
            buffer.extend_from_slice(byte_slice);
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnIpv6::new(self.type_.clone()))
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
            ColumnIpv6::new(self.type_.clone()).with_data(sliced_data),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Parse IPv6 string to 16-byte array
fn parse_ipv6(s: &str) -> Result<[u8; 16]> {
    let parts: Vec<&str> = s.split("::").collect();

    if parts.len() > 2 {
        return Err(Error::Protocol(format!(
            "Invalid IPv6 format (multiple ::): {}",
            s
        )));
    }

    let mut result = [0u8; 16];

    if parts.len() == 2 {
        // Compressed format with ::
        let left_parts: Vec<&str> = if parts[0].is_empty() {
            vec![]
        } else {
            parts[0].split(':').collect()
        };
        let right_parts: Vec<&str> = if parts[1].is_empty() {
            vec![]
        } else {
            parts[1].split(':').collect()
        };

        // Parse left side
        for (i, part) in left_parts.iter().enumerate() {
            let value = u16::from_str_radix(part, 16).map_err(|e| {
                Error::Protocol(format!("Invalid IPv6 hex: {}", e))
            })?;
            result[i * 2] = (value >> 8) as u8;
            result[i * 2 + 1] = (value & 0xFF) as u8;
        }

        // Parse right side
        let right_start = 16 - right_parts.len() * 2;
        for (i, part) in right_parts.iter().enumerate() {
            let value = u16::from_str_radix(part, 16).map_err(|e| {
                Error::Protocol(format!("Invalid IPv6 hex: {}", e))
            })?;
            result[right_start + i * 2] = (value >> 8) as u8;
            result[right_start + i * 2 + 1] = (value & 0xFF) as u8;
        }
    } else {
        // Full format
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 8 {
            return Err(Error::Protocol(format!(
                "Invalid IPv6 format (expected 8 parts): {}",
                s
            )));
        }

        for (i, part) in parts.iter().enumerate() {
            let value = u16::from_str_radix(part, 16).map_err(|e| {
                Error::Protocol(format!("Invalid IPv6 hex: {}", e))
            })?;
            result[i * 2] = (value >> 8) as u8;
            result[i * 2 + 1] = (value & 0xFF) as u8;
        }
    }

    Ok(result)
}

/// Format 16-byte array as IPv6 string (compressed format)
fn format_ipv6(bytes: &[u8; 16]) -> String {
    // Convert bytes to u16 groups
    let mut groups = [0u16; 8];
    for i in 0..8 {
        groups[i] = ((bytes[i * 2] as u16) << 8) | (bytes[i * 2 + 1] as u16);
    }

    // Find longest run of zeros for compression
    let mut max_zero_start = None;
    let mut max_zero_len = 0;
    let mut current_zero_start = None;
    let mut current_zero_len = 0;

    for (i, &group) in groups.iter().enumerate() {
        if group == 0 {
            if current_zero_start.is_none() {
                current_zero_start = Some(i);
                current_zero_len = 1;
            } else {
                current_zero_len += 1;
            }
        } else {
            if current_zero_len > max_zero_len {
                max_zero_start = current_zero_start;
                max_zero_len = current_zero_len;
            }
            current_zero_start = None;
            current_zero_len = 0;
        }
    }

    // Check final run
    if current_zero_len > max_zero_len {
        max_zero_start = current_zero_start;
        max_zero_len = current_zero_len;
    }

    // Format with compression if we have a run of 2+ zeros
    if max_zero_len >= 2 {
        let start = max_zero_start.unwrap();
        let end = start + max_zero_len;

        let mut result = String::new();

        // Add groups before compression
        for (i, &group) in groups.iter().enumerate().take(start) {
            if i > 0 {
                result.push(':');
            }
            result.push_str(&format!("{:x}", group));
        }

        // Add compression marker
        result.push_str("::");

        // Add groups after compression
        for (i, &group) in groups.iter().enumerate().skip(end) {
            if i > end {
                result.push(':');
            }
            result.push_str(&format!("{:x}", group));
        }

        result
    } else {
        // No compression
        groups.iter().map(|g| format!("{:x}", g)).collect::<Vec<_>>().join(":")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipv6_full_format() {
        let mut col = ColumnIpv6::new(Type::ipv6());
        col.append_from_string("2001:0db8:85a3:0000:0000:8a2e:0370:7334")
            .unwrap();

        assert_eq!(col.len(), 1);
        // Should be compressed when formatted
        let formatted = col.as_string(0);
        assert!(formatted.contains("2001") && formatted.contains("7334"));
    }

    #[test]
    fn test_ipv6_compressed() {
        let mut col = ColumnIpv6::new(Type::ipv6());
        col.append_from_string("::1").unwrap();
        col.append_from_string("fe80::1").unwrap();

        assert_eq!(col.len(), 2);
    }

    #[test]
    fn test_ipv6_zeros() {
        let mut col = ColumnIpv6::new(Type::ipv6());
        col.append([0u8; 16]);

        assert_eq!(col.len(), 1);
        let formatted = col.as_string(0);
        assert_eq!(formatted, "::");
    }

    #[test]
    fn test_ipv6_from_bytes() {
        let mut col = ColumnIpv6::new(Type::ipv6());
        let bytes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        col.append(bytes);

        assert_eq!(col.len(), 1);
        assert_eq!(col.at(0), bytes);
    }
}
