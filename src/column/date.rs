use super::{Column, ColumnRef};
use crate::types::Type;
use crate::{Error, Result};
use bytes::{Buf, BufMut, BytesMut};
use std::sync::Arc;

const SECONDS_PER_DAY: i64 = 86400;

/// Column for Date type (stored as UInt16 - days since Unix epoch 1970-01-01)
/// Range: 1970-01-01 to 2149-06-06
pub struct ColumnDate {
    type_: Type,
    data: Vec<u16>, // Days since Unix epoch
}

impl ColumnDate {
    pub fn new(type_: Type) -> Self {
        Self {
            type_,
            data: Vec::new(),
        }
    }

    pub fn with_data(mut self, data: Vec<u16>) -> Self {
        self.data = data;
        self
    }

    /// Append days since epoch (raw UInt16 value)
    pub fn append(&mut self, days: u16) {
        self.data.push(days);
    }

    /// Append from Unix timestamp (seconds since epoch)
    pub fn append_timestamp(&mut self, timestamp: i64) {
        let days = (timestamp / SECONDS_PER_DAY) as u16;
        self.data.push(days);
    }

    /// Get raw days value at index
    pub fn at(&self, index: usize) -> u16 {
        self.data[index]
    }

    /// Get Unix timestamp (seconds) at index
    pub fn timestamp_at(&self, index: usize) -> i64 {
        self.data[index] as i64 * SECONDS_PER_DAY
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Column for ColumnDate {
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
            .downcast_ref::<ColumnDate>()
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
            if buffer.len() < 2 {
                return Err(Error::Protocol("Not enough data for Date".to_string()));
            }
            let value = buffer.get_u16_le();
            self.data.push(value);
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for &value in &self.data {
            buffer.put_u16_le(value);
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnDate::new(self.type_.clone()))
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
            ColumnDate::new(self.type_.clone()).with_data(sliced_data),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Column for Date32 type (stored as Int32 - days since Unix epoch 1970-01-01)
/// Extended range: 1900-01-01 to 2299-12-31
pub struct ColumnDate32 {
    type_: Type,
    data: Vec<i32>, // Days since Unix epoch (signed for extended range)
}

impl ColumnDate32 {
    pub fn new(type_: Type) -> Self {
        Self {
            type_,
            data: Vec::new(),
        }
    }

    pub fn with_data(mut self, data: Vec<i32>) -> Self {
        self.data = data;
        self
    }

    /// Append days since epoch (raw Int32 value)
    pub fn append(&mut self, days: i32) {
        self.data.push(days);
    }

    /// Append from Unix timestamp (seconds since epoch)
    pub fn append_timestamp(&mut self, timestamp: i64) {
        let days = (timestamp / SECONDS_PER_DAY) as i32;
        self.data.push(days);
    }

    /// Get raw days value at index
    pub fn at(&self, index: usize) -> i32 {
        self.data[index]
    }

    /// Get Unix timestamp (seconds) at index
    pub fn timestamp_at(&self, index: usize) -> i64 {
        self.data[index] as i64 * SECONDS_PER_DAY
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Column for ColumnDate32 {
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
            .downcast_ref::<ColumnDate32>()
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
            if buffer.len() < 4 {
                return Err(Error::Protocol("Not enough data for Date32".to_string()));
            }
            let value = buffer.get_i32_le();
            self.data.push(value);
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for &value in &self.data {
            buffer.put_i32_le(value);
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnDate32::new(self.type_.clone()))
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
            ColumnDate32::new(self.type_.clone()).with_data(sliced_data),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Column for DateTime type (stored as UInt32 - seconds since Unix epoch)
/// Range: 1970-01-01 00:00:00 to 2106-02-07 06:28:15
pub struct ColumnDateTime {
    type_: Type,
    data: Vec<u32>, // Seconds since Unix epoch
    timezone: Option<String>,
}

impl ColumnDateTime {
    pub fn new(type_: Type) -> Self {
        let timezone = match &type_ {
            Type::DateTime { timezone } => timezone.clone(),
            _ => None,
        };

        Self {
            type_,
            data: Vec::new(),
            timezone,
        }
    }

    pub fn with_data(mut self, data: Vec<u32>) -> Self {
        self.data = data;
        self
    }

    /// Append Unix timestamp (seconds since epoch)
    pub fn append(&mut self, timestamp: u32) {
        self.data.push(timestamp);
    }

    /// Get timestamp at index
    pub fn at(&self, index: usize) -> u32 {
        self.data[index]
    }

    /// Get timezone
    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Column for ColumnDateTime {
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
            .downcast_ref::<ColumnDateTime>()
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
            if buffer.len() < 4 {
                return Err(Error::Protocol("Not enough data for DateTime".to_string()));
            }
            let value = buffer.get_u32_le();
            self.data.push(value);
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for &value in &self.data {
            buffer.put_u32_le(value);
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnDateTime::new(self.type_.clone()))
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
            ColumnDateTime::new(self.type_.clone()).with_data(sliced_data),
        ))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Column for DateTime64 type (stored as Int64 - subsecond precision)
/// Supports arbitrary sub-second precision, extended date range
pub struct ColumnDateTime64 {
    type_: Type,
    data: Vec<i64>, // Timestamp with precision
    precision: usize,
    timezone: Option<String>,
}

impl ColumnDateTime64 {
    pub fn new(type_: Type) -> Self {
        let (precision, timezone) = match &type_ {
            Type::DateTime64 {
                precision,
                timezone,
            } => (*precision, timezone.clone()),
            _ => panic!("ColumnDateTime64 requires DateTime64 type"),
        };

        Self {
            type_,
            data: Vec::new(),
            precision,
            timezone,
        }
    }

    pub fn with_data(mut self, data: Vec<i64>) -> Self {
        self.data = data;
        self
    }

    /// Append timestamp with precision (e.g., for precision 3, value is milliseconds)
    pub fn append(&mut self, value: i64) {
        self.data.push(value);
    }

    /// Get timestamp at index
    pub fn at(&self, index: usize) -> i64 {
        self.data[index]
    }

    /// Get precision (0-9, number of decimal places)
    pub fn precision(&self) -> usize {
        self.precision
    }

    /// Get timezone
    pub fn timezone(&self) -> Option<&str> {
        self.timezone.as_deref()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Column for ColumnDateTime64 {
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
            .downcast_ref::<ColumnDateTime64>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            })?;

        if self.precision != other.precision {
            return Err(Error::TypeMismatch {
                expected: format!("DateTime64({})", self.precision),
                actual: format!("DateTime64({})", other.precision),
            });
        }

        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
        self.data.reserve(rows);

        for _ in 0..rows {
            if buffer.len() < 8 {
                return Err(Error::Protocol(
                    "Not enough data for DateTime64".to_string(),
                ));
            }
            let value = buffer.get_i64_le();
            self.data.push(value);
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        for &value in &self.data {
            buffer.put_i64_le(value);
        }
        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnDateTime64::new(self.type_.clone()))
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
            ColumnDateTime64::new(self.type_.clone()).with_data(sliced_data),
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
    fn test_date_append_and_retrieve() {
        let mut col = ColumnDate::new(Type::date());
        col.append(19000); // Days since epoch
        col.append(19001);

        assert_eq!(col.len(), 2);
        assert_eq!(col.at(0), 19000);
        assert_eq!(col.at(1), 19001);
    }

    #[test]
    fn test_date_timestamp() {
        let mut col = ColumnDate::new(Type::date());
        col.append_timestamp(1640995200); // 2022-01-01 00:00:00

        assert_eq!(col.len(), 1);
        let days = col.at(0);
        assert_eq!(days, 18993);
    }

    #[test]
    fn test_date32() {
        let mut col = ColumnDate32::new(Type::date32());
        col.append(-25567); // 1900-01-01 (negative days)
        col.append(0); // 1970-01-01
        col.append(100000); // Future date

        assert_eq!(col.len(), 3);
        assert_eq!(col.at(0), -25567);
        assert_eq!(col.at(1), 0);
        assert_eq!(col.at(2), 100000);
    }

    #[test]
    fn test_datetime() {
        let mut col = ColumnDateTime::new(Type::datetime(None));
        col.append(1640995200); // 2022-01-01 00:00:00 UTC
        col.append(1640995201);

        assert_eq!(col.len(), 2);
        assert_eq!(col.at(0), 1640995200);
        assert_eq!(col.at(1), 1640995201);
    }

    #[test]
    fn test_datetime_with_timezone() {
        let mut col = ColumnDateTime::new(Type::datetime(Some("UTC".to_string())));
        col.append(1640995200);

        assert_eq!(col.timezone(), Some("UTC"));
        assert_eq!(col.at(0), 1640995200);
    }

    #[test]
    fn test_datetime64() {
        let mut col = ColumnDateTime64::new(Type::datetime64(3, None)); // millisecond precision
        col.append(1640995200000); // 2022-01-01 00:00:00.000 UTC
        col.append(1640995200123);

        assert_eq!(col.len(), 2);
        assert_eq!(col.precision(), 3);
        assert_eq!(col.at(0), 1640995200000);
        assert_eq!(col.at(1), 1640995200123);
    }
}
