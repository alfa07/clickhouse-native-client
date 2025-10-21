//! Date and DateTime column implementations
//!
//! **ClickHouse Documentation:**
//! - [Date](https://clickhouse.com/docs/en/sql-reference/data-types/date) -
//!   Days since 1970-01-01 (UInt16)
//! - [Date32](https://clickhouse.com/docs/en/sql-reference/data-types/date32)
//!   - Extended range date (Int32)
//! - [DateTime](https://clickhouse.com/docs/en/sql-reference/data-types/datetime)
//!   - Unix timestamp (UInt32)
//! - [DateTime64](https://clickhouse.com/docs/en/sql-reference/data-types/datetime64)
//!   - High-precision timestamp (Int64)
//!
//! ## Storage Details
//!
//! | Type | Storage | Range | Precision |
//! |------|---------|-------|-----------|
//! | `Date` | UInt16 | 1970-01-01 to 2149-06-06 | 1 day |
//! | `Date32` | Int32 | 1900-01-01 to 2299-12-31 | 1 day |
//! | `DateTime` | UInt32 | 1970-01-01 00:00:00 to 2106-02-07 06:28:15 UTC | 1 second |
//! | `DateTime64(P)` | Int64 | Large range | 10^-P seconds (P=0..9) |
//!
//! ## Timezones
//!
//! `DateTime` and `DateTime64` support optional timezone parameter:
//! - `DateTime('UTC')` - Store as UTC timestamp
//! - `DateTime('Europe/Moscow')` - Store with timezone info
//!
//! The timezone affects how values are displayed and interpreted, but storage
//! is always in Unix time.

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
    Buf,
    BytesMut,
};
use std::sync::Arc;

const SECONDS_PER_DAY: i64 = 86400;

/// Column for Date type (stored as UInt16 - days since Unix epoch 1970-01-01)
///
/// **Range:** 1970-01-01 to 2149-06-06
///
/// **ClickHouse Reference:** <https://clickhouse.com/docs/en/sql-reference/data-types/date>
///
/// **C++ Implementation Pattern:**
/// Uses delegation to `ColumnUInt16` for storage, matching the C++
/// clickhouse-cpp reference implementation's `std::shared_ptr<ColumnUInt16>
/// data_` pattern.
pub struct ColumnDate {
    type_: Type,
    data: Arc<super::ColumnUInt16>, /* Delegates to ColumnUInt16, matches
                                     * C++ pattern */
}

impl ColumnDate {
    pub fn new(type_: Type) -> Self {
        Self {
            type_,
            data: Arc::new(super::ColumnUInt16::new(Type::uint16())),
        }
    }

    pub fn with_data(mut self, data: Vec<u16>) -> Self {
        self.data =
            Arc::new(super::ColumnUInt16::from_vec(Type::uint16(), data));
        self
    }

    /// Append days since epoch (raw UInt16 value)
    pub fn append(&mut self, days: u16) {
        Arc::get_mut(&mut self.data)
            .expect("Cannot append to shared column")
            .append(days);
    }

    /// Append from Unix timestamp (seconds since epoch)
    pub fn append_timestamp(&mut self, timestamp: i64) {
        let days = (timestamp / SECONDS_PER_DAY) as u16;
        self.append(days);
    }

    /// Get raw days value at index
    pub fn at(&self, index: usize) -> u16 {
        self.data.at(index)
    }

    /// Get Unix timestamp (seconds) at index
    pub fn timestamp_at(&self, index: usize) -> i64 {
        self.data.at(index) as i64 * SECONDS_PER_DAY
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get reference to underlying data column (for advanced use)
    pub fn data(&self) -> &super::ColumnUInt16 {
        &self.data
    }
}

impl Column for ColumnDate {
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
            other.as_any().downcast_ref::<ColumnDate>().ok_or_else(|| {
                Error::TypeMismatch {
                    expected: self.type_.name(),
                    actual: other.column_type().name(),
                }
            })?;

        // Delegate to underlying ColumnUInt16
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
        // Delegate to ColumnUInt16 which has bulk copy optimization
        Arc::get_mut(&mut self.data)
            .expect("Cannot load into shared column")
            .load_from_buffer(buffer, rows)
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Delegate to ColumnUInt16 which has bulk copy optimization
        self.data.save_to_buffer(buffer)
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnDate::new(self.type_.clone()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        // Delegate to underlying column and wrap result
        let sliced_data = self.data.slice(begin, len)?;

        Ok(Arc::new(ColumnDate {
            type_: self.type_.clone(),
            data: sliced_data
                .as_any()
                .downcast_ref::<super::ColumnUInt16>()
                .map(|col| {
                    // Create new Arc from the sliced data
                    Arc::new(super::ColumnUInt16::from_vec(
                        Type::uint16(),
                        col.data().to_vec(),
                    ))
                })
                .expect("Slice should return ColumnUInt16"),
        }))
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
///
/// **C++ Implementation Pattern:**
/// Uses delegation to `ColumnInt32` for storage, matching the C++
/// clickhouse-cpp reference implementation's `std::shared_ptr<ColumnInt32>
/// data_` pattern.
pub struct ColumnDate32 {
    type_: Type,
    data: Arc<super::ColumnInt32>, /* Delegates to ColumnInt32, matches C++
                                    * pattern */
}

impl ColumnDate32 {
    pub fn new(type_: Type) -> Self {
        Self { type_, data: Arc::new(super::ColumnInt32::new(Type::int32())) }
    }

    pub fn with_data(mut self, data: Vec<i32>) -> Self {
        self.data =
            Arc::new(super::ColumnInt32::from_vec(Type::int32(), data));
        self
    }

    /// Append days since epoch (raw Int32 value)
    pub fn append(&mut self, days: i32) {
        Arc::get_mut(&mut self.data)
            .expect("Cannot append to shared column")
            .append(days);
    }

    /// Append from Unix timestamp (seconds since epoch)
    pub fn append_timestamp(&mut self, timestamp: i64) {
        let days = (timestamp / SECONDS_PER_DAY) as i32;
        self.append(days);
    }

    /// Get raw days value at index
    pub fn at(&self, index: usize) -> i32 {
        self.data.at(index)
    }

    /// Get Unix timestamp (seconds) at index
    pub fn timestamp_at(&self, index: usize) -> i64 {
        self.data.at(index) as i64 * SECONDS_PER_DAY
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get reference to underlying data column (for advanced use)
    pub fn data(&self) -> &super::ColumnInt32 {
        &self.data
    }
}

impl Column for ColumnDate32 {
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
        let other = other.as_any().downcast_ref::<ColumnDate32>().ok_or_else(
            || Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            },
        )?;

        // Delegate to underlying ColumnInt32
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
        // Delegate to ColumnInt32 which has bulk copy optimization
        Arc::get_mut(&mut self.data)
            .expect("Cannot load into shared column")
            .load_from_buffer(buffer, rows)
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Delegate to ColumnInt32 which has bulk copy optimization
        self.data.save_to_buffer(buffer)
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnDate32::new(self.type_.clone()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        // Delegate to underlying column and wrap result
        let sliced_data = self.data.slice(begin, len)?;

        Ok(Arc::new(ColumnDate32 {
            type_: self.type_.clone(),
            data: sliced_data
                .as_any()
                .downcast_ref::<super::ColumnInt32>()
                .map(|col| {
                    // Create new Arc from the sliced data
                    Arc::new(super::ColumnInt32::from_vec(
                        Type::int32(),
                        col.data().to_vec(),
                    ))
                })
                .expect("Slice should return ColumnInt32"),
        }))
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
///
/// **C++ Implementation Pattern:**
/// Uses delegation to `ColumnUInt32` for storage, matching the C++
/// clickhouse-cpp reference implementation's `std::shared_ptr<ColumnUInt32>
/// data_` pattern.
pub struct ColumnDateTime {
    type_: Type,
    data: Arc<super::ColumnUInt32>, /* Delegates to ColumnUInt32, matches
                                     * C++ pattern */
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
            data: Arc::new(super::ColumnUInt32::new(Type::uint32())),
            timezone,
        }
    }

    pub fn with_data(mut self, data: Vec<u32>) -> Self {
        self.data =
            Arc::new(super::ColumnUInt32::from_vec(Type::uint32(), data));
        self
    }

    /// Append Unix timestamp (seconds since epoch)
    pub fn append(&mut self, timestamp: u32) {
        Arc::get_mut(&mut self.data)
            .expect("Cannot append to shared column")
            .append(timestamp);
    }

    /// Get timestamp at index
    pub fn at(&self, index: usize) -> u32 {
        self.data.at(index)
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

    /// Get reference to underlying data column (for advanced use)
    pub fn data(&self) -> &super::ColumnUInt32 {
        &self.data
    }
}

impl Column for ColumnDateTime {
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
        let other = other
            .as_any()
            .downcast_ref::<ColumnDateTime>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
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
        Arc::new(ColumnDateTime::new(self.type_.clone()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        // Delegate to underlying column and wrap result
        let sliced_data = self.data.slice(begin, len)?;

        Ok(Arc::new(ColumnDateTime {
            type_: self.type_.clone(),
            timezone: self.timezone.clone(),
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

/// Column for DateTime64 type (stored as Int64 - subsecond precision)
/// Supports arbitrary sub-second precision, extended date range
///
/// **C++ Implementation Pattern:**
/// C++ uses delegation to `ColumnDecimal`, but we use `ColumnInt64` for
/// simplicity since DateTime64 is fundamentally stored as Int64.
pub struct ColumnDateTime64 {
    type_: Type,
    data: Arc<super::ColumnInt64>, // Delegates to ColumnInt64
    precision: usize,
    timezone: Option<String>,
}

impl ColumnDateTime64 {
    pub fn new(type_: Type) -> Self {
        let (precision, timezone) = match &type_ {
            Type::DateTime64 { precision, timezone } => {
                (*precision, timezone.clone())
            }
            _ => panic!("ColumnDateTime64 requires DateTime64 type"),
        };

        Self {
            type_,
            data: Arc::new(super::ColumnInt64::new(Type::int64())),
            precision,
            timezone,
        }
    }

    pub fn with_data(mut self, data: Vec<i64>) -> Self {
        self.data =
            Arc::new(super::ColumnInt64::from_vec(Type::int64(), data));
        self
    }

    /// Append timestamp with precision (e.g., for precision 3, value is
    /// milliseconds)
    pub fn append(&mut self, value: i64) {
        Arc::get_mut(&mut self.data)
            .expect("Cannot append to shared column")
            .append(value);
    }

    /// Get timestamp at index
    pub fn at(&self, index: usize) -> i64 {
        self.data.at(index)
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

    /// Get reference to underlying data column (for advanced use)
    pub fn data(&self) -> &super::ColumnInt64 {
        &self.data
    }
}

impl Column for ColumnDateTime64 {
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

        // Delegate to underlying ColumnInt64
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
        // Delegate to ColumnInt64 which has bulk copy optimization
        Arc::get_mut(&mut self.data)
            .expect("Cannot load into shared column")
            .load_from_buffer(buffer, rows)
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Delegate to ColumnInt64 which has bulk copy optimization
        self.data.save_to_buffer(buffer)
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnDateTime64::new(self.type_.clone()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        // Delegate to underlying column and wrap result
        let sliced_data = self.data.slice(begin, len)?;

        Ok(Arc::new(ColumnDateTime64 {
            type_: self.type_.clone(),
            precision: self.precision,
            timezone: self.timezone.clone(),
            data: sliced_data
                .as_any()
                .downcast_ref::<super::ColumnInt64>()
                .map(|col| {
                    // Create new Arc from the sliced data
                    Arc::new(super::ColumnInt64::from_vec(
                        Type::int64(),
                        col.data().to_vec(),
                    ))
                })
                .expect("Slice should return ColumnInt64"),
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
        let mut col =
            ColumnDateTime::new(Type::datetime(Some("UTC".to_string())));
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
