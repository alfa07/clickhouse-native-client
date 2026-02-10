use super::{
    Column,
    ColumnInt128,
    ColumnInt32,
    ColumnInt64,
    ColumnRef,
};
use crate::{
    types::Type,
    Error,
    Result,
};
use bytes::BytesMut;
use std::sync::Arc;

/// Column for Decimal types with precision and scale
/// Stores values as integers (scaled by 10^scale)
///
/// Uses efficient internal representation based on precision:
/// - precision <= 9: ColumnInt32 (4 bytes per value)
/// - precision <= 18: ColumnInt64 (8 bytes per value)
/// - precision > 18: ColumnInt128 (16 bytes per value)
pub struct ColumnDecimal {
    type_: Type,
    precision: usize,
    scale: usize,
    data: ColumnRef, // Internally delegates to ColumnInt32/Int64/Int128
}

impl ColumnDecimal {
    /// Create a new empty decimal column for the given `Decimal` type.
    ///
    /// # Panics
    ///
    /// Panics if `type_` is not a `Type::Decimal`.
    pub fn new(type_: Type) -> Self {
        let (precision, scale) = match &type_ {
            Type::Decimal { precision, scale } => (*precision, *scale),
            _ => panic!("ColumnDecimal requires Decimal type"),
        };

        // Choose appropriate storage type based on precision (like C++
        // implementation)
        let data: ColumnRef = if precision <= 9 {
            Arc::new(ColumnInt32::new())
        } else if precision <= 18 {
            Arc::new(ColumnInt64::new())
        } else {
            Arc::new(ColumnInt128::new())
        };

        Self { type_, precision, scale, data }
    }

    /// Set the column data from a vector of raw scaled `i128` values.
    pub fn with_data(mut self, data: Vec<i128>) -> Self {
        // Convert Vec<i128> to the appropriate column type
        if self.precision <= 9 {
            let mut col = ColumnInt32::new();
            for value in data {
                col.append(value as i32);
            }
            self.data = Arc::new(col);
        } else if self.precision <= 18 {
            let mut col = ColumnInt64::new();
            for value in data {
                col.append(value as i64);
            }
            self.data = Arc::new(col);
        } else {
            let mut col = ColumnInt128::new();
            for value in data {
                col.append(value);
            }
            self.data = Arc::new(col);
        }
        self
    }

    /// Append a decimal value parsed from a string like `"123.45"`.
    ///
    /// # Errors
    ///
    /// Returns an error if the string is not a valid decimal or the
    /// fractional part exceeds the column's scale.
    pub fn append_from_string(&mut self, s: &str) -> Result<()> {
        let value = parse_decimal(s, self.scale)?;
        self.append(value);
        Ok(())
    }

    /// Append decimal from i128 (raw scaled value)
    pub fn append(&mut self, value: i128) {
        // Delegate to the underlying column based on precision
        let data_mut =
            Arc::get_mut(&mut self.data).expect("Cannot modify shared column");

        if self.precision <= 9 {
            let col = data_mut
                .as_any_mut()
                .downcast_mut::<ColumnInt32>()
                .expect("Expected ColumnInt32");
            col.append(value as i32);
        } else if self.precision <= 18 {
            let col = data_mut
                .as_any_mut()
                .downcast_mut::<ColumnInt64>()
                .expect("Expected ColumnInt64");
            col.append(value as i64);
        } else {
            let col = data_mut
                .as_any_mut()
                .downcast_mut::<ColumnInt128>()
                .expect("Expected ColumnInt128");
            col.append(value);
        }
    }

    /// Get decimal at index as i128 (raw scaled value)
    pub fn at(&self, index: usize) -> i128 {
        if self.precision <= 9 {
            let col = self
                .data
                .as_any()
                .downcast_ref::<ColumnInt32>()
                .expect("Expected ColumnInt32");
            col.at(index) as i128
        } else if self.precision <= 18 {
            let col = self
                .data
                .as_any()
                .downcast_ref::<ColumnInt64>()
                .expect("Expected ColumnInt64");
            col.at(index) as i128
        } else {
            let col = self
                .data
                .as_any()
                .downcast_ref::<ColumnInt128>()
                .expect("Expected ColumnInt128");
            col.at(index)
        }
    }

    /// Format decimal at index as string
    pub fn as_string(&self, index: usize) -> String {
        format_decimal(self.at(index), self.scale)
    }

    /// Returns the precision (total number of digits) of this decimal column.
    pub fn precision(&self) -> usize {
        self.precision
    }

    /// Returns the scale (digits after the decimal point) of this decimal column.
    pub fn scale(&self) -> usize {
        self.scale
    }

    /// Returns the number of values in this column.
    pub fn len(&self) -> usize {
        self.data.size()
    }

    /// Returns `true` if the column contains no values.
    pub fn is_empty(&self) -> bool {
        self.data.size() == 0
    }
}

impl Column for ColumnDecimal {
    fn column_type(&self) -> &Type {
        &self.type_
    }

    fn size(&self) -> usize {
        self.data.size()
    }

    fn clear(&mut self) {
        let data_mut =
            Arc::get_mut(&mut self.data).expect("Cannot modify shared column");
        data_mut.clear();
    }

    fn reserve(&mut self, new_cap: usize) {
        let data_mut =
            Arc::get_mut(&mut self.data).expect("Cannot modify shared column");
        data_mut.reserve(new_cap);
    }

    fn append_column(&mut self, other: ColumnRef) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<ColumnDecimal>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            })?;

        if self.precision != other.precision || self.scale != other.scale {
            return Err(Error::TypeMismatch {
                expected: format!(
                    "Decimal({}, {})",
                    self.precision, self.scale
                ),
                actual: format!(
                    "Decimal({}, {})",
                    other.precision, other.scale
                ),
            });
        }

        // Delegate to underlying column's append_column
        let data_mut =
            Arc::get_mut(&mut self.data).expect("Cannot modify shared column");
        data_mut.append_column(other.data.clone())?;
        Ok(())
    }

    fn load_from_buffer(
        &mut self,
        buffer: &mut &[u8],
        rows: usize,
    ) -> Result<()> {
        // Delegate to the underlying column - it knows how to load its data
        // efficiently
        let data_mut =
            Arc::get_mut(&mut self.data).expect("Cannot modify shared column");
        data_mut.load_from_buffer(buffer, rows)
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Delegate to the underlying column - it knows how to save its data
        // efficiently
        self.data.save_to_buffer(buffer)
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnDecimal::new(self.type_.clone()))
    }

    fn slice(&self, begin: usize, len: usize) -> Result<ColumnRef> {
        if begin + len > self.data.size() {
            return Err(Error::InvalidArgument(format!(
                "Slice out of bounds: begin={}, len={}, size={}",
                begin,
                len,
                self.data.size()
            )));
        }

        // Create a new ColumnDecimal with the sliced underlying data
        let sliced_data = self.data.slice(begin, len)?;
        let mut result = ColumnDecimal::new(self.type_.clone());
        result.data = sliced_data;
        Ok(Arc::new(result))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Parse decimal string to scaled integer
/// "123.45" with scale 2 -> 12345
fn parse_decimal(s: &str, scale: usize) -> Result<i128> {
    let s = s.trim();
    let (sign, s) = if let Some(stripped) = s.strip_prefix('-') {
        (-1, stripped)
    } else if let Some(stripped) = s.strip_prefix('+') {
        (1, stripped)
    } else {
        (1, s)
    };

    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() > 2 {
        return Err(Error::Protocol(format!("Invalid decimal format: {}", s)));
    }

    let integer_part = parts[0].parse::<i128>().map_err(|e| {
        Error::Protocol(format!("Invalid decimal integer part: {}", e))
    })?;

    let fractional_part = if parts.len() == 2 {
        let frac_str = parts[1];
        if frac_str.len() > scale {
            return Err(Error::Protocol(format!(
                "Decimal fractional part exceeds scale: {} > {}",
                frac_str.len(),
                scale
            )));
        }

        // Pad with zeros to match scale
        let mut padded = frac_str.to_string();
        while padded.len() < scale {
            padded.push('0');
        }

        padded.parse::<i128>().map_err(|e| {
            Error::Protocol(format!("Invalid decimal fractional part: {}", e))
        })?
    } else {
        0
    };

    // Calculate scaled value: integer_part * 10^scale + fractional_part
    let scale_multiplier = 10_i128.pow(scale as u32);
    let scaled_value = integer_part * scale_multiplier + fractional_part;

    Ok(sign * scaled_value)
}

/// Format scaled integer to decimal string
/// 12345 with scale 2 -> "123.45"
fn format_decimal(value: i128, scale: usize) -> String {
    let (sign, abs_value) =
        if value < 0 { ("-", -value) } else { ("", value) };

    let scale_divisor = 10_i128.pow(scale as u32);
    let integer_part = abs_value / scale_divisor;
    let fractional_part = abs_value % scale_divisor;

    if scale > 0 {
        format!(
            "{}{}.{:0width$}",
            sign,
            integer_part,
            fractional_part,
            width = scale
        )
    } else {
        format!("{}{}", sign, integer_part)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn test_parse_decimal() {
        assert_eq!(parse_decimal("123.45", 2).unwrap(), 12345);
        assert_eq!(parse_decimal("123", 2).unwrap(), 12300);
        assert_eq!(parse_decimal("0.5", 2).unwrap(), 50);
        assert_eq!(parse_decimal("-123.45", 2).unwrap(), -12345);
    }

    #[test]
    fn test_format_decimal() {
        assert_eq!(format_decimal(12345, 2), "123.45");
        assert_eq!(format_decimal(12300, 2), "123.00");
        assert_eq!(format_decimal(50, 2), "0.50");
        assert_eq!(format_decimal(-12345, 2), "-123.45");
        assert_eq!(format_decimal(123, 0), "123");
    }

    #[test]
    fn test_decimal_column() {
        let mut col = ColumnDecimal::new(Type::decimal(9, 2));
        col.append_from_string("123.45").unwrap();
        col.append_from_string("-56.78").unwrap();
        col.append_from_string("0.01").unwrap();

        assert_eq!(col.len(), 3);
        assert_eq!(col.as_string(0), "123.45");
        assert_eq!(col.as_string(1), "-56.78");
        assert_eq!(col.as_string(2), "0.01");
    }

    #[test]
    fn test_decimal_precision_scale() {
        let col = ColumnDecimal::new(Type::decimal(18, 4));
        assert_eq!(col.precision(), 18);
        assert_eq!(col.scale(), 4);
    }

    // Tests for efficient internal representation

    #[test]
    fn test_decimal_uses_int32_for_precision_9() {
        // Decimal with precision <= 9 should use ColumnInt32 internally
        let col = ColumnDecimal::new(Type::decimal(9, 2));

        // Verify the internal column is ColumnInt32
        assert!(col.data.as_any().is::<ColumnInt32>());

        // Verify we can downcast to ColumnInt32
        let int32_col = col.data.as_any().downcast_ref::<ColumnInt32>();
        assert!(int32_col.is_some(), "Expected ColumnInt32 for precision 9");
    }

    #[test]
    fn test_decimal_uses_int64_for_precision_18() {
        // Decimal with precision <= 18 should use ColumnInt64 internally
        let col = ColumnDecimal::new(Type::decimal(18, 4));

        // Verify the internal column is ColumnInt64
        assert!(col.data.as_any().is::<ColumnInt64>());

        // Verify we can downcast to ColumnInt64
        let int64_col = col.data.as_any().downcast_ref::<ColumnInt64>();
        assert!(int64_col.is_some(), "Expected ColumnInt64 for precision 18");
    }

    #[test]
    fn test_decimal_uses_int128_for_precision_38() {
        // Decimal with precision > 18 should use ColumnInt128 internally
        let col = ColumnDecimal::new(Type::decimal(38, 10));

        // Verify the internal column is ColumnInt128
        assert!(col.data.as_any().is::<ColumnInt128>());

        // Verify we can downcast to ColumnInt128
        let int128_col = col.data.as_any().downcast_ref::<ColumnInt128>();
        assert!(
            int128_col.is_some(),
            "Expected ColumnInt128 for precision 38"
        );
    }

    #[test]
    fn test_decimal_memory_efficiency() {
        // Verify memory efficiency: precision 9 should use 4 bytes per value
        let mut col9 = ColumnDecimal::new(Type::decimal(9, 2));
        for i in 0..1000 {
            col9.append(i * 100);
        }

        // Save to buffer and check size
        let mut buf9 = BytesMut::new();
        col9.save_to_buffer(&mut buf9).unwrap();
        assert_eq!(
            buf9.len(),
            1000 * 4,
            "Decimal(9,2) should use 4 bytes per value"
        );

        // Verify memory efficiency: precision 18 should use 8 bytes per value
        let mut col18 = ColumnDecimal::new(Type::decimal(18, 4));
        for i in 0..1000 {
            col18.append(i * 10000);
        }

        let mut buf18 = BytesMut::new();
        col18.save_to_buffer(&mut buf18).unwrap();
        assert_eq!(
            buf18.len(),
            1000 * 8,
            "Decimal(18,4) should use 8 bytes per value"
        );

        // Verify memory efficiency: precision 38 should use 16 bytes per value
        let mut col38 = ColumnDecimal::new(Type::decimal(38, 10));
        for i in 0..1000 {
            col38.append(i * 1000000000);
        }

        let mut buf38 = BytesMut::new();
        col38.save_to_buffer(&mut buf38).unwrap();
        assert_eq!(
            buf38.len(),
            1000 * 16,
            "Decimal(38,10) should use 16 bytes per value"
        );
    }

    #[test]
    fn test_decimal_bulk_copy_int32() {
        // Test bulk copy for Decimal using ColumnInt32 internally
        let mut col = ColumnDecimal::new(Type::decimal(9, 2));

        // Add test data
        let test_values = vec![12345, -67890, 0, 100, -200];
        for &val in &test_values {
            col.append(val);
        }

        // Save to buffer
        let mut buf = BytesMut::new();
        col.save_to_buffer(&mut buf).unwrap();

        // Load into new column (uses bulk copy internally)
        let mut col2 = ColumnDecimal::new(Type::decimal(9, 2));
        let mut reader = &buf[..];
        col2.load_from_buffer(&mut reader, test_values.len()).unwrap();

        // Verify all values match
        assert_eq!(col2.len(), test_values.len());
        for (i, &expected) in test_values.iter().enumerate() {
            assert_eq!(col2.at(i), expected, "Value mismatch at index {}", i);
        }
    }

    #[test]
    fn test_decimal_bulk_copy_int64() {
        // Test bulk copy for Decimal using ColumnInt64 internally
        let mut col = ColumnDecimal::new(Type::decimal(18, 4));

        // Add test data with larger values
        let test_values =
            vec![1234567890123, -9876543210987, 0, 100000000, -200000000];
        for &val in &test_values {
            col.append(val);
        }

        // Save to buffer
        let mut buf = BytesMut::new();
        col.save_to_buffer(&mut buf).unwrap();

        // Load into new column
        let mut col2 = ColumnDecimal::new(Type::decimal(18, 4));
        let mut reader = &buf[..];
        col2.load_from_buffer(&mut reader, test_values.len()).unwrap();

        // Verify all values match
        assert_eq!(col2.len(), test_values.len());
        for (i, &expected) in test_values.iter().enumerate() {
            assert_eq!(col2.at(i), expected, "Value mismatch at index {}", i);
        }
    }

    #[test]
    fn test_decimal_bulk_copy_int128() {
        // Test bulk copy for Decimal using ColumnInt128 internally
        let mut col = ColumnDecimal::new(Type::decimal(38, 10));

        // Add test data with very large values
        let test_values = vec![
            123456789012345678901234567890_i128,
            -987654321098765432109876543210_i128,
            0,
            1000000000000000000,
            -2000000000000000000,
        ];
        for &val in &test_values {
            col.append(val);
        }

        // Save to buffer
        let mut buf = BytesMut::new();
        col.save_to_buffer(&mut buf).unwrap();

        // Load into new column
        let mut col2 = ColumnDecimal::new(Type::decimal(38, 10));
        let mut reader = &buf[..];
        col2.load_from_buffer(&mut reader, test_values.len()).unwrap();

        // Verify all values match
        assert_eq!(col2.len(), test_values.len());
        for (i, &expected) in test_values.iter().enumerate() {
            assert_eq!(col2.at(i), expected, "Value mismatch at index {}", i);
        }
    }

    #[test]
    fn test_decimal_bulk_copy_large_dataset() {
        // Test bulk copy with 10,000 elements
        let mut col = ColumnDecimal::new(Type::decimal(9, 2));

        for i in 0..10_000 {
            col.append(i * 100);
        }

        // Save to buffer
        let mut buf = BytesMut::new();
        col.save_to_buffer(&mut buf).unwrap();

        // Load into new column (uses efficient bulk copy)
        let mut col2 = ColumnDecimal::new(Type::decimal(9, 2));
        let mut reader = &buf[..];
        col2.load_from_buffer(&mut reader, 10_000).unwrap();

        // Verify size and spot check values
        assert_eq!(col2.len(), 10_000);
        assert_eq!(col2.at(0), 0);
        assert_eq!(col2.at(5_000), 5_000 * 100);
        assert_eq!(col2.at(9_999), 9_999 * 100);
    }

    #[test]
    fn test_decimal_append_column() {
        // Test appending one decimal column to another
        let mut col1 = ColumnDecimal::new(Type::decimal(9, 2));
        col1.append(12345);
        col1.append(67890);

        let mut col2 = ColumnDecimal::new(Type::decimal(9, 2));
        col2.append(11111);
        col2.append(22222);

        // Append col2 to col1
        col1.append_column(Arc::new(col2)).unwrap();

        assert_eq!(col1.len(), 4);
        assert_eq!(col1.at(0), 12345);
        assert_eq!(col1.at(1), 67890);
        assert_eq!(col1.at(2), 11111);
        assert_eq!(col1.at(3), 22222);
    }

    #[test]
    fn test_decimal_slice() {
        // Test slicing a decimal column
        let mut col = ColumnDecimal::new(Type::decimal(18, 4));
        for i in 0..10 {
            col.append(i * 10000);
        }

        let sliced = col.slice(2, 5).unwrap();
        assert_eq!(sliced.size(), 5);

        let sliced_concrete =
            sliced.as_any().downcast_ref::<ColumnDecimal>().unwrap();
        assert_eq!(sliced_concrete.at(0), 2 * 10000);
        assert_eq!(sliced_concrete.at(4), 6 * 10000);
    }

    #[test]
    fn test_decimal_clear_and_reuse() {
        // Test that clear() works correctly and column can be reused
        let mut col = ColumnDecimal::new(Type::decimal(9, 2));
        col.append(100);
        col.append(200);
        assert_eq!(col.len(), 2);

        col.clear();
        assert_eq!(col.len(), 0);
        assert!(col.is_empty());

        // Reuse after clear
        col.append(300);
        col.append(400);
        assert_eq!(col.len(), 2);
        assert_eq!(col.at(0), 300);
        assert_eq!(col.at(1), 400);
    }

    #[test]
    fn test_decimal_with_data_constructor() {
        // Test the with_data constructor with different precisions
        let data = vec![100, 200, 300];

        // Precision 9 (uses Int32)
        let col9 =
            ColumnDecimal::new(Type::decimal(9, 2)).with_data(data.clone());
        assert_eq!(col9.len(), 3);
        assert_eq!(col9.at(0), 100);
        assert!(col9.data.as_any().is::<ColumnInt32>());

        // Precision 18 (uses Int64)
        let col18 =
            ColumnDecimal::new(Type::decimal(18, 4)).with_data(data.clone());
        assert_eq!(col18.len(), 3);
        assert_eq!(col18.at(0), 100);
        assert!(col18.data.as_any().is::<ColumnInt64>());

        // Precision 38 (uses Int128)
        let col38 =
            ColumnDecimal::new(Type::decimal(38, 10)).with_data(data.clone());
        assert_eq!(col38.len(), 3);
        assert_eq!(col38.at(0), 100);
        assert!(col38.data.as_any().is::<ColumnInt128>());
    }
}
