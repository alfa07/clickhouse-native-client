use super::{Column, ColumnRef};
use crate::types::Type;
use crate::{Error, Result};
use bytes::{BufMut, BytesMut};
use std::sync::Arc;

/// Column for Decimal types with precision and scale
/// Stores values as integers (scaled by 10^scale)
pub struct ColumnDecimal {
    type_: Type,
    precision: usize,
    scale: usize,
    data: Vec<i128>, // Store all decimals as i128 internally
}

impl ColumnDecimal {
    pub fn new(type_: Type) -> Self {
        let (precision, scale) = match &type_ {
            Type::Decimal { precision, scale } => (*precision, *scale),
            _ => panic!("ColumnDecimal requires Decimal type"),
        };

        Self {
            type_,
            precision,
            scale,
            data: Vec::new(),
        }
    }

    pub fn with_data(mut self, data: Vec<i128>) -> Self {
        self.data = data;
        self
    }

    /// Append decimal from string "123.45"
    pub fn append_from_string(&mut self, s: &str) -> Result<()> {
        let value = parse_decimal(s, self.scale)?;
        self.data.push(value);
        Ok(())
    }

    /// Append decimal from i128 (raw scaled value)
    pub fn append(&mut self, value: i128) {
        self.data.push(value);
    }

    /// Get decimal at index as i128 (raw scaled value)
    pub fn at(&self, index: usize) -> i128 {
        self.data[index]
    }

    /// Format decimal at index as string
    pub fn as_string(&self, index: usize) -> String {
        format_decimal(self.data[index], self.scale)
    }

    pub fn precision(&self) -> usize {
        self.precision
    }

    pub fn scale(&self) -> usize {
        self.scale
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Column for ColumnDecimal {
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
            .downcast_ref::<ColumnDecimal>()
            .ok_or_else(|| Error::TypeMismatch {
                expected: self.type_.name(),
                actual: other.column_type().name(),
            })?;

        if self.precision != other.precision || self.scale != other.scale {
            return Err(Error::TypeMismatch {
                expected: format!("Decimal({}, {})", self.precision, self.scale),
                actual: format!("Decimal({}, {})", other.precision, other.scale),
            });
        }

        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn load_from_buffer(&mut self, buffer: &mut &[u8], rows: usize) -> Result<()> {
        use bytes::Buf;

        self.data.reserve(rows);

        // Determine storage size based on precision
        let bytes_per_value = if self.precision <= 9 {
            4 // Int32
        } else if self.precision <= 18 {
            8 // Int64
        } else {
            16 // Int128
        };

        for _ in 0..rows {
            if buffer.len() < bytes_per_value {
                return Err(Error::Protocol("Not enough data for Decimal".to_string()));
            }

            let value = match bytes_per_value {
                4 => buffer.get_i32_le() as i128,
                8 => buffer.get_i64_le() as i128,
                16 => {
                    let low = buffer.get_u64_le() as u128;
                    let high = buffer.get_u64_le() as u128;
                    ((high << 64) | low) as i128
                }
                _ => unreachable!(),
            };

            self.data.push(value);
        }

        Ok(())
    }

    fn save_to_buffer(&self, buffer: &mut BytesMut) -> Result<()> {
        // Determine storage size based on precision
        let bytes_per_value = if self.precision <= 9 {
            4 // Int32
        } else if self.precision <= 18 {
            8 // Int64
        } else {
            16 // Int128
        };

        for &value in &self.data {
            match bytes_per_value {
                4 => buffer.put_i32_le(value as i32),
                8 => buffer.put_i64_le(value as i64),
                16 => {
                    let low = (value as u128) & 0xFFFFFFFFFFFFFFFF;
                    let high = ((value as u128) >> 64) & 0xFFFFFFFFFFFFFFFF;
                    buffer.put_u64_le(low as u64);
                    buffer.put_u64_le(high as u64);
                }
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    fn clone_empty(&self) -> ColumnRef {
        Arc::new(ColumnDecimal::new(self.type_.clone()))
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
            ColumnDecimal::new(self.type_.clone()).with_data(sliced_data),
        ))
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
    let (sign, s) = if s.starts_with('-') {
        (-1, &s[1..])
    } else if s.starts_with('+') {
        (1, &s[1..])
    } else {
        (1, s)
    };

    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() > 2 {
        return Err(Error::Protocol(format!(
            "Invalid decimal format: {}",
            s
        )));
    }

    let integer_part = parts[0]
        .parse::<i128>()
        .map_err(|e| Error::Protocol(format!("Invalid decimal integer part: {}", e)))?;

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

        padded
            .parse::<i128>()
            .map_err(|e| Error::Protocol(format!("Invalid decimal fractional part: {}", e)))?
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
    let (sign, abs_value) = if value < 0 {
        ("-", -value)
    } else {
        ("", value)
    };

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
}
