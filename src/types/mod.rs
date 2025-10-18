use std::sync::Arc;

/// Type code enumeration matching ClickHouse types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeCode {
    Void = 0,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    FixedString,
    DateTime,
    Date,
    Array,
    Nullable,
    Tuple,
    Enum8,
    Enum16,
    UUID,
    IPv4,
    IPv6,
    Int128,
    UInt128,
    Decimal,
    Decimal32,
    Decimal64,
    Decimal128,
    LowCardinality,
    DateTime64,
    Date32,
    Map,
    Point,
    Ring,
    Polygon,
    MultiPolygon,
}

impl TypeCode {
    pub fn name(&self) -> &'static str {
        match self {
            TypeCode::Void => "Void",
            TypeCode::Int8 => "Int8",
            TypeCode::Int16 => "Int16",
            TypeCode::Int32 => "Int32",
            TypeCode::Int64 => "Int64",
            TypeCode::UInt8 => "UInt8",
            TypeCode::UInt16 => "UInt16",
            TypeCode::UInt32 => "UInt32",
            TypeCode::UInt64 => "UInt64",
            TypeCode::Float32 => "Float32",
            TypeCode::Float64 => "Float64",
            TypeCode::String => "String",
            TypeCode::FixedString => "FixedString",
            TypeCode::DateTime => "DateTime",
            TypeCode::Date => "Date",
            TypeCode::Array => "Array",
            TypeCode::Nullable => "Nullable",
            TypeCode::Tuple => "Tuple",
            TypeCode::Enum8 => "Enum8",
            TypeCode::Enum16 => "Enum16",
            TypeCode::UUID => "UUID",
            TypeCode::IPv4 => "IPv4",
            TypeCode::IPv6 => "IPv6",
            TypeCode::Int128 => "Int128",
            TypeCode::UInt128 => "UInt128",
            TypeCode::Decimal => "Decimal",
            TypeCode::Decimal32 => "Decimal32",
            TypeCode::Decimal64 => "Decimal64",
            TypeCode::Decimal128 => "Decimal128",
            TypeCode::LowCardinality => "LowCardinality",
            TypeCode::DateTime64 => "DateTime64",
            TypeCode::Date32 => "Date32",
            TypeCode::Map => "Map",
            TypeCode::Point => "Point",
            TypeCode::Ring => "Ring",
            TypeCode::Polygon => "Polygon",
            TypeCode::MultiPolygon => "MultiPolygon",
        }
    }
}

/// Enum item for Enum8/Enum16 types
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumItem {
    pub name: String,
    pub value: i16,
}

/// Type definition
#[derive(Debug, Clone)]
pub enum Type {
    Simple(TypeCode),
    FixedString { size: usize },
    DateTime { timezone: Option<String> },
    DateTime64 { precision: usize, timezone: Option<String> },
    Decimal { precision: usize, scale: usize },
    Enum8 { items: Vec<EnumItem> },
    Enum16 { items: Vec<EnumItem> },
    Array { item_type: Box<Type> },
    Nullable { nested_type: Box<Type> },
    Tuple { item_types: Vec<Type> },
    LowCardinality { nested_type: Box<Type> },
    Map { key_type: Box<Type>, value_type: Box<Type> },
}

impl Type {
    pub fn code(&self) -> TypeCode {
        match self {
            Type::Simple(code) => *code,
            Type::FixedString { .. } => TypeCode::FixedString,
            Type::DateTime { .. } => TypeCode::DateTime,
            Type::DateTime64 { .. } => TypeCode::DateTime64,
            Type::Decimal { .. } => TypeCode::Decimal,
            Type::Enum8 { .. } => TypeCode::Enum8,
            Type::Enum16 { .. } => TypeCode::Enum16,
            Type::Array { .. } => TypeCode::Array,
            Type::Nullable { .. } => TypeCode::Nullable,
            Type::Tuple { .. } => TypeCode::Tuple,
            Type::LowCardinality { .. } => TypeCode::LowCardinality,
            Type::Map { .. } => TypeCode::Map,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Type::Simple(code) => code.name().to_string(),
            Type::FixedString { size } => format!("FixedString({})", size),
            Type::DateTime { timezone: None } => "DateTime".to_string(),
            Type::DateTime { timezone: Some(tz) } => format!("DateTime('{}')", tz),
            Type::DateTime64 { precision, timezone: None } => format!("DateTime64({})", precision),
            Type::DateTime64 { precision, timezone: Some(tz) } => {
                format!("DateTime64({}, '{}')", precision, tz)
            }
            Type::Decimal { precision, scale } => format!("Decimal({}, {})", precision, scale),
            Type::Enum8 { items } => format!("Enum8({})", format_enum_items(items)),
            Type::Enum16 { items } => format!("Enum16({})", format_enum_items(items)),
            Type::Array { item_type } => format!("Array({})", item_type.name()),
            Type::Nullable { nested_type } => format!("Nullable({})", nested_type.name()),
            Type::Tuple { item_types } => {
                let types: Vec<String> = item_types.iter().map(|t| t.name()).collect();
                format!("Tuple({})", types.join(", "))
            }
            Type::LowCardinality { nested_type } => {
                format!("LowCardinality({})", nested_type.name())
            }
            Type::Map { key_type, value_type } => {
                format!("Map({}, {})", key_type.name(), value_type.name())
            }
        }
    }

    // Factory methods for simple types
    pub fn int8() -> Self {
        Type::Simple(TypeCode::Int8)
    }

    pub fn int16() -> Self {
        Type::Simple(TypeCode::Int16)
    }

    pub fn int32() -> Self {
        Type::Simple(TypeCode::Int32)
    }

    pub fn int64() -> Self {
        Type::Simple(TypeCode::Int64)
    }

    pub fn int128() -> Self {
        Type::Simple(TypeCode::Int128)
    }

    pub fn uint8() -> Self {
        Type::Simple(TypeCode::UInt8)
    }

    pub fn uint16() -> Self {
        Type::Simple(TypeCode::UInt16)
    }

    pub fn uint32() -> Self {
        Type::Simple(TypeCode::UInt32)
    }

    pub fn uint64() -> Self {
        Type::Simple(TypeCode::UInt64)
    }

    pub fn uint128() -> Self {
        Type::Simple(TypeCode::UInt128)
    }

    pub fn float32() -> Self {
        Type::Simple(TypeCode::Float32)
    }

    pub fn float64() -> Self {
        Type::Simple(TypeCode::Float64)
    }

    pub fn string() -> Self {
        Type::Simple(TypeCode::String)
    }

    pub fn fixed_string(size: usize) -> Self {
        Type::FixedString { size }
    }

    pub fn date() -> Self {
        Type::Simple(TypeCode::Date)
    }

    pub fn date32() -> Self {
        Type::Simple(TypeCode::Date32)
    }

    pub fn datetime(timezone: Option<String>) -> Self {
        Type::DateTime { timezone }
    }

    pub fn datetime64(precision: usize, timezone: Option<String>) -> Self {
        Type::DateTime64 { precision, timezone }
    }

    pub fn decimal(precision: usize, scale: usize) -> Self {
        Type::Decimal { precision, scale }
    }

    pub fn ipv4() -> Self {
        Type::Simple(TypeCode::IPv4)
    }

    pub fn ipv6() -> Self {
        Type::Simple(TypeCode::IPv6)
    }

    pub fn uuid() -> Self {
        Type::Simple(TypeCode::UUID)
    }

    pub fn array(item_type: Type) -> Self {
        Type::Array {
            item_type: Box::new(item_type),
        }
    }

    pub fn nullable(nested_type: Type) -> Self {
        Type::Nullable {
            nested_type: Box::new(nested_type),
        }
    }

    pub fn tuple(item_types: Vec<Type>) -> Self {
        Type::Tuple { item_types }
    }

    pub fn enum8(items: Vec<EnumItem>) -> Self {
        Type::Enum8 { items }
    }

    pub fn enum16(items: Vec<EnumItem>) -> Self {
        Type::Enum16 { items }
    }

    pub fn low_cardinality(nested_type: Type) -> Self {
        Type::LowCardinality {
            nested_type: Box::new(nested_type),
        }
    }

    pub fn map(key_type: Type, value_type: Type) -> Self {
        Type::Map {
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
        }
    }

    pub fn point() -> Self {
        Type::Simple(TypeCode::Point)
    }

    pub fn ring() -> Self {
        Type::Simple(TypeCode::Ring)
    }

    pub fn polygon() -> Self {
        Type::Simple(TypeCode::Polygon)
    }

    pub fn multi_polygon() -> Self {
        Type::Simple(TypeCode::MultiPolygon)
    }

    /// Parse a type from its string representation
    /// This is a simplified parser for common types
    pub fn parse(type_str: &str) -> crate::Result<Self> {
        // Handle nullable
        if type_str.starts_with("Nullable(") && type_str.ends_with(")") {
            let inner = &type_str[9..type_str.len() - 1];
            return Ok(Type::nullable(Type::parse(inner)?));
        }

        // Handle arrays
        if type_str.starts_with("Array(") && type_str.ends_with(")") {
            let inner = &type_str[6..type_str.len() - 1];
            return Ok(Type::array(Type::parse(inner)?));
        }

        // Handle fixed strings
        if type_str.starts_with("FixedString(") && type_str.ends_with(")") {
            let size_str = &type_str[12..type_str.len() - 1];
            let size = size_str.parse::<usize>().map_err(|_| {
                crate::Error::Protocol(format!("Invalid FixedString size: {}", size_str))
            })?;
            return Ok(Type::fixed_string(size));
        }

        // Handle Enum8/Enum16
        // Format: Enum8('name' = value, 'name2' = value2, ...)
        // For now, we treat them as their underlying storage types (Int8/Int16)
        // Full enum parsing would require extracting the name-value pairs
        if type_str.starts_with("Enum8(") {
            return Ok(Type::Simple(TypeCode::Int8));
        }
        if type_str.starts_with("Enum16(") {
            return Ok(Type::Simple(TypeCode::Int16));
        }

        // Handle simple types
        match type_str {
            "UInt8" => Ok(Type::uint8()),
            "UInt16" => Ok(Type::uint16()),
            "UInt32" => Ok(Type::uint32()),
            "UInt64" => Ok(Type::uint64()),
            "Int8" => Ok(Type::int8()),
            "Int16" => Ok(Type::int16()),
            "Int32" => Ok(Type::int32()),
            "Int64" => Ok(Type::int64()),
            "Float32" => Ok(Type::float32()),
            "Float64" => Ok(Type::float64()),
            "String" => Ok(Type::string()),
            "Date" => Ok(Type::date()),
            "DateTime" => Ok(Type::datetime(None)),
            "UUID" => Ok(Type::uuid()),
            _ => Err(crate::Error::Protocol(format!(
                "Unknown type: {}",
                type_str
            ))),
        }
    }
}

impl PartialEq for Type {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Type::Simple(a), Type::Simple(b)) => a == b,
            (Type::FixedString { size: a }, Type::FixedString { size: b }) => a == b,
            (
                Type::DateTime { timezone: tz_a },
                Type::DateTime { timezone: tz_b },
            ) => tz_a == tz_b,
            (
                Type::DateTime64 { precision: p_a, timezone: tz_a },
                Type::DateTime64 { precision: p_b, timezone: tz_b },
            ) => p_a == p_b && tz_a == tz_b,
            (
                Type::Decimal { precision: p_a, scale: s_a },
                Type::Decimal { precision: p_b, scale: s_b },
            ) => p_a == p_b && s_a == s_b,
            (Type::Array { item_type: a }, Type::Array { item_type: b }) => a == b,
            (Type::Nullable { nested_type: a }, Type::Nullable { nested_type: b }) => a == b,
            (Type::Tuple { item_types: a }, Type::Tuple { item_types: b }) => a == b,
            (Type::LowCardinality { nested_type: a }, Type::LowCardinality { nested_type: b }) => {
                a == b
            }
            (
                Type::Map { key_type: k_a, value_type: v_a },
                Type::Map { key_type: k_b, value_type: v_b },
            ) => k_a == k_b && v_a == v_b,
            _ => false,
        }
    }
}

impl Eq for Type {}

pub type TypeRef = Arc<Type>;

fn format_enum_items(items: &[EnumItem]) -> String {
    let formatted: Vec<String> = items
        .iter()
        .map(|item| format!("'{}' = {}", item.name, item.value))
        .collect();
    formatted.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_code_name() {
        assert_eq!(TypeCode::Int32.name(), "Int32");
        assert_eq!(TypeCode::String.name(), "String");
        assert_eq!(TypeCode::DateTime.name(), "DateTime");
    }

    #[test]
    fn test_simple_type_name() {
        assert_eq!(Type::int32().name(), "Int32");
        assert_eq!(Type::uint64().name(), "UInt64");
        assert_eq!(Type::string().name(), "String");
    }

    #[test]
    fn test_fixed_string_type() {
        let t = Type::fixed_string(10);
        assert_eq!(t.code(), TypeCode::FixedString);
        assert_eq!(t.name(), "FixedString(10)");
    }

    #[test]
    fn test_array_type() {
        let t = Type::array(Type::int32());
        assert_eq!(t.code(), TypeCode::Array);
        assert_eq!(t.name(), "Array(Int32)");
    }

    #[test]
    fn test_nullable_type() {
        let t = Type::nullable(Type::string());
        assert_eq!(t.code(), TypeCode::Nullable);
        assert_eq!(t.name(), "Nullable(String)");
    }

    #[test]
    fn test_tuple_type() {
        let t = Type::tuple(vec![Type::int32(), Type::string()]);
        assert_eq!(t.code(), TypeCode::Tuple);
        assert_eq!(t.name(), "Tuple(Int32, String)");
    }

    #[test]
    fn test_map_type() {
        let t = Type::map(Type::string(), Type::int32());
        assert_eq!(t.code(), TypeCode::Map);
        assert_eq!(t.name(), "Map(String, Int32)");
    }

    #[test]
    fn test_datetime_with_timezone() {
        let t = Type::datetime(Some("UTC".to_string()));
        assert_eq!(t.name(), "DateTime('UTC')");
    }

    #[test]
    fn test_decimal_type() {
        let t = Type::decimal(10, 2);
        assert_eq!(t.name(), "Decimal(10, 2)");
    }

    #[test]
    fn test_type_equality() {
        assert_eq!(Type::int32(), Type::int32());
        assert_eq!(Type::array(Type::string()), Type::array(Type::string()));
        assert_ne!(Type::int32(), Type::int64());
        assert_ne!(Type::fixed_string(10), Type::fixed_string(20));
    }
}
