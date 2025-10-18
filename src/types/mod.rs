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

    // Enum helper methods
    pub fn has_enum_value(&self, value: i16) -> bool {
        match self {
            Type::Enum8 { items } => items.iter().any(|item| item.value == value),
            Type::Enum16 { items } => items.iter().any(|item| item.value == value),
            _ => false,
        }
    }

    pub fn has_enum_name(&self, name: &str) -> bool {
        match self {
            Type::Enum8 { items } => items.iter().any(|item| item.name == name),
            Type::Enum16 { items } => items.iter().any(|item| item.name == name),
            _ => false,
        }
    }

    pub fn get_enum_name(&self, value: i16) -> Option<&str> {
        match self {
            Type::Enum8 { items } => items
                .iter()
                .find(|item| item.value == value)
                .map(|item| item.name.as_str()),
            Type::Enum16 { items } => items
                .iter()
                .find(|item| item.value == value)
                .map(|item| item.name.as_str()),
            _ => None,
        }
    }

    pub fn get_enum_value(&self, name: &str) -> Option<i16> {
        match self {
            Type::Enum8 { items } => items
                .iter()
                .find(|item| item.name == name)
                .map(|item| item.value),
            Type::Enum16 { items } => items
                .iter()
                .find(|item| item.name == name)
                .map(|item| item.value),
            _ => None,
        }
    }

    pub fn enum_items(&self) -> Option<&[EnumItem]> {
        match self {
            Type::Enum8 { items } => Some(items),
            Type::Enum16 { items } => Some(items),
            _ => None,
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
    pub fn parse(type_str: &str) -> crate::Result<Self> {
        let type_str = type_str.trim();

        // Handle empty/whitespace-only strings
        if type_str.is_empty() {
            return Err(crate::Error::Protocol("Empty type string".to_string()));
        }

        // Find the first '(' to split type name from parameters
        if let Some(paren_pos) = type_str.find('(') {
            if !type_str.ends_with(')') {
                return Err(crate::Error::Protocol(format!("Mismatched parentheses in type: {}", type_str)));
            }

            let type_name = &type_str[..paren_pos];
            let params_str = &type_str[paren_pos + 1..type_str.len() - 1];

            return match type_name {
                "Nullable" => {
                    Ok(Type::nullable(Type::parse(params_str)?))
                }
                "Array" => {
                    Ok(Type::array(Type::parse(params_str)?))
                }
                "FixedString" => {
                    let size = params_str.parse::<usize>().map_err(|_| {
                        crate::Error::Protocol(format!("Invalid FixedString size: {}", params_str))
                    })?;
                    Ok(Type::fixed_string(size))
                }
                "DateTime" => {
                    // DateTime('UTC') or DateTime('Europe/Minsk')
                    let tz = parse_string_literal(params_str)?;
                    Ok(Type::datetime(Some(tz)))
                }
                "DateTime64" => {
                    // DateTime64(3, 'UTC') or DateTime64(3)
                    let params = parse_comma_separated(params_str)?;
                    if params.is_empty() {
                        return Err(crate::Error::Protocol("DateTime64 requires precision parameter".to_string()));
                    }
                    let precision = params[0].parse::<usize>().map_err(|_| {
                        crate::Error::Protocol(format!("Invalid DateTime64 precision: {}", params[0]))
                    })?;
                    let timezone = if params.len() > 1 {
                        Some(parse_string_literal(&params[1])?)
                    } else {
                        None
                    };
                    Ok(Type::datetime64(precision, timezone))
                }
                "Decimal" => {
                    // Decimal(12, 5)
                    let params = parse_comma_separated(params_str)?;
                    if params.len() != 2 {
                        return Err(crate::Error::Protocol(format!("Decimal requires 2 parameters, got {}", params.len())));
                    }
                    let precision = params[0].parse::<usize>().map_err(|_| {
                        crate::Error::Protocol(format!("Invalid Decimal precision: {}", params[0]))
                    })?;
                    let scale = params[1].parse::<usize>().map_err(|_| {
                        crate::Error::Protocol(format!("Invalid Decimal scale: {}", params[1]))
                    })?;
                    Ok(Type::decimal(precision, scale))
                }
                "Decimal32" | "Decimal64" | "Decimal128" => {
                    // Decimal32(7) - single precision parameter, scale defaults to 0
                    let precision = params_str.parse::<usize>().map_err(|_| {
                        crate::Error::Protocol(format!("Invalid {} precision: {}", type_name, params_str))
                    })?;
                    Ok(Type::decimal(precision, 0))
                }
                "Enum8" => {
                    // Enum8('red' = 1, 'green' = 2)
                    let items = parse_enum_items(params_str)?;
                    Ok(Type::enum8(items))
                }
                "Enum16" => {
                    // Enum16('red' = 1, 'green' = 2)
                    let items = parse_enum_items(params_str)?;
                    Ok(Type::enum16(items))
                }
                "LowCardinality" => {
                    Ok(Type::low_cardinality(Type::parse(params_str)?))
                }
                "Map" => {
                    // Map(Int32, String)
                    let params = parse_comma_separated(params_str)?;
                    if params.len() != 2 {
                        return Err(crate::Error::Protocol(format!("Map requires 2 type parameters, got {}", params.len())));
                    }
                    let key_type = Type::parse(&params[0])?;
                    let value_type = Type::parse(&params[1])?;
                    Ok(Type::map(key_type, value_type))
                }
                "Tuple" => {
                    // Tuple(UInt8, String, Date)
                    let params = parse_comma_separated(params_str)?;
                    if params.is_empty() {
                        return Err(crate::Error::Protocol("Tuple requires at least one type parameter".to_string()));
                    }
                    let mut item_types = Vec::new();
                    for param in params {
                        item_types.push(Type::parse(&param)?);
                    }
                    Ok(Type::tuple(item_types))
                }
                "SimpleAggregateFunction" => {
                    // SimpleAggregateFunction(func, Type) -> unwrap to Type
                    // Example: SimpleAggregateFunction(func, Int32) -> Int32
                    let params = parse_comma_separated(params_str)?;
                    if params.len() < 2 {
                        return Err(crate::Error::Protocol("SimpleAggregateFunction requires at least 2 parameters".to_string()));
                    }
                    // First param is function name, second is type - we just care about the type
                    Type::parse(&params[1])
                }
                _ => Err(crate::Error::Protocol(format!("Unknown parametric type: {}", type_name)))
            };
        }

        // Simple types without parameters
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
            "Date32" => Ok(Type::date32()),
            "DateTime" => Ok(Type::datetime(None)),
            "UUID" => Ok(Type::uuid()),
            "IPv4" => Ok(Type::ipv4()),
            "IPv6" => Ok(Type::ipv6()),
            "Bool" => Ok(Type::uint8()), // Bool is an alias for UInt8
            _ => Err(crate::Error::Protocol(format!(
                "Unknown type: {}",
                type_str
            ))),
        }
    }
}

// Helper functions for type parsing

/// Parse a string literal from 'quoted' or "quoted" format
fn parse_string_literal(s: &str) -> crate::Result<String> {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        Ok(s[1..s.len() - 1].to_string())
    } else {
        Err(crate::Error::Protocol(format!("Expected quoted string, got: {}", s)))
    }
}

/// Split comma-separated parameters, respecting nested parentheses
/// Example: "Int32, String" -> ["Int32", "String"]
/// Example: "Map(Int32, String), UInt64" -> ["Map(Int32, String)", "UInt64"]
fn parse_comma_separated(s: &str) -> crate::Result<Vec<String>> {
    let mut params = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;
    let mut in_quotes = false;
    let mut quote_char = '\0';

    for ch in s.chars() {
        match ch {
            '\'' | '"' if !in_quotes => {
                in_quotes = true;
                quote_char = ch;
                current.push(ch);
            }
            ch if in_quotes && ch == quote_char => {
                in_quotes = false;
                current.push(ch);
            }
            '(' if !in_quotes => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' if !in_quotes => {
                paren_depth -= 1;
                current.push(ch);
            }
            ',' if !in_quotes && paren_depth == 0 => {
                params.push(current.trim().to_string());
                current.clear();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    if !current.trim().is_empty() {
        params.push(current.trim().to_string());
    }

    Ok(params)
}

/// Parse enum items from string like "'red' = 1, 'green' = 2, 'blue' = 3"
fn parse_enum_items(s: &str) -> crate::Result<Vec<EnumItem>> {
    let mut items = Vec::new();
    let parts = parse_comma_separated(s)?;

    for part in parts {
        // Each part should be 'name' = value
        let eq_parts: Vec<&str> = part.split('=').collect();
        if eq_parts.len() != 2 {
            return Err(crate::Error::Protocol(format!(
                "Invalid enum item format (expected 'name' = value): {}",
                part
            )));
        }

        let name = parse_string_literal(eq_parts[0].trim())?;
        let value = eq_parts[1].trim().parse::<i16>().map_err(|_| {
            crate::Error::Protocol(format!("Invalid enum value: {}", eq_parts[1]))
        })?;

        items.push(EnumItem { name, value });
    }

    Ok(items)
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
            (Type::Enum8 { items: a }, Type::Enum8 { items: b }) => a == b,
            (Type::Enum16 { items: a }, Type::Enum16 { items: b }) => a == b,
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
