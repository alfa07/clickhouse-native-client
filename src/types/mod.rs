//! # ClickHouse Type System
//!
//! This module implements the ClickHouse type system for the native TCP
//! protocol.
//!
//! ## ClickHouse Documentation References
//!
//! ### Numeric Types
//! - [Integer Types](https://clickhouse.com/docs/en/sql-reference/data-types/int-uint)
//!   - Int8/16/32/64/128, UInt8/16/32/64/128
//! - [Floating-Point Types](https://clickhouse.com/docs/en/sql-reference/data-types/float)
//!   - Float32, Float64
//! - [Decimal Types](https://clickhouse.com/docs/en/sql-reference/data-types/decimal)
//!   - Decimal, Decimal32/64/128
//!
//! ### String Types
//! - [String](https://clickhouse.com/docs/en/sql-reference/data-types/string)
//!   - Variable-length strings
//! - [FixedString](https://clickhouse.com/docs/en/sql-reference/data-types/fixedstring)
//!   - Fixed-length binary strings
//!
//! ### Date and Time Types
//! - [Date](https://clickhouse.com/docs/en/sql-reference/data-types/date) -
//!   Days since 1970-01-01
//! - [Date32](https://clickhouse.com/docs/en/sql-reference/data-types/date32)
//!   - Extended date range
//! - [DateTime](https://clickhouse.com/docs/en/sql-reference/data-types/datetime)
//!   - Unix timestamp (UInt32)
//! - [DateTime64](https://clickhouse.com/docs/en/sql-reference/data-types/datetime64)
//!   - High precision timestamp (Int64)
//!
//! ### Compound Types
//! - [Array](https://clickhouse.com/docs/en/sql-reference/data-types/array) -
//!   Arrays of elements
//! - [Tuple](https://clickhouse.com/docs/en/sql-reference/data-types/tuple) -
//!   Fixed-size collections
//! - [Map](https://clickhouse.com/docs/en/sql-reference/data-types/map) -
//!   Key-value pairs
//!
//! ### Special Types
//! - [Nullable](https://clickhouse.com/docs/en/sql-reference/data-types/nullable)
//!   - Adds NULL support to any type
//! - [LowCardinality](https://clickhouse.com/docs/en/sql-reference/data-types/lowcardinality)
//!   - Dictionary encoding for compression
//! - [Enum8/Enum16](https://clickhouse.com/docs/en/sql-reference/data-types/enum)
//!   - Enumerated values
//! - [UUID](https://clickhouse.com/docs/en/sql-reference/data-types/uuid) -
//!   Universally unique identifiers
//! - [IPv4/IPv6](https://clickhouse.com/docs/en/sql-reference/data-types/ipv4)
//!   - IP addresses
//!
//! ### Geo Types
//! - [Point](https://clickhouse.com/docs/en/sql-reference/data-types/geo) - 2D
//!   point (Tuple(Float64, Float64))
//! - [Ring](https://clickhouse.com/docs/en/sql-reference/data-types/geo) -
//!   Array of Points
//! - [Polygon](https://clickhouse.com/docs/en/sql-reference/data-types/geo) -
//!   Array of Rings
//! - [MultiPolygon](https://clickhouse.com/docs/en/sql-reference/data-types/geo)
//!   - Array of Polygons
//!
//! ## Type Nesting Rules
//!
//! ClickHouse enforces strict type nesting rules (Error code 43:
//! `ILLEGAL_TYPE_OF_ARGUMENT`):
//!
//! **✅ Allowed:**
//! - `Array(Nullable(T))` - Array where each element can be NULL
//! - `LowCardinality(Nullable(T))` - Dictionary-encoded nullable values
//! - `Array(LowCardinality(T))` - Array of dictionary-encoded values
//! - `Array(LowCardinality(Nullable(T)))` - Combination of all three
//!
//! **❌ NOT Allowed:**
//! - `Nullable(Array(T))` - Arrays themselves cannot be NULL (use empty array
//!   instead)
//! - `Nullable(LowCardinality(T))` - Wrong nesting order
//! - `Nullable(Nullable(T))` - Double-nullable is invalid
//!
//! For more details, see the [column module documentation](crate::column).

mod parser;

pub use parser::{
    parse_type_name,
    TypeAst,
    TypeMeta,
};

use std::sync::Arc;

/// Trait for mapping Rust primitive types to ClickHouse types
/// Equivalent to C++ `Type::CreateSimple<T>()` template specializations
///
/// This trait allows type inference in column constructors, eliminating the
/// need to pass Type explicitly when creating typed columns.
///
/// # Examples
///
/// ```
/// use clickhouse_client::types::{Type, ToType};
///
/// assert_eq!(i32::to_type(), Type::int32());
/// assert_eq!(u64::to_type(), Type::uint64());
/// assert_eq!(f64::to_type(), Type::float64());
/// ```
pub trait ToType {
    /// Returns the corresponding ClickHouse [`Type`] for this Rust type.
    fn to_type() -> Type;
}

// Implement ToType for all primitive numeric types
impl ToType for i8 {
    fn to_type() -> Type {
        Type::int8()
    }
}

impl ToType for i16 {
    fn to_type() -> Type {
        Type::int16()
    }
}

impl ToType for i32 {
    fn to_type() -> Type {
        Type::int32()
    }
}

impl ToType for i64 {
    fn to_type() -> Type {
        Type::int64()
    }
}

impl ToType for i128 {
    fn to_type() -> Type {
        Type::int128()
    }
}

impl ToType for u8 {
    fn to_type() -> Type {
        Type::uint8()
    }
}

impl ToType for u16 {
    fn to_type() -> Type {
        Type::uint16()
    }
}

impl ToType for u32 {
    fn to_type() -> Type {
        Type::uint32()
    }
}

impl ToType for u64 {
    fn to_type() -> Type {
        Type::uint64()
    }
}

impl ToType for u128 {
    fn to_type() -> Type {
        Type::uint128()
    }
}

impl ToType for f32 {
    fn to_type() -> Type {
        Type::float32()
    }
}

impl ToType for f64 {
    fn to_type() -> Type {
        Type::float64()
    }
}

/// Type code enumeration matching ClickHouse types
///
/// Each variant represents a base type in ClickHouse. For parametric types
/// (like Array, Nullable, etc.), see the [`Type`] enum which includes
/// parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeCode {
    /// Nothing/Void type, used for NULL-only columns.
    Void = 0,
    /// Signed 8-bit integer (-128 to 127).
    Int8,
    /// Signed 16-bit integer (-32768 to 32767).
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// Unsigned 8-bit integer (0 to 255), also used as Bool.
    UInt8,
    /// Unsigned 16-bit integer (0 to 65535).
    UInt16,
    /// Unsigned 32-bit integer.
    UInt32,
    /// Unsigned 64-bit integer.
    UInt64,
    /// 32-bit IEEE 754 floating-point number.
    Float32,
    /// 64-bit IEEE 754 floating-point number.
    Float64,
    /// Variable-length byte string.
    String,
    /// Fixed-length byte string, padded with null bytes.
    FixedString,
    /// Date and time as a Unix timestamp (UInt32), with optional timezone.
    DateTime,
    /// Date stored as days since 1970-01-01 (UInt16).
    Date,
    /// Variable-length array of elements of a single type.
    Array,
    /// Wrapper type that adds NULL support to the nested type.
    Nullable,
    /// Fixed-size ordered collection of heterogeneous types.
    Tuple,
    /// Enumeration with Int8 storage (up to 128 values).
    Enum8,
    /// Enumeration with Int16 storage (up to 32768 values).
    Enum16,
    /// Universally unique identifier (128-bit).
    UUID,
    /// IPv4 address stored as UInt32.
    IPv4,
    /// IPv6 address stored as 16 bytes in network byte order.
    IPv6,
    /// Signed 128-bit integer.
    Int128,
    /// Unsigned 128-bit integer.
    UInt128,
    /// Arbitrary-precision decimal with configurable precision and scale.
    Decimal,
    /// Decimal with up to 9 digits of precision (stored as Int32).
    Decimal32,
    /// Decimal with up to 18 digits of precision (stored as Int64).
    Decimal64,
    /// Decimal with up to 38 digits of precision (stored as Int128).
    Decimal128,
    /// Dictionary-encoded column for low-cardinality data.
    LowCardinality,
    /// High-precision date and time stored as Int64, with sub-second precision.
    DateTime64,
    /// Extended date range stored as Int32 (days since 1970-01-01).
    Date32,
    /// Key-value pairs with typed keys and values.
    Map,
    /// 2D geographic point as Tuple(Float64, Float64).
    Point,
    /// Geographic ring as Array(Point).
    Ring,
    /// Geographic polygon as Array(Ring).
    Polygon,
    /// Collection of polygons as Array(Polygon).
    MultiPolygon,
}

impl TypeCode {
    /// Returns the ClickHouse type name string for this type code.
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

/// Enum item for Enum8/Enum16 types, mapping a name to its integer value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumItem {
    /// The string name of this enum variant.
    pub name: String,
    /// The integer value associated with this enum variant.
    pub value: i16,
}

/// ClickHouse type definition, representing both simple and parametric types.
#[derive(Debug, Clone)]
pub enum Type {
    /// A non-parametric type identified by its [`TypeCode`].
    Simple(TypeCode),
    /// Fixed-length byte string with the given size in bytes.
    FixedString {
        /// Length of the fixed string in bytes.
        size: usize,
    },
    /// Date and time with optional timezone.
    DateTime {
        /// Optional IANA timezone name (e.g. "UTC", "Europe/Moscow").
        timezone: Option<String>,
    },
    /// High-precision date and time with sub-second precision and optional timezone.
    DateTime64 {
        /// Number of sub-second decimal digits (0 to 18).
        precision: usize,
        /// Optional IANA timezone name.
        timezone: Option<String>,
    },
    /// Arbitrary-precision decimal with given precision and scale.
    Decimal {
        /// Total number of significant digits.
        precision: usize,
        /// Number of digits after the decimal point.
        scale: usize,
    },
    /// Enum with Int8 storage, containing named integer variants.
    Enum8 {
        /// The named variants with their integer values.
        items: Vec<EnumItem>,
    },
    /// Enum with Int16 storage, containing named integer variants.
    Enum16 {
        /// The named variants with their integer values.
        items: Vec<EnumItem>,
    },
    /// Variable-length array of the given element type.
    Array {
        /// The type of each element in the array.
        item_type: Box<Type>,
    },
    /// Nullable wrapper around the given nested type.
    Nullable {
        /// The type that is made nullable.
        nested_type: Box<Type>,
    },
    /// Fixed-size tuple of heterogeneous element types.
    Tuple {
        /// The ordered list of element types in the tuple.
        item_types: Vec<Type>,
    },
    /// Dictionary-encoded wrapper around the given nested type.
    LowCardinality {
        /// The type that is dictionary-encoded.
        nested_type: Box<Type>,
    },
    /// Key-value map with typed keys and values.
    Map {
        /// The type of map keys.
        key_type: Box<Type>,
        /// The type of map values.
        value_type: Box<Type>,
    },
}

impl Type {
    /// Returns the [`TypeCode`] for this type.
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

    /// Returns the full ClickHouse type name string, including parameters.
    pub fn name(&self) -> String {
        match self {
            Type::Simple(code) => code.name().to_string(),
            Type::FixedString { size } => format!("FixedString({})", size),
            Type::DateTime { timezone: None } => "DateTime".to_string(),
            Type::DateTime { timezone: Some(tz) } => {
                format!("DateTime('{}')", tz)
            }
            Type::DateTime64 { precision, timezone: None } => {
                format!("DateTime64({})", precision)
            }
            Type::DateTime64 { precision, timezone: Some(tz) } => {
                format!("DateTime64({}, '{}')", precision, tz)
            }
            Type::Decimal { precision, scale } => {
                format!("Decimal({}, {})", precision, scale)
            }
            Type::Enum8 { items } => {
                format!("Enum8({})", format_enum_items(items))
            }
            Type::Enum16 { items } => {
                format!("Enum16({})", format_enum_items(items))
            }
            Type::Array { item_type } => {
                format!("Array({})", item_type.name())
            }
            Type::Nullable { nested_type } => {
                format!("Nullable({})", nested_type.name())
            }
            Type::Tuple { item_types } => {
                let types: Vec<String> =
                    item_types.iter().map(|t| t.name()).collect();
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

    /// Returns the storage size in bytes for fixed-size types
    ///
    /// This is used for calculating buffer sizes when reading/writing
    /// uncompressed column data. Returns `None` for variable-length types.
    ///
    /// # Examples
    ///
    /// ```
    /// use clickhouse_client::types::Type;
    ///
    /// assert_eq!(Type::uint32().storage_size_bytes(), Some(4));
    /// assert_eq!(Type::uint64().storage_size_bytes(), Some(8));
    /// assert_eq!(Type::fixed_string(10).storage_size_bytes(), Some(10));
    /// assert_eq!(Type::string().storage_size_bytes(), None); // Variable length
    /// ```
    pub fn storage_size_bytes(&self) -> Option<usize> {
        match self {
            Type::Simple(code) => match code {
                TypeCode::Int8 | TypeCode::UInt8 => Some(1),
                TypeCode::Int16 | TypeCode::UInt16 => Some(2),
                TypeCode::Int32 | TypeCode::UInt32 | TypeCode::Float32 => {
                    Some(4)
                }
                TypeCode::Int64 | TypeCode::UInt64 | TypeCode::Float64 => {
                    Some(8)
                }
                TypeCode::Int128 | TypeCode::UInt128 | TypeCode::UUID => {
                    Some(16)
                }
                TypeCode::Date => Some(2),   // UInt16
                TypeCode::Date32 => Some(4), // Int32
                TypeCode::IPv4 => Some(4),
                TypeCode::IPv6 => Some(16),
                TypeCode::Point => Some(16), // 2x Float64
                TypeCode::String => None,    // Variable length
                _ => None,
            },
            Type::FixedString { size } => Some(*size),
            Type::DateTime { .. } => Some(4), // UInt32 timestamp
            Type::DateTime64 { .. } => Some(8), // Int64 timestamp
            Type::Enum8 { .. } => Some(1),    // Stored as Int8
            Type::Enum16 { .. } => Some(2),   // Stored as Int16
            Type::Decimal { precision, .. } => {
                // Decimal storage depends on precision
                if *precision <= 9 {
                    Some(4) // Decimal32
                } else if *precision <= 18 {
                    Some(8) // Decimal64
                } else {
                    Some(16) // Decimal128
                }
            }
            // Complex types don't have fixed storage size
            Type::Array { .. }
            | Type::Nullable { .. }
            | Type::Tuple { .. }
            | Type::LowCardinality { .. }
            | Type::Map { .. } => None,
        }
    }

    /// Creates an Int8 type.
    pub fn int8() -> Self {
        Type::Simple(TypeCode::Int8)
    }

    /// Creates an Int16 type.
    pub fn int16() -> Self {
        Type::Simple(TypeCode::Int16)
    }

    /// Creates an Int32 type.
    pub fn int32() -> Self {
        Type::Simple(TypeCode::Int32)
    }

    /// Creates an Int64 type.
    pub fn int64() -> Self {
        Type::Simple(TypeCode::Int64)
    }

    /// Creates an Int128 type.
    pub fn int128() -> Self {
        Type::Simple(TypeCode::Int128)
    }

    /// Creates a UInt8 type.
    pub fn uint8() -> Self {
        Type::Simple(TypeCode::UInt8)
    }

    /// Creates a UInt16 type.
    pub fn uint16() -> Self {
        Type::Simple(TypeCode::UInt16)
    }

    /// Creates a UInt32 type.
    pub fn uint32() -> Self {
        Type::Simple(TypeCode::UInt32)
    }

    /// Creates a UInt64 type.
    pub fn uint64() -> Self {
        Type::Simple(TypeCode::UInt64)
    }

    /// Creates a UInt128 type.
    pub fn uint128() -> Self {
        Type::Simple(TypeCode::UInt128)
    }

    /// Creates a Float32 type.
    pub fn float32() -> Self {
        Type::Simple(TypeCode::Float32)
    }

    /// Creates a Float64 type.
    pub fn float64() -> Self {
        Type::Simple(TypeCode::Float64)
    }

    /// Creates a variable-length String type.
    pub fn string() -> Self {
        Type::Simple(TypeCode::String)
    }

    /// Creates a FixedString type with the given size in bytes.
    pub fn fixed_string(size: usize) -> Self {
        Type::FixedString { size }
    }

    /// Creates a Date type (days since 1970-01-01, stored as UInt16).
    pub fn date() -> Self {
        Type::Simple(TypeCode::Date)
    }

    /// Creates a Date32 type (days since 1970-01-01, stored as Int32).
    pub fn date32() -> Self {
        Type::Simple(TypeCode::Date32)
    }

    /// Creates a DateTime type with an optional timezone.
    pub fn datetime(timezone: Option<String>) -> Self {
        Type::DateTime { timezone }
    }

    /// Creates a DateTime64 type with the given sub-second precision and optional timezone.
    pub fn datetime64(precision: usize, timezone: Option<String>) -> Self {
        Type::DateTime64 { precision, timezone }
    }

    /// Creates a Decimal type with the given precision and scale.
    pub fn decimal(precision: usize, scale: usize) -> Self {
        Type::Decimal { precision, scale }
    }

    /// Creates an IPv4 address type.
    pub fn ipv4() -> Self {
        Type::Simple(TypeCode::IPv4)
    }

    /// Creates an IPv6 address type.
    pub fn ipv6() -> Self {
        Type::Simple(TypeCode::IPv6)
    }

    /// Creates a UUID type.
    pub fn uuid() -> Self {
        Type::Simple(TypeCode::UUID)
    }

    /// Creates an Array type with the given element type.
    pub fn array(item_type: Type) -> Self {
        Type::Array { item_type: Box::new(item_type) }
    }

    /// Creates a Nullable wrapper around the given type.
    pub fn nullable(nested_type: Type) -> Self {
        Type::Nullable { nested_type: Box::new(nested_type) }
    }

    /// Creates a Tuple type with the given element types.
    pub fn tuple(item_types: Vec<Type>) -> Self {
        Type::Tuple { item_types }
    }

    /// Creates an Enum8 type with the given name-value items.
    pub fn enum8(items: Vec<EnumItem>) -> Self {
        Type::Enum8 { items }
    }

    /// Creates an Enum16 type with the given name-value items.
    pub fn enum16(items: Vec<EnumItem>) -> Self {
        Type::Enum16 { items }
    }

    /// Creates a LowCardinality wrapper around the given type.
    pub fn low_cardinality(nested_type: Type) -> Self {
        Type::LowCardinality { nested_type: Box::new(nested_type) }
    }

    /// Creates a Map type with the given key and value types.
    pub fn map(key_type: Type, value_type: Type) -> Self {
        Type::Map {
            key_type: Box::new(key_type),
            value_type: Box::new(value_type),
        }
    }

    /// Returns true if this enum type contains a variant with the given integer value.
    pub fn has_enum_value(&self, value: i16) -> bool {
        match self {
            Type::Enum8 { items } => {
                items.iter().any(|item| item.value == value)
            }
            Type::Enum16 { items } => {
                items.iter().any(|item| item.value == value)
            }
            _ => false,
        }
    }

    /// Returns true if this enum type contains a variant with the given name.
    pub fn has_enum_name(&self, name: &str) -> bool {
        match self {
            Type::Enum8 { items } => {
                items.iter().any(|item| item.name == name)
            }
            Type::Enum16 { items } => {
                items.iter().any(|item| item.name == name)
            }
            _ => false,
        }
    }

    /// Returns the enum variant name for the given integer value, if it exists.
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

    /// Returns the integer value for the given enum variant name, if it exists.
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

    /// Returns the enum items slice if this is an Enum8 or Enum16 type, or None otherwise.
    pub fn enum_items(&self) -> Option<&[EnumItem]> {
        match self {
            Type::Enum8 { items } => Some(items),
            Type::Enum16 { items } => Some(items),
            _ => None,
        }
    }

    /// Creates a Point geo type (Tuple(Float64, Float64)).
    pub fn point() -> Self {
        Type::Simple(TypeCode::Point)
    }

    /// Creates a Ring geo type (Array(Point)).
    pub fn ring() -> Self {
        Type::Simple(TypeCode::Ring)
    }

    /// Creates a Polygon geo type (Array(Ring)).
    pub fn polygon() -> Self {
        Type::Simple(TypeCode::Polygon)
    }

    /// Creates a MultiPolygon geo type (Array(Polygon)).
    pub fn multi_polygon() -> Self {
        Type::Simple(TypeCode::MultiPolygon)
    }

    /// Creates a Nothing/Void type, used for NULL-only columns.
    pub fn nothing() -> Self {
        Type::Simple(TypeCode::Void)
    }

    /// Create a Type from a Rust primitive type
    /// Equivalent to C++ `Type::CreateSimple<T>()`
    ///
    /// # Examples
    ///
    /// ```
    /// use clickhouse_client::types::Type;
    ///
    /// assert_eq!(Type::for_rust_type::<i32>(), Type::int32());
    /// assert_eq!(Type::for_rust_type::<u64>(), Type::uint64());
    /// assert_eq!(Type::for_rust_type::<f32>(), Type::float32());
    /// ```
    pub fn for_rust_type<T: ToType>() -> Self {
        T::to_type()
    }

    /// Convert TypeAst to Type
    /// Mirrors C++ CreateColumnFromAst logic
    pub fn from_ast(ast: &TypeAst) -> crate::Result<Self> {
        match ast.meta {
            TypeMeta::Terminal => {
                // Simple terminal types
                match ast.code {
                    TypeCode::Void
                    | TypeCode::Int8
                    | TypeCode::Int16
                    | TypeCode::Int32
                    | TypeCode::Int64
                    | TypeCode::Int128
                    | TypeCode::UInt8
                    | TypeCode::UInt16
                    | TypeCode::UInt32
                    | TypeCode::UInt64
                    | TypeCode::UInt128
                    | TypeCode::Float32
                    | TypeCode::Float64
                    | TypeCode::String
                    | TypeCode::Date
                    | TypeCode::Date32
                    | TypeCode::UUID
                    | TypeCode::IPv4
                    | TypeCode::IPv6
                    | TypeCode::Point
                    | TypeCode::Ring
                    | TypeCode::Polygon
                    | TypeCode::MultiPolygon => Ok(Type::Simple(ast.code)),

                    TypeCode::FixedString => {
                        // First element should be the size (Number)
                        if ast.elements.is_empty() {
                            return Err(crate::Error::Protocol(
                                "FixedString requires size parameter"
                                    .to_string(),
                            ));
                        }
                        let size = ast.elements[0].value as usize;
                        Ok(Type::FixedString { size })
                    }

                    TypeCode::DateTime => {
                        // Optional timezone parameter
                        if ast.elements.is_empty() {
                            Ok(Type::DateTime { timezone: None })
                        } else {
                            let timezone =
                                Some(ast.elements[0].value_string.clone());
                            Ok(Type::DateTime { timezone })
                        }
                    }

                    TypeCode::DateTime64 => {
                        // Precision + optional timezone
                        if ast.elements.is_empty() {
                            return Err(crate::Error::Protocol(
                                "DateTime64 requires precision parameter"
                                    .to_string(),
                            ));
                        }
                        let precision = ast.elements[0].value as usize;
                        let timezone = if ast.elements.len() > 1 {
                            Some(ast.elements[1].value_string.clone())
                        } else {
                            None
                        };
                        Ok(Type::DateTime64 { precision, timezone })
                    }

                    TypeCode::Decimal
                    | TypeCode::Decimal32
                    | TypeCode::Decimal64
                    | TypeCode::Decimal128 => {
                        if ast.elements.len() >= 2 {
                            let precision = ast.elements[0].value as usize;
                            let scale = ast.elements[1].value as usize;
                            Ok(Type::Decimal { precision, scale })
                        } else if ast.elements.len() == 1 {
                            // For Decimal32/64/128, scale may default to the
                            // last element
                            let scale = ast.elements[0].value as usize;
                            let precision = match ast.code {
                                TypeCode::Decimal32 => 9,
                                TypeCode::Decimal64 => 18,
                                TypeCode::Decimal128 => 38,
                                _ => scale,
                            };
                            Ok(Type::Decimal { precision, scale })
                        } else {
                            Err(crate::Error::Protocol(
                                "Decimal requires precision and scale parameters".to_string(),
                            ))
                        }
                    }

                    _ => Err(crate::Error::Protocol(format!(
                        "Unsupported terminal type: {:?}",
                        ast.code
                    ))),
                }
            }

            TypeMeta::Array => {
                if ast.elements.is_empty() {
                    return Err(crate::Error::Protocol(
                        "Array requires element type".to_string(),
                    ));
                }
                let item_type = Type::from_ast(&ast.elements[0])?;
                Ok(Type::Array { item_type: Box::new(item_type) })
            }

            TypeMeta::Nullable => {
                if ast.elements.is_empty() {
                    return Err(crate::Error::Protocol(
                        "Nullable requires nested type".to_string(),
                    ));
                }
                let nested_type = Type::from_ast(&ast.elements[0])?;
                Ok(Type::Nullable { nested_type: Box::new(nested_type) })
            }

            TypeMeta::Tuple => {
                let mut item_types = Vec::new();
                for elem in &ast.elements {
                    item_types.push(Type::from_ast(elem)?);
                }
                Ok(Type::Tuple { item_types })
            }

            TypeMeta::Enum => {
                // Enum elements are stored as: name1, value1, name2, value2,
                // ...
                let mut items = Vec::new();
                for i in (0..ast.elements.len()).step_by(2) {
                    if i + 1 >= ast.elements.len() {
                        break;
                    }
                    let name = ast.elements[i].value_string.clone();
                    let value = ast.elements[i + 1].value as i16;
                    items.push(EnumItem { name, value });
                }

                match ast.code {
                    TypeCode::Enum8 => Ok(Type::Enum8 { items }),
                    TypeCode::Enum16 => Ok(Type::Enum16 { items }),
                    _ => Err(crate::Error::Protocol(format!(
                        "Invalid enum type code: {:?}",
                        ast.code
                    ))),
                }
            }

            TypeMeta::LowCardinality => {
                if ast.elements.is_empty() {
                    return Err(crate::Error::Protocol(
                        "LowCardinality requires nested type".to_string(),
                    ));
                }
                let nested_type = Type::from_ast(&ast.elements[0])?;
                Ok(Type::LowCardinality { nested_type: Box::new(nested_type) })
            }

            TypeMeta::Map => {
                if ast.elements.len() != 2 {
                    return Err(crate::Error::Protocol(
                        "Map requires exactly 2 type parameters".to_string(),
                    ));
                }
                let key_type = Type::from_ast(&ast.elements[0])?;
                let value_type = Type::from_ast(&ast.elements[1])?;
                Ok(Type::Map {
                    key_type: Box::new(key_type),
                    value_type: Box::new(value_type),
                })
            }

            TypeMeta::SimpleAggregateFunction => {
                // SimpleAggregateFunction(func, Type) -> unwrap to Type
                // Last element is the actual type
                if ast.elements.is_empty() {
                    return Err(crate::Error::Protocol(
                        "SimpleAggregateFunction requires type parameter"
                            .to_string(),
                    ));
                }
                let type_elem = ast.elements.last().unwrap();
                Type::from_ast(type_elem)
            }

            TypeMeta::Number
            | TypeMeta::String
            | TypeMeta::Assign
            | TypeMeta::Null => {
                // These are intermediate AST nodes, not actual types
                Err(crate::Error::Protocol(format!(
                    "Cannot convert AST meta {:?} to Type",
                    ast.meta
                )))
            }
        }
    }

    /// Parse a type from its string representation
    ///
    /// Uses token-based parser with AST caching for performance
    pub fn parse(type_str: &str) -> crate::Result<Self> {
        let ast = parse_type_name(type_str)?;
        Type::from_ast(&ast)
    }

    /// Parse a type from its string representation (old implementation for
    /// fallback)
    #[allow(dead_code)]
    fn parse_old(type_str: &str) -> crate::Result<Self> {
        let type_str = type_str.trim();

        // Handle empty/whitespace-only strings
        if type_str.is_empty() {
            return Err(crate::Error::Protocol(
                "Empty type string".to_string(),
            ));
        }

        // Find the first '(' to split type name from parameters
        if let Some(paren_pos) = type_str.find('(') {
            if !type_str.ends_with(')') {
                return Err(crate::Error::Protocol(format!(
                    "Mismatched parentheses in type: {}",
                    type_str
                )));
            }

            let type_name = &type_str[..paren_pos];
            let params_str = &type_str[paren_pos + 1..type_str.len() - 1];

            return match type_name {
                "Nullable" => Ok(Type::nullable(Type::parse(params_str)?)),
                "Array" => Ok(Type::array(Type::parse(params_str)?)),
                "FixedString" => {
                    let size = params_str.parse::<usize>().map_err(|_| {
                        crate::Error::Protocol(format!(
                            "Invalid FixedString size: {}",
                            params_str
                        ))
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
                        return Err(crate::Error::Protocol(
                            "DateTime64 requires precision parameter"
                                .to_string(),
                        ));
                    }
                    let precision =
                        params[0].parse::<usize>().map_err(|_| {
                            crate::Error::Protocol(format!(
                                "Invalid DateTime64 precision: {}",
                                params[0]
                            ))
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
                        return Err(crate::Error::Protocol(format!(
                            "Decimal requires 2 parameters, got {}",
                            params.len()
                        )));
                    }
                    let precision =
                        params[0].parse::<usize>().map_err(|_| {
                            crate::Error::Protocol(format!(
                                "Invalid Decimal precision: {}",
                                params[0]
                            ))
                        })?;
                    let scale = params[1].parse::<usize>().map_err(|_| {
                        crate::Error::Protocol(format!(
                            "Invalid Decimal scale: {}",
                            params[1]
                        ))
                    })?;
                    Ok(Type::decimal(precision, scale))
                }
                "Decimal32" | "Decimal64" | "Decimal128" => {
                    // Decimal32(7) - single precision parameter, scale
                    // defaults to 0
                    let precision =
                        params_str.parse::<usize>().map_err(|_| {
                            crate::Error::Protocol(format!(
                                "Invalid {} precision: {}",
                                type_name, params_str
                            ))
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
                        return Err(crate::Error::Protocol(format!(
                            "Map requires 2 type parameters, got {}",
                            params.len()
                        )));
                    }
                    let key_type = Type::parse(&params[0])?;
                    let value_type = Type::parse(&params[1])?;
                    Ok(Type::map(key_type, value_type))
                }
                "Tuple" => {
                    // Tuple(UInt8, String, Date)
                    let params = parse_comma_separated(params_str)?;
                    if params.is_empty() {
                        return Err(crate::Error::Protocol(
                            "Tuple requires at least one type parameter"
                                .to_string(),
                        ));
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
                    // First param is function name, second is type - we just
                    // care about the type
                    Type::parse(&params[1])
                }
                "AggregateFunction" => {
                    // AggregateFunction is not supported for reading
                    // Matches C++ client behavior which throws
                    // UnimplementedError These columns
                    // contain internal aggregation state which requires
                    // specialized deserialization logic for each aggregate
                    // function
                    Err(crate::Error::Protocol(
                        "AggregateFunction columns are not supported. Use SimpleAggregateFunction or finalize the aggregation with -State combinators.".to_string()
                    ))
                }
                _ => Err(crate::Error::Protocol(format!(
                    "Unknown parametric type: {}",
                    type_name
                ))),
            };
        }

        // Simple types without parameters
        match type_str {
            "UInt8" => Ok(Type::uint8()),
            "UInt16" => Ok(Type::uint16()),
            "UInt32" => Ok(Type::uint32()),
            "UInt64" => Ok(Type::uint64()),
            "UInt128" => Ok(Type::Simple(TypeCode::UInt128)),
            "Int8" => Ok(Type::int8()),
            "Int16" => Ok(Type::int16()),
            "Int32" => Ok(Type::int32()),
            "Int64" => Ok(Type::int64()),
            "Int128" => Ok(Type::Simple(TypeCode::Int128)),
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
            "Nothing" => Ok(Type::Simple(TypeCode::Void)), /* Nothing type for NULL columns */
            "Point" => Ok(Type::point()), // Point is Tuple(Float64, Float64)
            "Ring" => Ok(Type::ring()),   // Ring is Array(Point)
            "Polygon" => Ok(Type::polygon()), // Polygon is Array(Ring)
            "MultiPolygon" => Ok(Type::multi_polygon()), /* MultiPolygon is Array(Polygon) */
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
    if (s.starts_with('\'') && s.ends_with('\''))
        || (s.starts_with('"') && s.ends_with('"'))
    {
        Ok(s[1..s.len() - 1].to_string())
    } else {
        Err(crate::Error::Protocol(format!(
            "Expected quoted string, got: {}",
            s
        )))
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
            crate::Error::Protocol(format!(
                "Invalid enum value: {}",
                eq_parts[1]
            ))
        })?;

        items.push(EnumItem { name, value });
    }

    Ok(items)
}

impl PartialEq for Type {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Type::Simple(a), Type::Simple(b)) => a == b,
            (Type::FixedString { size: a }, Type::FixedString { size: b }) => {
                a == b
            }
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
            (Type::Array { item_type: a }, Type::Array { item_type: b }) => {
                a == b
            }
            (
                Type::Nullable { nested_type: a },
                Type::Nullable { nested_type: b },
            ) => a == b,
            (Type::Tuple { item_types: a }, Type::Tuple { item_types: b }) => {
                a == b
            }
            (
                Type::LowCardinality { nested_type: a },
                Type::LowCardinality { nested_type: b },
            ) => a == b,
            (
                Type::Map { key_type: k_a, value_type: v_a },
                Type::Map { key_type: k_b, value_type: v_b },
            ) => k_a == k_b && v_a == v_b,
            _ => false,
        }
    }
}

impl Eq for Type {}

/// Reference-counted shared pointer to a [`Type`].
pub type TypeRef = Arc<Type>;

fn format_enum_items(items: &[EnumItem]) -> String {
    let formatted: Vec<String> = items
        .iter()
        .map(|item| format!("'{}' = {}", item.name, item.value))
        .collect();
    formatted.join(", ")
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
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
