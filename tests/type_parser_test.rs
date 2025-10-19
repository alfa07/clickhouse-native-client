// Type parser tests ported from clickhouse-cpp ut/type_parser_ut.cpp
// These tests verify that we can correctly parse all ClickHouse type strings

use clickhouse_client::types::{
    Type,
    TypeCode,
};

#[test]
fn test_parse_terminals() {
    let t = Type::parse("UInt8").expect("Failed to parse UInt8");
    assert_eq!(t.code(), TypeCode::UInt8);
    assert_eq!(t.name(), "UInt8");

    let t = Type::parse("Int32").expect("Failed to parse Int32");
    assert_eq!(t.code(), TypeCode::Int32);

    let t = Type::parse("String").expect("Failed to parse String");
    assert_eq!(t.code(), TypeCode::String);

    let t = Type::parse("Float64").expect("Failed to parse Float64");
    assert_eq!(t.code(), TypeCode::Float64);
}

#[test]
fn test_parse_fixed_string() {
    let t =
        Type::parse("FixedString(24)").expect("Failed to parse FixedString");
    assert_eq!(t.code(), TypeCode::FixedString);
    assert_eq!(t.name(), "FixedString(24)");

    match t {
        Type::FixedString { size } => {
            assert_eq!(size, 24);
        }
        _ => panic!("Expected FixedString type"),
    }
}

#[test]
fn test_parse_array() {
    let t = Type::parse("Array(Int32)").expect("Failed to parse Array");
    assert_eq!(t.code(), TypeCode::Array);

    match t {
        Type::Array { item_type } => {
            assert_eq!(item_type.code(), TypeCode::Int32);
            assert_eq!(item_type.name(), "Int32");
        }
        _ => panic!("Expected Array type"),
    }
}

#[test]
fn test_parse_nullable() {
    let t = Type::parse("Nullable(Date)").expect("Failed to parse Nullable");
    assert_eq!(t.code(), TypeCode::Nullable);

    match t {
        Type::Nullable { nested_type } => {
            assert_eq!(nested_type.code(), TypeCode::Date);
            assert_eq!(nested_type.name(), "Date");
        }
        _ => panic!("Expected Nullable type"),
    }
}

#[test]
fn test_parse_enum8_simple() {
    // Our current implementation simplifies Enum8 to Int8
    // We'll enhance it to actually parse enum values
    let t = Type::parse("Enum8('red' = 1, 'green' = 2, 'blue' = 3)")
        .expect("Failed to parse Enum8");

    match t {
        Type::Enum8 { items } => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0].name, "red");
            assert_eq!(items[0].value, 1);
            assert_eq!(items[1].name, "green");
            assert_eq!(items[1].value, 2);
            assert_eq!(items[2].name, "blue");
            assert_eq!(items[2].value, 3);
        }
        _ => panic!("Expected Enum8 type, got {:?}", t),
    }
}

#[test]
fn test_parse_enum8_complex() {
    // Test from C++: Enum8('COLOR_red_10_T' = -12, 'COLOR_green_20_T'=-25,
    // 'COLOR_blue_30_T'= 53, 'COLOR_black_30_T' = 107)
    let type_str = "Enum8('COLOR_red_10_T' = -12, 'COLOR_green_20_T'=-25, 'COLOR_blue_30_T'= 53, 'COLOR_black_30_T' = 107)";
    let t = Type::parse(type_str).expect("Failed to parse complex Enum8");

    match t {
        Type::Enum8 { items } => {
            assert_eq!(items.len(), 4);
            assert_eq!(items[0].name, "COLOR_red_10_T");
            assert_eq!(items[0].value, -12);
            assert_eq!(items[1].name, "COLOR_green_20_T");
            assert_eq!(items[1].value, -25);
            assert_eq!(items[2].name, "COLOR_blue_30_T");
            assert_eq!(items[2].value, 53);
            assert_eq!(items[3].name, "COLOR_black_30_T");
            assert_eq!(items[3].value, 107);
        }
        _ => panic!("Expected Enum8 type"),
    }
}

#[test]
fn test_parse_tuple() {
    let t =
        Type::parse("Tuple(UInt8, String)").expect("Failed to parse Tuple");
    assert_eq!(t.code(), TypeCode::Tuple);

    match t {
        Type::Tuple { item_types } => {
            assert_eq!(item_types.len(), 2);
            assert_eq!(item_types[0].code(), TypeCode::UInt8);
            assert_eq!(item_types[0].name(), "UInt8");
            assert_eq!(item_types[1].code(), TypeCode::String);
            assert_eq!(item_types[1].name(), "String");
        }
        _ => panic!("Expected Tuple type"),
    }
}

#[test]
fn test_parse_decimal() {
    let t = Type::parse("Decimal(12, 5)").expect("Failed to parse Decimal");
    assert_eq!(t.code(), TypeCode::Decimal);

    match t {
        Type::Decimal { precision, scale } => {
            assert_eq!(precision, 12);
            assert_eq!(scale, 5);
        }
        _ => panic!("Expected Decimal type"),
    }
}

#[test]
fn test_parse_decimal32() {
    let t = Type::parse("Decimal32(7)").expect("Failed to parse Decimal32");

    match t {
        Type::Decimal { precision, .. } => {
            // Decimal32 is represented as Decimal with default scale
            assert_eq!(precision, 7);
        }
        _ => panic!("Expected Decimal type"),
    }
}

#[test]
fn test_parse_decimal64() {
    let t = Type::parse("Decimal64(1)").expect("Failed to parse Decimal64");

    match t {
        Type::Decimal { precision, .. } => {
            assert_eq!(precision, 1);
        }
        _ => panic!("Expected Decimal type"),
    }
}

#[test]
fn test_parse_decimal128() {
    let t = Type::parse("Decimal128(3)").expect("Failed to parse Decimal128");

    match t {
        Type::Decimal { precision, .. } => {
            assert_eq!(precision, 3);
        }
        _ => panic!("Expected Decimal type"),
    }
}

#[test]
fn test_parse_datetime_no_timezone() {
    let t = Type::parse("DateTime").expect("Failed to parse DateTime");
    assert_eq!(t.code(), TypeCode::DateTime);
    assert_eq!(t.name(), "DateTime");

    match t {
        Type::DateTime { timezone } => {
            assert_eq!(timezone, None);
        }
        _ => panic!("Expected DateTime type"),
    }
}

#[test]
fn test_parse_datetime_utc() {
    let t = Type::parse("DateTime('UTC')").expect("Failed to parse DateTime");
    assert_eq!(t.code(), TypeCode::DateTime);

    match t {
        Type::DateTime { timezone } => {
            assert_eq!(timezone, Some("UTC".to_string()));
        }
        _ => panic!("Expected DateTime type"),
    }
}

#[test]
fn test_parse_datetime_europe_minsk() {
    let t = Type::parse("DateTime('Europe/Minsk')")
        .expect("Failed to parse DateTime");
    assert_eq!(t.code(), TypeCode::DateTime);

    match t {
        Type::DateTime { timezone } => {
            assert_eq!(timezone, Some("Europe/Minsk".to_string()));
        }
        _ => panic!("Expected DateTime type"),
    }
}

#[test]
fn test_parse_datetime64() {
    let t = Type::parse("DateTime64(3, 'UTC')")
        .expect("Failed to parse DateTime64");
    assert_eq!(t.code(), TypeCode::DateTime64);

    match t {
        Type::DateTime64 { precision, timezone } => {
            assert_eq!(precision, 3);
            assert_eq!(timezone, Some("UTC".to_string()));
        }
        _ => panic!("Expected DateTime64 type"),
    }
}

#[test]
fn test_parse_low_cardinality_string() {
    let t = Type::parse("LowCardinality(String)")
        .expect("Failed to parse LowCardinality");
    assert_eq!(t.code(), TypeCode::LowCardinality);

    match t {
        Type::LowCardinality { nested_type } => {
            assert_eq!(nested_type.code(), TypeCode::String);
            assert_eq!(nested_type.name(), "String");
        }
        _ => panic!("Expected LowCardinality type"),
    }
}

#[test]
fn test_parse_low_cardinality_fixed_string() {
    let t = Type::parse("LowCardinality(FixedString(10))")
        .expect("Failed to parse LowCardinality");
    assert_eq!(t.code(), TypeCode::LowCardinality);

    match t {
        Type::LowCardinality { nested_type } => {
            assert_eq!(nested_type.code(), TypeCode::FixedString);
            match nested_type.as_ref() {
                Type::FixedString { size } => {
                    assert_eq!(size, &10);
                }
                _ => panic!("Expected FixedString nested type"),
            }
        }
        _ => panic!("Expected LowCardinality type"),
    }
}

#[test]
fn test_parse_map() {
    let t = Type::parse("Map(Int32, String)").expect("Failed to parse Map");
    assert_eq!(t.code(), TypeCode::Map);

    match t {
        Type::Map { key_type, value_type } => {
            assert_eq!(key_type.code(), TypeCode::Int32);
            assert_eq!(key_type.name(), "Int32");
            assert_eq!(value_type.code(), TypeCode::String);
            assert_eq!(value_type.name(), "String");
        }
        _ => panic!("Expected Map type"),
    }
}

#[test]
fn test_parse_empty_name() {
    // Empty names should fail
    assert!(Type::parse("").is_err());
    assert!(Type::parse(" ").is_err());
}

#[test]
fn test_parse_complex_nested() {
    // Test complex nested types
    let t = Type::parse("Array(Nullable(String))").expect("Failed to parse");
    match t {
        Type::Array { item_type } => match &*item_type {
            Type::Nullable { nested_type } => {
                assert_eq!(nested_type.code(), TypeCode::String);
            }
            _ => panic!("Expected Nullable inner type"),
        },
        _ => panic!("Expected Array type"),
    }
}
