// Type methods tests - test Type helper methods like name(), enum helpers, etc.

use clickhouse_client::types::{Type, EnumItem};

// ============================================================================
// Type Name Generation Tests
// ============================================================================

#[test]
fn test_type_name_date() {
    let t = Type::date();
    assert_eq!(t.name(), "Date");
}

#[test]
fn test_type_name_array() {
    let t = Type::array(Type::int32());
    assert_eq!(t.name(), "Array(Int32)");
}

#[test]
fn test_type_name_nullable() {
    let t = Type::nullable(Type::int32());
    assert_eq!(t.name(), "Nullable(Int32)");
}

#[test]
fn test_type_name_tuple() {
    let t = Type::tuple(vec![Type::int32(), Type::string()]);
    assert_eq!(t.name(), "Tuple(Int32, String)");
}

#[test]
fn test_type_name_enum8_single() {
    let t = Type::enum8(vec![EnumItem {
        name: "One".to_string(),
        value: 1,
    }]);
    assert_eq!(t.name(), "Enum8('One' = 1)");
}

#[test]
fn test_type_name_enum8_empty() {
    let t = Type::enum8(vec![]);
    assert_eq!(t.name(), "Enum8()");
}

#[test]
fn test_type_name_enum8_multiple() {
    let t = Type::enum8(vec![
        EnumItem {
            name: "One".to_string(),
            value: 1,
        },
        EnumItem {
            name: "Two".to_string(),
            value: 2,
        },
    ]);
    assert_eq!(t.name(), "Enum8('One' = 1, 'Two' = 2)");
}

#[test]
fn test_type_name_map() {
    let t = Type::map(Type::int32(), Type::string());
    assert_eq!(t.name(), "Map(Int32, String)");
}

#[test]
fn test_type_name_fixed_string() {
    let t = Type::fixed_string(10);
    assert_eq!(t.name(), "FixedString(10)");
}

#[test]
fn test_type_name_datetime_with_timezone() {
    let t = Type::datetime(Some("UTC".to_string()));
    assert_eq!(t.name(), "DateTime('UTC')");
}

#[test]
fn test_type_name_datetime64() {
    let t = Type::datetime64(3, Some("UTC".to_string()));
    assert_eq!(t.name(), "DateTime64(3, 'UTC')");
}

#[test]
fn test_type_name_decimal() {
    let t = Type::decimal(9, 3);
    assert_eq!(t.name(), "Decimal(9, 3)");
}

#[test]
fn test_type_name_simple_types() {
    assert_eq!(Type::int8().name(), "Int8");
    assert_eq!(Type::int16().name(), "Int16");
    assert_eq!(Type::int32().name(), "Int32");
    assert_eq!(Type::int64().name(), "Int64");
    assert_eq!(Type::uint8().name(), "UInt8");
    assert_eq!(Type::uint16().name(), "UInt16");
    assert_eq!(Type::uint32().name(), "UInt32");
    assert_eq!(Type::uint64().name(), "UInt64");
    assert_eq!(Type::string().name(), "String");
    assert_eq!(Type::float32().name(), "Float32");
    assert_eq!(Type::float64().name(), "Float64");
}

#[test]
fn test_type_name_complex_nested() {
    // Array(Nullable(Tuple(String, Int8)))
    let t = Type::array(Type::nullable(Type::tuple(vec![
        Type::string(),
        Type::int8(),
    ])));
    assert_eq!(t.name(), "Array(Nullable(Tuple(String, Int8)))");
}

// ============================================================================
// Enum Helper Methods Tests
// ============================================================================

#[test]
fn test_enum8_has_enum_value() {
    let enum8 = Type::enum8(vec![
        EnumItem {
            name: "One".to_string(),
            value: 1,
        },
        EnumItem {
            name: "Two".to_string(),
            value: 2,
        },
    ]);

    assert_eq!(enum8.name(), "Enum8('One' = 1, 'Two' = 2)");
    assert!(enum8.has_enum_value(1));
    assert!(enum8.has_enum_value(2));
    assert!(!enum8.has_enum_value(10));
}

#[test]
fn test_enum8_has_enum_name() {
    let enum8 = Type::enum8(vec![
        EnumItem {
            name: "One".to_string(),
            value: 1,
        },
        EnumItem {
            name: "Two".to_string(),
            value: 2,
        },
    ]);

    assert!(enum8.has_enum_name("One"));
    assert!(enum8.has_enum_name("Two"));
    assert!(!enum8.has_enum_name("Ten"));
}

#[test]
fn test_enum8_get_enum_name() {
    let enum8 = Type::enum8(vec![
        EnumItem {
            name: "One".to_string(),
            value: 1,
        },
        EnumItem {
            name: "Two".to_string(),
            value: 2,
        },
    ]);

    assert_eq!(enum8.get_enum_name(1), Some("One"));
    assert_eq!(enum8.get_enum_name(2), Some("Two"));
    assert_eq!(enum8.get_enum_name(10), None);
}

#[test]
fn test_enum8_get_enum_value() {
    let enum8 = Type::enum8(vec![
        EnumItem {
            name: "One".to_string(),
            value: 1,
        },
        EnumItem {
            name: "Two".to_string(),
            value: 2,
        },
    ]);

    assert_eq!(enum8.get_enum_value("One"), Some(1));
    assert_eq!(enum8.get_enum_value("Two"), Some(2));
    assert_eq!(enum8.get_enum_value("Ten"), None);
}

#[test]
fn test_enum16_has_enum_value() {
    let enum16 = Type::enum16(vec![
        EnumItem {
            name: "Green".to_string(),
            value: 1,
        },
        EnumItem {
            name: "Red".to_string(),
            value: 2,
        },
        EnumItem {
            name: "Yellow".to_string(),
            value: 3,
        },
    ]);

    assert_eq!(
        enum16.name(),
        "Enum16('Green' = 1, 'Red' = 2, 'Yellow' = 3)"
    );
    assert!(enum16.has_enum_value(1));
    assert!(enum16.has_enum_value(2));
    assert!(enum16.has_enum_value(3));
    assert!(!enum16.has_enum_value(10));
}

#[test]
fn test_enum16_has_enum_name() {
    let enum16 = Type::enum16(vec![
        EnumItem {
            name: "Green".to_string(),
            value: 1,
        },
        EnumItem {
            name: "Red".to_string(),
            value: 2,
        },
        EnumItem {
            name: "Yellow".to_string(),
            value: 3,
        },
    ]);

    assert!(enum16.has_enum_name("Green"));
    assert!(enum16.has_enum_name("Red"));
    assert!(enum16.has_enum_name("Yellow"));
    assert!(!enum16.has_enum_name("Black"));
}

#[test]
fn test_enum16_get_enum_name() {
    let enum16 = Type::enum16(vec![
        EnumItem {
            name: "Green".to_string(),
            value: 1,
        },
        EnumItem {
            name: "Red".to_string(),
            value: 2,
        },
        EnumItem {
            name: "Yellow".to_string(),
            value: 3,
        },
    ]);

    assert_eq!(enum16.get_enum_name(1), Some("Green"));
    assert_eq!(enum16.get_enum_name(2), Some("Red"));
    assert_eq!(enum16.get_enum_name(3), Some("Yellow"));
    assert_eq!(enum16.get_enum_name(10), None);
}

#[test]
fn test_enum16_get_enum_value() {
    let enum16 = Type::enum16(vec![
        EnumItem {
            name: "Green".to_string(),
            value: 1,
        },
        EnumItem {
            name: "Red".to_string(),
            value: 2,
        },
        EnumItem {
            name: "Yellow".to_string(),
            value: 3,
        },
    ]);

    assert_eq!(enum16.get_enum_value("Green"), Some(1));
    assert_eq!(enum16.get_enum_value("Red"), Some(2));
    assert_eq!(enum16.get_enum_value("Yellow"), Some(3));
    assert_eq!(enum16.get_enum_value("Black"), None);
}

#[test]
fn test_enum_items() {
    let enum16 = Type::enum16(vec![
        EnumItem {
            name: "Green".to_string(),
            value: 1,
        },
        EnumItem {
            name: "Red".to_string(),
            value: 2,
        },
        EnumItem {
            name: "Yellow".to_string(),
            value: 3,
        },
    ]);

    let items = enum16.enum_items().unwrap();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].name, "Green");
    assert_eq!(items[0].value, 1);
    assert_eq!(items[1].name, "Red");
    assert_eq!(items[1].value, 2);
    assert_eq!(items[2].name, "Yellow");
    assert_eq!(items[2].value, 3);
}

#[test]
fn test_enum_methods_on_non_enum_type() {
    let t = Type::int32();

    assert!(!t.has_enum_value(1));
    assert!(!t.has_enum_name("One"));
    assert_eq!(t.get_enum_name(1), None);
    assert_eq!(t.get_enum_value("One"), None);
    assert_eq!(t.enum_items(), None);
}

#[test]
fn test_enum16_empty() {
    let enum16 = Type::enum16(vec![]);
    assert_eq!(enum16.name(), "Enum16()");

    assert!(!enum16.has_enum_value(1));
    assert!(!enum16.has_enum_name("One"));
    assert_eq!(enum16.get_enum_name(1), None);
    assert_eq!(enum16.get_enum_value("One"), None);

    let items = enum16.enum_items().unwrap();
    assert_eq!(items.len(), 0);
}

// ============================================================================
// Type Roundtrip: Parse -> Name
// ============================================================================

#[test]
fn test_type_parse_and_name_roundtrip() {
    let type_strings = vec![
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float64",
        "String",
        "Date",
        "FixedString(10)",
        "DateTime('UTC')",
        "DateTime64(3, 'UTC')",
        "Decimal(9, 3)",
        "Array(Int32)",
        "Nullable(String)",
        "Tuple(Int32, String)",
        "Map(String, Int32)",
    ];

    for type_str in type_strings {
        let parsed = Type::parse(type_str).unwrap();
        let generated_name = parsed.name();

        // Names should match (with potential normalization)
        // For example, spaces might differ: "Decimal(9,3)" vs "Decimal(9, 3)"
        // So we normalize by removing spaces for comparison
        let normalized_original = type_str.replace(" ", "");
        let normalized_generated = generated_name.replace(" ", "");

        assert_eq!(
            normalized_original, normalized_generated,
            "Mismatch for type: {}",
            type_str
        );
    }
}

#[test]
fn test_type_name_preserves_nested_structure() {
    // Complex nested type
    let t = Type::array(Type::array(Type::nullable(Type::tuple(vec![
        Type::string(),
        Type::int8(),
        Type::date(),
    ]))));

    let name = t.name();
    assert_eq!(name, "Array(Array(Nullable(Tuple(String, Int8, Date))))");

    // Parse it back
    let reparsed = Type::parse(&name).unwrap();
    assert_eq!(reparsed.name(), name);
}
