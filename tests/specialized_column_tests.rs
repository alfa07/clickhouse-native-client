use bytes::BytesMut;
use clickhouse_client::{
    column::*,
    types::{
        EnumItem,
        Type,
    },
};

// ============================================================================
// UUID Column Tests
// ============================================================================

#[test]
fn test_uuid_parse_and_format() {
    let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
    let uuid = Uuid::parse(uuid_str).unwrap();
    assert_eq!(uuid.as_string(), uuid_str);
}

#[test]
fn test_uuid_parse_uppercase() {
    let uuid_str = "550E8400-E29B-41D4-A716-446655440000";
    let uuid = Uuid::parse(uuid_str).unwrap();
    // as_string returns lowercase
    assert_eq!(uuid.as_string(), "550e8400-e29b-41d4-a716-446655440000");
}

#[test]
fn test_uuid_parse_invalid_format() {
    let result = Uuid::parse("invalid-uuid");
    assert!(result.is_err());
}

#[test]
fn test_uuid_column_append_and_retrieve() {
    let mut col = ColumnUuid::new(Type::uuid());
    let uuid1 = Uuid::parse("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let uuid2 = Uuid::parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();

    col.append(uuid1);
    col.append(uuid2);

    assert_eq!(col.len(), 2);
    assert_eq!(col.at(0), uuid1);
    assert_eq!(col.at(1), uuid2);
}

#[test]
fn test_uuid_column_append_from_string() {
    let mut col = ColumnUuid::new(Type::uuid());
    col.append_from_string("550e8400-e29b-41d4-a716-446655440000").unwrap();
    col.append_from_string("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap();

    assert_eq!(col.len(), 2);
    assert_eq!(col.as_string(0), "550e8400-e29b-41d4-a716-446655440000");
    assert_eq!(col.as_string(1), "6ba7b810-9dad-11d1-80b4-00c04fd430c8");
}

#[test]
fn test_uuid_column_slice() {
    let mut col = ColumnUuid::new(Type::uuid());
    let uuids = [
        Uuid::parse("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        Uuid::parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap(),
        Uuid::parse("00000000-0000-0000-0000-000000000000").unwrap(),
    ];

    for uuid in &uuids {
        col.append(*uuid);
    }

    let sliced = col.slice(1, 2).unwrap();
    let sliced_col = sliced.as_any().downcast_ref::<ColumnUuid>().unwrap();
    assert_eq!(sliced_col.len(), 2);
    assert_eq!(sliced_col.at(0), uuids[1]);
    assert_eq!(sliced_col.at(1), uuids[2]);
}

#[test]
fn test_uuid_column_serialization() {
    let mut col = ColumnUuid::new(Type::uuid());
    let uuid = Uuid::parse("550e8400-e29b-41d4-a716-446655440000").unwrap();
    col.append(uuid);

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();

    // UUID is saved as 16 bytes (2x u64 little-endian)
    assert_eq!(buffer.len(), 16);

    // Create new column and load from buffer
    let mut col2 = ColumnUuid::new(Type::uuid());
    let mut buffer_slice = &buffer[..];
    col2.load_from_buffer(&mut buffer_slice, 1).unwrap();

    assert_eq!(col2.len(), 1);
    assert_eq!(col2.at(0), uuid);
}

#[test]
fn test_uuid_zero() {
    let uuid = Uuid::parse("00000000-0000-0000-0000-000000000000").unwrap();
    assert_eq!(uuid.high, 0);
    assert_eq!(uuid.low, 0);
    assert_eq!(uuid.as_string(), "00000000-0000-0000-0000-000000000000");
}

// ============================================================================
// IPv4 Column Tests
// ============================================================================

#[test]
fn test_ipv4_parse_dotted_decimal() {
    let mut col = ColumnIpv4::new(Type::ipv4());
    col.append_from_string("192.168.1.1").unwrap();
    col.append_from_string("10.0.0.1").unwrap();
    col.append_from_string("255.255.255.255").unwrap();

    assert_eq!(col.len(), 3);
    assert_eq!(col.as_string(0), "192.168.1.1");
    assert_eq!(col.as_string(1), "10.0.0.1");
    assert_eq!(col.as_string(2), "255.255.255.255");
}

#[test]
fn test_ipv4_append_by_u32() {
    let mut col = ColumnIpv4::new(Type::ipv4());
    // 192.168.1.1 in network byte order
    let ip = (192u32 << 24) | (168u32 << 16) | (1u32 << 8) | 1u32;
    col.append(ip);

    assert_eq!(col.len(), 1);
    assert_eq!(col.as_string(0), "192.168.1.1");
}

#[test]
fn test_ipv4_localhost() {
    let mut col = ColumnIpv4::new(Type::ipv4());
    col.append_from_string("127.0.0.1").unwrap();

    assert_eq!(col.len(), 1);
    assert_eq!(col.as_string(0), "127.0.0.1");
}

#[test]
fn test_ipv4_zero() {
    let mut col = ColumnIpv4::new(Type::ipv4());
    col.append_from_string("0.0.0.0").unwrap();

    assert_eq!(col.len(), 1);
    assert_eq!(col.at(0), 0);
    assert_eq!(col.as_string(0), "0.0.0.0");
}

#[test]
fn test_ipv4_invalid_format() {
    let mut col = ColumnIpv4::new(Type::ipv4());

    // Too many octets
    assert!(col.append_from_string("192.168.1.1.1").is_err());

    // Too few octets
    assert!(col.append_from_string("192.168.1").is_err());

    // Invalid octet value
    assert!(col.append_from_string("256.1.1.1").is_err());
}

#[test]
fn test_ipv4_column_slice() {
    let mut col = ColumnIpv4::new(Type::ipv4());
    col.append_from_string("192.168.1.1").unwrap();
    col.append_from_string("192.168.1.2").unwrap();
    col.append_from_string("192.168.1.3").unwrap();

    let sliced = col.slice(1, 2).unwrap();
    let sliced_col = sliced.as_any().downcast_ref::<ColumnIpv4>().unwrap();

    assert_eq!(sliced_col.len(), 2);
    assert_eq!(sliced_col.as_string(0), "192.168.1.2");
    assert_eq!(sliced_col.as_string(1), "192.168.1.3");
}

#[test]
fn test_ipv4_serialization() {
    let mut col = ColumnIpv4::new(Type::ipv4());
    col.append_from_string("192.168.1.1").unwrap();
    col.append_from_string("10.0.0.1").unwrap();

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();

    // IPv4 is 4 bytes per address
    assert_eq!(buffer.len(), 8);

    let mut col2 = ColumnIpv4::new(Type::ipv4());
    let mut buffer_slice = &buffer[..];
    col2.load_from_buffer(&mut buffer_slice, 2).unwrap();

    assert_eq!(col2.len(), 2);
    assert_eq!(col2.as_string(0), "192.168.1.1");
    assert_eq!(col2.as_string(1), "10.0.0.1");
}

#[test]
fn test_ipv4_private_ranges() {
    let mut col = ColumnIpv4::new(Type::ipv4());
    col.append_from_string("10.0.0.0").unwrap(); // Class A private
    col.append_from_string("172.16.0.0").unwrap(); // Class B private
    col.append_from_string("192.168.0.0").unwrap(); // Class C private

    assert_eq!(col.len(), 3);
    assert_eq!(col.as_string(0), "10.0.0.0");
    assert_eq!(col.as_string(1), "172.16.0.0");
    assert_eq!(col.as_string(2), "192.168.0.0");
}

// ============================================================================
// IPv6 Column Tests
// ============================================================================

#[test]
fn test_ipv6_full_format() {
    let mut col = ColumnIpv6::new(Type::ipv6());
    col.append_from_string("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap();

    assert_eq!(col.len(), 1);
    // Should be compressed when formatted
    let formatted = col.as_string(0);
    assert!(formatted.contains("2001") && formatted.contains("7334"));
}

#[test]
fn test_ipv6_compressed_format() {
    let mut col = ColumnIpv6::new(Type::ipv6());
    col.append_from_string("2001:db8::1").unwrap();

    assert_eq!(col.len(), 1);
    let formatted = col.as_string(0);
    // Should preserve compression
    assert!(formatted.contains("::"));
}

#[test]
fn test_ipv6_localhost() {
    let mut col = ColumnIpv6::new(Type::ipv6());
    col.append_from_string("::1").unwrap();

    assert_eq!(col.len(), 1);
    assert_eq!(col.as_string(0), "::1");
}

#[test]
fn test_ipv6_zero_address() {
    let mut col = ColumnIpv6::new(Type::ipv6());
    col.append([0u8; 16]);

    assert_eq!(col.len(), 1);
    assert_eq!(col.as_string(0), "::");
}

#[test]
fn test_ipv6_link_local() {
    let mut col = ColumnIpv6::new(Type::ipv6());
    col.append_from_string("fe80::1").unwrap();

    assert_eq!(col.len(), 1);
    let formatted = col.as_string(0);
    assert!(formatted.starts_with("fe80"));
}

#[test]
fn test_ipv6_from_bytes() {
    let mut col = ColumnIpv6::new(Type::ipv6());
    let bytes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    col.append(bytes);

    assert_eq!(col.len(), 1);
    assert_eq!(col.at(0), bytes);
}

#[test]
fn test_ipv6_slice() {
    let mut col = ColumnIpv6::new(Type::ipv6());
    col.append_from_string("::1").unwrap();
    col.append_from_string("fe80::1").unwrap();
    col.append_from_string("2001:db8::1").unwrap();

    let sliced = col.slice(1, 2).unwrap();
    let sliced_col = sliced.as_any().downcast_ref::<ColumnIpv6>().unwrap();

    assert_eq!(sliced_col.len(), 2);
}

#[test]
fn test_ipv6_serialization() {
    let mut col = ColumnIpv6::new(Type::ipv6());
    col.append_from_string("2001:db8::1").unwrap();
    col.append_from_string("fe80::1").unwrap();

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();

    // IPv6 is 16 bytes per address
    assert_eq!(buffer.len(), 32);

    let mut col2 = ColumnIpv6::new(Type::ipv6());
    let mut buffer_slice = &buffer[..];
    col2.load_from_buffer(&mut buffer_slice, 2).unwrap();

    assert_eq!(col2.len(), 2);
    assert_eq!(col2.at(0), col.at(0));
    assert_eq!(col2.at(1), col.at(1));
}

// ============================================================================
// Decimal Column Tests
// ============================================================================

#[test]
fn test_decimal_parse_and_format() {
    let mut col = ColumnDecimal::new(Type::decimal(9, 2));
    col.append_from_string("123.45").unwrap();
    col.append_from_string("-56.78").unwrap();

    assert_eq!(col.len(), 2);
    assert_eq!(col.as_string(0), "123.45");
    assert_eq!(col.as_string(1), "-56.78");
}

#[test]
fn test_decimal_integer_values() {
    let mut col = ColumnDecimal::new(Type::decimal(9, 2));
    col.append_from_string("123").unwrap();

    assert_eq!(col.len(), 1);
    assert_eq!(col.as_string(0), "123.00");
}

#[test]
fn test_decimal_precision_scale() {
    let col = ColumnDecimal::new(Type::decimal(18, 4));
    assert_eq!(col.precision(), 18);
    assert_eq!(col.scale(), 4);
}

#[test]
fn test_decimal_small_values() {
    let mut col = ColumnDecimal::new(Type::decimal(9, 4));
    col.append_from_string("0.0001").unwrap();
    col.append_from_string("0.9999").unwrap();

    assert_eq!(col.as_string(0), "0.0001");
    assert_eq!(col.as_string(1), "0.9999");
}

#[test]
fn test_decimal_large_precision() {
    let mut col = ColumnDecimal::new(Type::decimal(38, 10));
    col.append_from_string("12345678901234567890.1234567890").unwrap();

    assert_eq!(col.len(), 1);
}

#[test]
fn test_decimal_zero_scale() {
    let mut col = ColumnDecimal::new(Type::decimal(9, 0));
    col.append_from_string("123").unwrap();

    assert_eq!(col.as_string(0), "123");
}

#[test]
fn test_decimal_negative_values() {
    let mut col = ColumnDecimal::new(Type::decimal(9, 2));
    col.append_from_string("-0.01").unwrap();
    col.append_from_string("-999.99").unwrap();

    assert_eq!(col.as_string(0), "-0.01");
    assert_eq!(col.as_string(1), "-999.99");
}

#[test]
fn test_decimal_append_raw() {
    let mut col = ColumnDecimal::new(Type::decimal(9, 2));
    col.append(12345); // Represents 123.45

    assert_eq!(col.len(), 1);
    assert_eq!(col.at(0), 12345);
    assert_eq!(col.as_string(0), "123.45");
}

#[test]
fn test_decimal_serialization_int32() {
    // Precision <= 9 uses Int32 storage (4 bytes)
    let mut col = ColumnDecimal::new(Type::decimal(9, 2));
    col.append_from_string("123.45").unwrap();

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();

    assert_eq!(buffer.len(), 4);

    let mut col2 = ColumnDecimal::new(Type::decimal(9, 2));
    let mut buffer_slice = &buffer[..];
    col2.load_from_buffer(&mut buffer_slice, 1).unwrap();

    assert_eq!(col2.as_string(0), "123.45");
}

#[test]
fn test_decimal_serialization_int64() {
    // Precision 10-18 uses Int64 storage (8 bytes)
    let mut col = ColumnDecimal::new(Type::decimal(18, 4));
    col.append_from_string("123456789012.3456").unwrap();

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();

    assert_eq!(buffer.len(), 8);

    let mut col2 = ColumnDecimal::new(Type::decimal(18, 4));
    let mut buffer_slice = &buffer[..];
    col2.load_from_buffer(&mut buffer_slice, 1).unwrap();

    assert_eq!(col2.as_string(0), "123456789012.3456");
}

// ============================================================================
// Enum Column Tests
// ============================================================================

#[test]
fn test_enum8_append_by_value() {
    let items = [
        EnumItem { name: "Red".to_string(), value: 1 },
        EnumItem { name: "Green".to_string(), value: 2 },
        EnumItem { name: "Blue".to_string(), value: 3 },
    ];
    let mut col = ColumnEnum8::new(Type::enum8(items));

    col.append_value(1);
    col.append_value(2);
    col.append_value(3);

    assert_eq!(col.len(), 3);
    assert_eq!(col.at(0), 1);
    assert_eq!(col.at(1), 2);
    assert_eq!(col.at(2), 3);
}

#[test]
fn test_enum8_append_by_name() {
    let items = [
        EnumItem { name: "Red".to_string(), value: 1 },
        EnumItem { name: "Green".to_string(), value: 2 },
    ];
    let mut col = ColumnEnum8::new(Type::enum8(items));

    col.append_name("Red").unwrap();
    col.append_name("Green").unwrap();

    assert_eq!(col.len(), 2);
    assert_eq!(col.at(0), 1);
    assert_eq!(col.at(1), 2);
}

#[test]
fn test_enum8_name_lookup() {
    let items = [
        EnumItem { name: "Red".to_string(), value: 1 },
        EnumItem { name: "Green".to_string(), value: 2 },
    ];
    let mut col = ColumnEnum8::new(Type::enum8(items));

    col.append_value(1);
    col.append_value(2);

    assert_eq!(col.name_at(0), Some("Red"));
    assert_eq!(col.name_at(1), Some("Green"));
}

#[test]
fn test_enum8_invalid_name() {
    let items = [EnumItem { name: "Red".to_string(), value: 1 }];
    let mut col = ColumnEnum8::new(Type::enum8(items));

    let result = col.append_name("InvalidColor");
    assert!(result.is_err());
}

#[test]
fn test_enum8_serialization() {
    let items = [
        EnumItem { name: "Red".to_string(), value: 1 },
        EnumItem { name: "Green".to_string(), value: 2 },
    ];
    let mut col = ColumnEnum8::new(Type::enum8(items.clone()));

    col.append_value(1);
    col.append_value(2);

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();

    // Enum8 is 1 byte per value
    assert_eq!(buffer.len(), 2);

    let mut col2 = ColumnEnum8::new(Type::enum8(items));
    let mut buffer_slice = &buffer[..];
    col2.load_from_buffer(&mut buffer_slice, 2).unwrap();

    assert_eq!(col2.len(), 2);
    assert_eq!(col2.at(0), 1);
    assert_eq!(col2.at(1), 2);
}

#[test]
fn test_enum16_append_by_value() {
    let items = [
        EnumItem { name: "Small".to_string(), value: 100 },
        EnumItem { name: "Large".to_string(), value: 1000 },
    ];
    let mut col = ColumnEnum16::new(Type::enum16(items));

    col.append_value(100);
    col.append_value(1000);

    assert_eq!(col.len(), 2);
    assert_eq!(col.at(0), 100);
    assert_eq!(col.at(1), 1000);
}

#[test]
fn test_enum16_append_by_name() {
    let items = [
        EnumItem { name: "Small".to_string(), value: 100 },
        EnumItem { name: "Large".to_string(), value: 1000 },
    ];
    let mut col = ColumnEnum16::new(Type::enum16(items));

    col.append_name("Small").unwrap();
    col.append_name("Large").unwrap();

    assert_eq!(col.len(), 2);
    assert_eq!(col.name_at(0), Some("Small"));
    assert_eq!(col.name_at(1), Some("Large"));
}

#[test]
fn test_enum16_serialization() {
    let items = [
        EnumItem { name: "Small".to_string(), value: 100 },
        EnumItem { name: "Large".to_string(), value: 1000 },
    ];
    let mut col = ColumnEnum16::new(Type::enum16(items.clone()));

    col.append_value(100);
    col.append_value(1000);

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();

    // Enum16 is 2 bytes per value
    assert_eq!(buffer.len(), 4);

    let mut col2 = ColumnEnum16::new(Type::enum16(items));
    let mut buffer_slice = &buffer[..];
    col2.load_from_buffer(&mut buffer_slice, 2).unwrap();

    assert_eq!(col2.len(), 2);
    assert_eq!(col2.at(0), 100);
    assert_eq!(col2.at(1), 1000);
}

#[test]
fn test_enum16_negative_values() {
    let items = [
        EnumItem { name: "NegOne".to_string(), value: -1 },
        EnumItem { name: "Zero".to_string(), value: 0 },
        EnumItem { name: "PosOne".to_string(), value: 1 },
    ];
    let mut col = ColumnEnum16::new(Type::enum16(items));

    col.append_value(-1);
    col.append_value(0);
    col.append_value(1);

    assert_eq!(col.len(), 3);
    assert_eq!(col.name_at(0), Some("NegOne"));
    assert_eq!(col.name_at(1), Some("Zero"));
    assert_eq!(col.name_at(2), Some("PosOne"));
}

#[test]
fn test_enum_slice() {
    let items = [
        EnumItem { name: "A".to_string(), value: 1 },
        EnumItem { name: "B".to_string(), value: 2 },
        EnumItem { name: "C".to_string(), value: 3 },
    ];
    let mut col = ColumnEnum8::new(Type::enum8(items));

    col.append_value(1);
    col.append_value(2);
    col.append_value(3);

    let sliced = col.slice(1, 2).unwrap();
    let sliced_col = sliced.as_any().downcast_ref::<ColumnEnum8>().unwrap();

    assert_eq!(sliced_col.len(), 2);
    assert_eq!(sliced_col.at(0), 2);
    assert_eq!(sliced_col.at(1), 3);
}
