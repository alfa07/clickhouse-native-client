// Roundtrip tests - test serialization and deserialization of columns
// These test that data can be saved to a buffer and loaded back correctly

use bytes::BytesMut;
use clickhouse_client::{
    column::*,
    types::Type,
};
use std::sync::Arc;

// ============================================================================
// Test Helper Functions
// ============================================================================

/// Helper to roundtrip a column through serialization
fn roundtrip_column<T: Column + 'static>(
    col: &T,
) -> Result<Arc<dyn Column>, clickhouse_client::Error> {
    let mut buffer = BytesMut::new();

    // Save to buffer
    col.save_to_buffer(&mut buffer)?;

    // Create empty column of same type
    let mut result = col.clone_empty();

    // Load from buffer
    let mut reader = &buffer[..];
    if let Some(result_mut) = Arc::get_mut(&mut result) {
        result_mut.load_from_buffer(&mut reader, col.size())?;
    }

    Ok(result)
}

// ============================================================================
// Numeric Column Roundtrip Tests
// ============================================================================

#[test]
fn test_roundtrip_uint32() {
    let mut col = ColumnUInt32::new(Type::uint32());
    col.append(42);
    col.append(100);
    col.append(255);

    let result = roundtrip_column(&col).unwrap();
    let result_u32 = result.as_any().downcast_ref::<ColumnUInt32>().unwrap();

    assert_eq!(result_u32.len(), 3);
    assert_eq!(result_u32.at(0), 42);
    assert_eq!(result_u32.at(1), 100);
    assert_eq!(result_u32.at(2), 255);
}

#[test]
fn test_roundtrip_int64() {
    let mut col = ColumnInt64::new(Type::int64());
    col.append(-100);
    col.append(0);
    col.append(9223372036854775807); // i64::MAX

    let result = roundtrip_column(&col).unwrap();
    let result_i64 = result.as_any().downcast_ref::<ColumnInt64>().unwrap();

    assert_eq!(result_i64.len(), 3);
    assert_eq!(result_i64.at(0), -100);
    assert_eq!(result_i64.at(1), 0);
    assert_eq!(result_i64.at(2), 9223372036854775807);
}

#[test]
fn test_roundtrip_float32() {
    let mut col = ColumnFloat32::new(Type::float32());
    col.append(3.14);
    col.append(-2.71);
    col.append(0.0);

    let result = roundtrip_column(&col).unwrap();
    let result_f32 = result.as_any().downcast_ref::<ColumnFloat32>().unwrap();

    assert_eq!(result_f32.len(), 3);
    assert!((result_f32.at(0) - 3.14).abs() < 0.001);
    assert!((result_f32.at(1) - (-2.71)).abs() < 0.001);
    assert_eq!(result_f32.at(2), 0.0);
}

#[test]
fn test_roundtrip_uint8() {
    let data = vec![1u8, 2, 3, 5, 7, 11, 13, 17, 19, 23, 31];
    let col = ColumnUInt8::new(Type::uint8()).with_data(data.clone());

    let result = roundtrip_column(&col).unwrap();
    let result_u8 = result.as_any().downcast_ref::<ColumnUInt8>().unwrap();

    assert_eq!(result_u8.len(), data.len());
    for (i, &expected) in data.iter().enumerate() {
        assert_eq!(result_u8.at(i), expected);
    }
}

// ============================================================================
// String Column Roundtrip Tests
// ============================================================================

#[test]
fn test_roundtrip_string() {
    let mut col = ColumnString::new(Type::string());
    col.append("hello".to_string());
    col.append("world".to_string());
    col.append("".to_string()); // Empty string
    col.append("a very long string with many characters".to_string());

    let result = roundtrip_column(&col).unwrap();
    let result_str = result.as_any().downcast_ref::<ColumnString>().unwrap();

    assert_eq!(result_str.len(), 4);
    assert_eq!(result_str.at(0), "hello");
    assert_eq!(result_str.at(1), "world");
    assert_eq!(result_str.at(2), "");
    assert_eq!(result_str.at(3), "a very long string with many characters");
}

#[test]
fn test_roundtrip_fixed_string() {
    let size = 10;
    let mut col = ColumnFixedString::new(Type::fixed_string(size));

    col.append("abc".to_string()); // Will be padded to 10 bytes
    col.append("1234567890".to_string()); // Exactly 10 bytes
    col.append("".to_string()); // Empty, padded to 10 bytes

    let result = roundtrip_column(&col).unwrap();
    let result_fixed =
        result.as_any().downcast_ref::<ColumnFixedString>().unwrap();

    assert_eq!(result_fixed.len(), 3);

    // at() returns trimmed strings (null bytes removed)
    assert_eq!(result_fixed.at(0), "abc");
    assert_eq!(result_fixed.at(1), "1234567890");
    assert_eq!(result_fixed.at(2), "");

    // Verify the fixed size is correct
    assert_eq!(result_fixed.fixed_size(), size);
}

// ============================================================================
// Date Column Roundtrip Tests
// ============================================================================

#[test]
fn test_roundtrip_date() {
    let mut col = ColumnDate::new(Type::date());
    col.append(0); // 1970-01-01
    col.append(19000); // 2022-01-05 (approximately)
    col.append(10000); // 1997-05-19

    let result = roundtrip_column(&col).unwrap();
    let result_date = result.as_any().downcast_ref::<ColumnDate>().unwrap();

    assert_eq!(result_date.len(), 3);
    assert_eq!(result_date.at(0), 0);
    assert_eq!(result_date.at(1), 19000);
    assert_eq!(result_date.at(2), 10000);
}

// ============================================================================
// Array Column Roundtrip Tests
// ============================================================================
//
// These tests verify that Array columns properly serialize and deserialize
// nested data. Fixed: load_from_buffer() now properly loads nested column data
// using Arc::get_mut.

#[test]
fn test_roundtrip_array_uint64() {
    let inner_type = Type::uint64();
    let col_type = Type::array(inner_type.clone());
    let mut col = ColumnArray::new(col_type.clone());

    // Create first array: [1, 2, 3]
    let mut inner1 = ColumnUInt64::new(inner_type.clone());
    inner1.append(1);
    inner1.append(2);
    inner1.append(3);
    col.append_array(Arc::new(inner1));

    // Create second array: [10, 20]
    let mut inner2 = ColumnUInt64::new(inner_type.clone());
    inner2.append(10);
    inner2.append(20);
    col.append_array(Arc::new(inner2));

    let result = roundtrip_column(&col).unwrap();
    let result_array = result.as_any().downcast_ref::<ColumnArray>().unwrap();

    assert_eq!(result_array.len(), 2);

    // Check first array
    let arr1 = result_array.at(0);
    let arr1_u64 = arr1.as_any().downcast_ref::<ColumnUInt64>().unwrap();
    assert_eq!(arr1_u64.len(), 3);
    assert_eq!(arr1_u64.at(0), 1);
    assert_eq!(arr1_u64.at(1), 2);
    assert_eq!(arr1_u64.at(2), 3);

    // Check second array
    let arr2 = result_array.at(1);
    let arr2_u64 = arr2.as_any().downcast_ref::<ColumnUInt64>().unwrap();
    assert_eq!(arr2_u64.len(), 2);
    assert_eq!(arr2_u64.at(0), 10);
    assert_eq!(arr2_u64.at(1), 20);
}

#[test]
fn test_roundtrip_array_string() {
    let inner_type = Type::string();
    let col_type = Type::array(inner_type.clone());
    let mut col = ColumnArray::new(col_type.clone());

    // Create first array: ["hello", "world"]
    let mut inner1 = ColumnString::new(inner_type.clone());
    inner1.append("hello".to_string());
    inner1.append("world".to_string());
    col.append_array(Arc::new(inner1));

    // Create second array: ["rust"]
    let mut inner2 = ColumnString::new(inner_type.clone());
    inner2.append("rust".to_string());
    col.append_array(Arc::new(inner2));

    let result = roundtrip_column(&col).unwrap();
    let result_array = result.as_any().downcast_ref::<ColumnArray>().unwrap();

    assert_eq!(result_array.len(), 2);

    // Check first array
    let arr1 = result_array.at(0);
    let arr1_str = arr1.as_any().downcast_ref::<ColumnString>().unwrap();
    assert_eq!(arr1_str.len(), 2);
    assert_eq!(arr1_str.at(0), "hello");
    assert_eq!(arr1_str.at(1), "world");

    // Check second array
    let arr2 = result_array.at(1);
    let arr2_str = arr2.as_any().downcast_ref::<ColumnString>().unwrap();
    assert_eq!(arr2_str.len(), 1);
    assert_eq!(arr2_str.at(0), "rust");
}

// ============================================================================
// Nullable Column Roundtrip Tests
// ============================================================================
// These tests verify that Nullable columns properly serialize and deserialize
// nested data. Fixed: load_from_buffer() now properly loads nested column data
// using Arc::get_mut.

#[test]
fn test_roundtrip_nullable_uint32() {
    let inner_type = Type::uint32();
    let null_type = Type::nullable(inner_type.clone());
    let mut col = ColumnNullable::new(null_type);

    col.append_nullable(Some(42));
    col.append_nullable(None);
    col.append_nullable(Some(100));
    col.append_nullable(None);
    col.append_nullable(Some(255));

    let result = roundtrip_column(&col).unwrap();
    let result_nullable =
        result.as_any().downcast_ref::<ColumnNullable>().unwrap();

    assert_eq!(result_nullable.len(), 5);

    // Check null flags
    assert_eq!(result_nullable.is_null_at(0), false);
    assert_eq!(result_nullable.is_null_at(1), true);
    assert_eq!(result_nullable.is_null_at(2), false);
    assert_eq!(result_nullable.is_null_at(3), true);
    assert_eq!(result_nullable.is_null_at(4), false);

    // Check values (for non-null entries)
    let nested = result_nullable.nested();
    let nested_u32 = nested.as_any().downcast_ref::<ColumnUInt32>().unwrap();
    assert_eq!(nested_u32.at(0), 42);
    assert_eq!(nested_u32.at(2), 100);
    assert_eq!(nested_u32.at(4), 255);
}

// ============================================================================
// Tuple Column Roundtrip Tests
// ============================================================================

#[test]
fn test_roundtrip_tuple() {
    let types = vec![Type::uint64(), Type::string()];
    let tuple_type = Type::tuple(types.clone());

    // Create and populate inner columns first
    let mut inner1 = ColumnUInt64::new(Type::uint64());
    inner1.append(42);
    inner1.append(100);

    let mut inner2 = ColumnString::new(Type::string());
    inner2.append("test".to_string());
    inner2.append("hello".to_string());

    let col = ColumnTuple::new(
        tuple_type,
        vec![Arc::new(inner1) as ColumnRef, Arc::new(inner2) as ColumnRef],
    );

    let result = roundtrip_column(&col).unwrap();
    let result_tuple = result.as_any().downcast_ref::<ColumnTuple>().unwrap();

    assert_eq!(result_tuple.len(), 2);

    // Check first column (UInt64)
    let col0 = result_tuple.column_at(0);
    let col0_u64 = col0.as_any().downcast_ref::<ColumnUInt64>().unwrap();
    assert_eq!(col0_u64.at(0), 42);
    assert_eq!(col0_u64.at(1), 100);

    // Check second column (String)
    let col1 = result_tuple.column_at(1);
    let col1_str = col1.as_any().downcast_ref::<ColumnString>().unwrap();
    assert_eq!(col1_str.at(0), "test");
    assert_eq!(col1_str.at(1), "hello");
}

// ============================================================================
// Empty Column Roundtrip Tests
// ============================================================================

#[test]
fn test_roundtrip_empty_columns() {
    // Test that empty columns roundtrip correctly

    // Empty UInt32
    let col_u32 = ColumnUInt32::new(Type::uint32());
    let result_u32 = roundtrip_column(&col_u32).unwrap();
    assert_eq!(result_u32.size(), 0);

    // Empty String
    let col_str = ColumnString::new(Type::string());
    let result_str = roundtrip_column(&col_str).unwrap();
    assert_eq!(result_str.size(), 0);

    // Empty Array
    let col_arr = ColumnArray::new(Type::array(Type::uint32()));
    let result_arr = roundtrip_column(&col_arr).unwrap();
    assert_eq!(result_arr.size(), 0);
}

// ============================================================================
// Large Data Roundtrip Tests
// ============================================================================

#[test]
fn test_roundtrip_large_dataset() {
    // Test with larger dataset to ensure buffer handling is correct
    let mut col = ColumnUInt64::new(Type::uint64());

    for i in 0..1000 {
        col.append(i);
    }

    let result = roundtrip_column(&col).unwrap();
    let result_u64 = result.as_any().downcast_ref::<ColumnUInt64>().unwrap();

    assert_eq!(result_u64.len(), 1000);
    for i in 0..1000 {
        assert_eq!(result_u64.at(i), i as u64);
    }
}

#[test]
fn test_roundtrip_large_strings() {
    let mut col = ColumnString::new(Type::string());

    // Add various sized strings
    for i in 0..100 {
        let s = "x".repeat(i * 10); // Strings of increasing length
        col.append(s);
    }

    let result = roundtrip_column(&col).unwrap();
    let result_str = result.as_any().downcast_ref::<ColumnString>().unwrap();

    assert_eq!(result_str.len(), 100);
    for i in 0..100 {
        let expected = "x".repeat(i * 10);
        assert_eq!(result_str.at(i), expected);
    }
}
