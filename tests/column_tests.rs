// Column tests ported from clickhouse-cpp ut/columns_ut.cpp
// These tests verify column functionality: init, append, slice, and type conversions

use clickhouse_client::column::*;
use clickhouse_client::types::Type;
use std::sync::Arc;

// ============================================================================
// Test Helper Functions
// ============================================================================

/// Generate a sequence of test numbers
fn make_numbers() -> Vec<u32> {
    vec![1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 31]
}

/// Generate test strings of varying lengths
fn make_strings() -> Vec<String> {
    vec![
        "".to_string(),
        "a".to_string(),
        "ab".to_string(),
        "abc".to_string(),
        "abcd".to_string(),
    ]
}

/// Generate fixed-size strings for testing
fn make_fixed_strings(size: usize) -> Vec<String> {
    let strs = vec!["aaa", "bbb", "ccc", "ddd", "eee"];
    strs.into_iter()
        .map(|s| {
            let mut padded = s.to_string();
            padded.truncate(size);
            while padded.len() < size {
                padded.push('\0');
            }
            padded
        })
        .collect()
}

// ============================================================================
// Numeric Column Tests
// ============================================================================

#[test]
fn test_numeric_init() {
    let numbers = make_numbers();
    let col = ColumnUInt32::new(Type::uint32()).with_data(numbers.clone());

    assert_eq!(col.len(), 11);
    assert_eq!(col.at(3), 5);   // Index 3 = value 5 (primes: 1,2,3,5,7,11,...)
    assert_eq!(col.at(4), 7);
    assert_eq!(col.at(10), 31);
}

#[test]
fn test_numeric_append() {
    let mut col = ColumnUInt32::new(Type::uint32());

    col.append(42);
    col.append(100);
    col.append(255);

    assert_eq!(col.len(), 3);
    assert_eq!(col.at(0), 42);
    assert_eq!(col.at(1), 100);
    assert_eq!(col.at(2), 255);
}

#[test]
fn test_numeric_slice() {
    let numbers = make_numbers();
    let col = ColumnUInt32::new(Type::uint32()).with_data(numbers);

    // Slice from index 3, length 3  (gets indices 3,4,5 = values 5,7,11)
    let slice = col.slice(3, 3).unwrap();
    let slice_u32 = slice.as_any().downcast_ref::<ColumnUInt32>().unwrap();

    assert_eq!(slice_u32.len(), 3);
    assert_eq!(slice_u32.at(0), 5);  // Original index 3 = value 5
    assert_eq!(slice_u32.at(1), 7);  // Original index 4 = value 7
    assert_eq!(slice_u32.at(2), 11); // Original index 5 = value 11
}

#[test]
fn test_int64_column() {
    let mut col = ColumnInt64::new(Type::int64());

    col.append(-100);
    col.append(0);
    col.append(9223372036854775807); // i64::MAX

    assert_eq!(col.len(), 3);
    assert_eq!(col.at(0), -100);
    assert_eq!(col.at(1), 0);
    assert_eq!(col.at(2), 9223372036854775807);
}

#[test]
fn test_float_column() {
    let mut col = ColumnFloat32::new(Type::float32());

    col.append(3.14);
    col.append(-2.71);
    col.append(0.0);

    assert_eq!(col.len(), 3);
    assert!((col.at(0) - 3.14).abs() < 0.001);
    assert!((col.at(1) - (-2.71)).abs() < 0.001);
    assert_eq!(col.at(2), 0.0);
}

// ============================================================================
// String Column Tests
// ============================================================================

#[test]
fn test_string_init() {
    let values = make_strings();
    let col = ColumnString::new(Type::string()).with_data(values.clone());

    assert_eq!(col.len(), values.len());
    assert_eq!(col.at(1), "a");
    assert_eq!(col.at(3), "abc");
}

#[test]
fn test_string_append() {
    let mut col = ColumnString::new(Type::string());
    let expected = "ufiudhf3493fyiudferyer3yrifhdflkdjfeuroe";

    col.append(expected.to_string());
    col.append(expected.to_string());
    col.append("11".to_string());

    assert_eq!(col.len(), 3);
    assert_eq!(col.at(0), expected);
    assert_eq!(col.at(1), expected);
    assert_eq!(col.at(2), "11");
}

#[test]
fn test_string_empty() {
    let mut col = ColumnString::new(Type::string());

    col.append("".to_string());
    col.append("test".to_string());
    col.append("".to_string());

    assert_eq!(col.len(), 3);
    assert_eq!(col.at(0), "");
    assert_eq!(col.at(1), "test");
    assert_eq!(col.at(2), "");
}

// ============================================================================
// FixedString Column Tests
// ============================================================================

#[test]
fn test_fixed_string_init() {
    let data = make_fixed_strings(3);
    let col = ColumnFixedString::new(Type::fixed_string(3)).with_data(data.clone());

    assert_eq!(col.len(), data.len());

    for (i, expected) in data.iter().enumerate() {
        assert_eq!(col.at(i), *expected);
    }
}

#[test]
fn test_fixed_string_append_small_strings() {
    // Ensure that strings smaller than FixedString's size are padded with zeroes
    let string_size = 7;
    let mut col = ColumnFixedString::new(Type::fixed_string(string_size));

    col.append("abc".to_string());
    col.append("xy".to_string());
    col.append("".to_string());

    assert_eq!(col.len(), 3);

    // Check that all strings are padded to fixed size
    assert_eq!(col.at(0).len(), string_size);
    assert_eq!(col.at(1).len(), string_size);
    assert_eq!(col.at(2).len(), string_size);

    // Check actual content (padded with \0)
    let mut expected1 = "abc".to_string();
    expected1.push_str(&"\0".repeat(string_size - 3));
    assert_eq!(col.at(0), expected1);
}

#[test]
#[should_panic(expected = "String too long")]
fn test_fixed_string_append_large_string() {
    // Ensure that inserting strings larger than FixedString size throws error
    let mut col = ColumnFixedString::new(Type::fixed_string(1));
    col.append("2c".to_string()); // Should panic - string too long
}

#[test]
fn test_fixed_string_type_size() {
    let col = ColumnFixedString::new(Type::fixed_string(10));
    assert_eq!(col.fixed_size(), 10);

    let col = ColumnFixedString::new(Type::fixed_string(0));
    assert_eq!(col.fixed_size(), 0);
}

// ============================================================================
// Date Column Tests
// ============================================================================

#[test]
fn test_date_append() {
    let mut col = ColumnDate::new(Type::date());

    // Add some date values (days since epoch)
    col.append(0);      // 1970-01-01
    col.append(19000);  // 2022-01-05 (approximately)
    col.append(10000);  // 1997-05-19

    assert_eq!(col.len(), 3);
    assert_eq!(col.at(0), 0);
    assert_eq!(col.at(1), 19000);
    assert_eq!(col.at(2), 10000);
}

// ============================================================================
// Array Column Tests
// ============================================================================

#[test]
fn test_array_uint64() {
    let inner_type = Type::uint64();
    let col_type = Type::array(inner_type.clone());
    let mut col = ColumnArray::new(col_type.clone());

    // Create inner column with data
    let mut inner1 = ColumnUInt64::new(inner_type.clone());
    inner1.append(1);
    inner1.append(2);
    inner1.append(3);

    let mut inner2 = ColumnUInt64::new(inner_type.clone());
    inner2.append(10);
    inner2.append(20);

    col.append_array(Arc::new(inner1));
    col.append_array(Arc::new(inner2));

    assert_eq!(col.len(), 2);

    // Check first array
    let arr1 = col.at(0);
    let arr1_u64 = arr1.as_any().downcast_ref::<ColumnUInt64>().unwrap();
    assert_eq!(arr1_u64.len(), 3);
    assert_eq!(arr1_u64.at(0), 1);
    assert_eq!(arr1_u64.at(2), 3);

    // Check second array
    let arr2 = col.at(1);
    let arr2_u64 = arr2.as_any().downcast_ref::<ColumnUInt64>().unwrap();
    assert_eq!(arr2_u64.len(), 2);
    assert_eq!(arr2_u64.at(0), 10);
}

// ============================================================================
// Nullable Column Tests
// ============================================================================

#[test]
fn test_nullable_with_nulls() {
    let inner_type = Type::uint32();
    let null_type = Type::nullable(inner_type.clone());
    let mut col = ColumnNullable::new(null_type);

    col.append_nullable(Some(42));
    col.append_nullable(None);
    col.append_nullable(Some(100));
    col.append_nullable(None);
    col.append_nullable(Some(255));

    assert_eq!(col.len(), 5);

    // Check values
    assert_eq!(col.is_null_at(0), false);
    assert_eq!(col.at(0).as_any().downcast_ref::<ColumnUInt32>().unwrap().at(0), 42);

    assert_eq!(col.is_null_at(1), true);

    assert_eq!(col.is_null_at(2), false);
    assert_eq!(col.at(2).as_any().downcast_ref::<ColumnUInt32>().unwrap().at(2), 100);

    assert_eq!(col.is_null_at(3), true);

    assert_eq!(col.is_null_at(4), false);
    assert_eq!(col.at(4).as_any().downcast_ref::<ColumnUInt32>().unwrap().at(4), 255);
}

#[test]
fn test_nullable_all_non_null() {
    let inner_type = Type::uint32();
    let null_type = Type::nullable(inner_type.clone());
    let mut col = ColumnNullable::new(null_type);

    col.append_nullable(Some(123));
    col.append_nullable(Some(456));

    assert_eq!(col.len(), 2);
    assert_eq!(col.is_null_at(0), false);
    assert_eq!(col.is_null_at(1), false);
}

// ============================================================================
// Tuple Column Tests
// ============================================================================

#[test]
fn test_tuple_basic() {
    let types = vec![Type::uint64(), Type::string()];
    let tuple_type = Type::tuple(types.clone());

    // Create and populate inner columns first
    let mut inner1 = ColumnUInt64::new(Type::uint64());
    inner1.append(42);
    inner1.append(100);

    let mut inner2 = ColumnString::new(Type::string());
    inner2.append("test".to_string());
    inner2.append("hello".to_string());

    let col = ColumnTuple::new(tuple_type, vec![Arc::new(inner1) as ColumnRef, Arc::new(inner2) as ColumnRef]);

    assert_eq!(col.len(), 2);

    // Access inner columns for reading
    let col0_ref = col.column_at(0);
    let col0 = col0_ref.as_any().downcast_ref::<ColumnUInt64>().unwrap();
    assert_eq!(col0.at(0), 42);
    assert_eq!(col0.at(1), 100);

    let col1_ref = col.column_at(1);
    let col1 = col1_ref.as_any().downcast_ref::<ColumnString>().unwrap();
    assert_eq!(col1.at(0), "test");
    assert_eq!(col1.at(1), "hello");
}
