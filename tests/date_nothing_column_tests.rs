use bytes::BytesMut;
use clickhouse_native_client::{
    column::*,
    types::{
        Type,
        TypeCode,
    },
};

// ============================================================================
// Date Column Tests
// ============================================================================

#[test]
fn test_date_append_and_retrieve() {
    let mut col = ColumnDate::new(Type::date());
    col.append(19000); // Days since epoch
    col.append(19001);
    col.append(19002);

    assert_eq!(col.len(), 3);
    assert_eq!(col.at(0), 19000);
    assert_eq!(col.at(1), 19001);
    assert_eq!(col.at(2), 19002);
}

#[test]
fn test_date_from_timestamp() {
    let mut col = ColumnDate::new(Type::date());
    // 2022-01-01 00:00:00 UTC = 1640995200 seconds
    col.append_timestamp(1640995200);

    assert_eq!(col.len(), 1);
    let days = col.at(0);
    // 1640995200 / 86400 = 18993 days
    assert_eq!(days, 18993);
}

#[test]
fn test_date_timestamp_at() {
    let mut col = ColumnDate::new(Type::date());
    col.append(18993); // 2022-01-01

    let timestamp = col.timestamp_at(0);
    // 18993 * 86400 = 1640995200
    assert_eq!(timestamp, 1640995200);
}

#[test]
fn test_date_slice() {
    let mut col = ColumnDate::new(Type::date());
    col.append(19000);
    col.append(19001);
    col.append(19002);
    col.append(19003);

    let sliced = col.slice(1, 2).unwrap();
    let sliced_col = sliced.as_any().downcast_ref::<ColumnDate>().unwrap();

    assert_eq!(sliced_col.len(), 2);
    assert_eq!(sliced_col.at(0), 19001);
    assert_eq!(sliced_col.at(1), 19002);
}

#[test]
fn test_date_serialization() {
    let mut col = ColumnDate::new(Type::date());
    col.append(19000);
    col.append(19001);

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();

    // Date is stored as UInt16 (2 bytes)
    assert_eq!(buffer.len(), 4);

    let mut col2 = ColumnDate::new(Type::date());
    let mut buffer_slice = &buffer[..];
    col2.load_from_buffer(&mut buffer_slice, 2).unwrap();

    assert_eq!(col2.len(), 2);
    assert_eq!(col2.at(0), 19000);
    assert_eq!(col2.at(1), 19001);
}

#[test]
fn test_date_epoch() {
    let mut col = ColumnDate::new(Type::date());
    col.append(0); // Unix epoch 1970-01-01

    assert_eq!(col.at(0), 0);
    assert_eq!(col.timestamp_at(0), 0);
}

// ============================================================================
// Date32 Column Tests
// ============================================================================

#[test]
fn test_date32_append_and_retrieve() {
    let mut col = ColumnDate32::new(Type::date32());
    col.append(-25567); // 1900-01-01
    col.append(0); // 1970-01-01
    col.append(50000); // Future date

    assert_eq!(col.len(), 3);
    assert_eq!(col.at(0), -25567);
    assert_eq!(col.at(1), 0);
    assert_eq!(col.at(2), 50000);
}

#[test]
fn test_date32_negative_dates() {
    let mut col = ColumnDate32::new(Type::date32());
    col.append(-25567); // Before Unix epoch

    assert_eq!(col.at(0), -25567);
    let timestamp = col.timestamp_at(0);
    assert_eq!(timestamp, -25567 * 86400);
}

#[test]
fn test_date32_from_timestamp() {
    let mut col = ColumnDate32::new(Type::date32());
    col.append_timestamp(1640995200); // 2022-01-01

    assert_eq!(col.len(), 1);
    assert_eq!(col.at(0), 18993);
}

#[test]
fn test_date32_serialization() {
    let mut col = ColumnDate32::new(Type::date32());
    col.append(-25567);
    col.append(0);
    col.append(50000);

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();

    // Date32 is stored as Int32 (4 bytes)
    assert_eq!(buffer.len(), 12);

    let mut col2 = ColumnDate32::new(Type::date32());
    let mut buffer_slice = &buffer[..];
    col2.load_from_buffer(&mut buffer_slice, 3).unwrap();

    assert_eq!(col2.len(), 3);
    assert_eq!(col2.at(0), -25567);
    assert_eq!(col2.at(1), 0);
    assert_eq!(col2.at(2), 50000);
}

#[test]
fn test_date32_slice() {
    let mut col = ColumnDate32::new(Type::date32());
    col.append(-100);
    col.append(0);
    col.append(100);

    let sliced = col.slice(1, 2).unwrap();
    let sliced_col = sliced.as_any().downcast_ref::<ColumnDate32>().unwrap();

    assert_eq!(sliced_col.len(), 2);
    assert_eq!(sliced_col.at(0), 0);
    assert_eq!(sliced_col.at(1), 100);
}

// ============================================================================
// DateTime Column Tests
// ============================================================================

#[test]
fn test_datetime_append_and_retrieve() {
    let mut col = ColumnDateTime::new(Type::datetime(None));
    col.append(1640995200); // 2022-01-01 00:00:00 UTC
    col.append(1640995201);
    col.append(1640995202);

    assert_eq!(col.len(), 3);
    assert_eq!(col.at(0), 1640995200);
    assert_eq!(col.at(1), 1640995201);
    assert_eq!(col.at(2), 1640995202);
}

#[test]
fn test_datetime_with_timezone() {
    let mut col = ColumnDateTime::new(Type::datetime(Some("UTC".to_string())));
    col.append(1640995200);

    assert_eq!(col.timezone(), Some("UTC"));
    assert_eq!(col.at(0), 1640995200);
}

#[test]
fn test_datetime_timezone_america_new_york() {
    let mut col = ColumnDateTime::new(Type::datetime(Some(
        "America/New_York".to_string(),
    )));
    col.append(1640995200);

    assert_eq!(col.timezone(), Some("America/New_York"));
    assert_eq!(col.at(0), 1640995200);
}

#[test]
fn test_datetime_serialization() {
    let mut col = ColumnDateTime::new(Type::datetime(None));
    col.append(1640995200);
    col.append(1640995201);

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();

    // DateTime is stored as UInt32 (4 bytes)
    assert_eq!(buffer.len(), 8);

    let mut col2 = ColumnDateTime::new(Type::datetime(None));
    let mut buffer_slice = &buffer[..];
    col2.load_from_buffer(&mut buffer_slice, 2).unwrap();

    assert_eq!(col2.len(), 2);
    assert_eq!(col2.at(0), 1640995200);
    assert_eq!(col2.at(1), 1640995201);
}

#[test]
fn test_datetime_slice() {
    let mut col = ColumnDateTime::new(Type::datetime(None));
    col.append(1640995200);
    col.append(1640995201);
    col.append(1640995202);

    let sliced = col.slice(1, 2).unwrap();
    let sliced_col = sliced.as_any().downcast_ref::<ColumnDateTime>().unwrap();

    assert_eq!(sliced_col.len(), 2);
    assert_eq!(sliced_col.at(0), 1640995201);
    assert_eq!(sliced_col.at(1), 1640995202);
}

#[test]
fn test_datetime_epoch() {
    let mut col = ColumnDateTime::new(Type::datetime(None));
    col.append(0); // Unix epoch

    assert_eq!(col.at(0), 0);
}

// ============================================================================
// DateTime64 Column Tests
// ============================================================================

#[test]
fn test_datetime64_millisecond_precision() {
    let mut col = ColumnDateTime64::new(Type::datetime64(3, None)); // millisecond precision
    col.append(1640995200000); // 2022-01-01 00:00:00.000 UTC
    col.append(1640995200123); // 2022-01-01 00:00:00.123 UTC

    assert_eq!(col.len(), 2);
    assert_eq!(col.precision(), 3);
    assert_eq!(col.at(0), 1640995200000);
    assert_eq!(col.at(1), 1640995200123);
}

#[test]
fn test_datetime64_microsecond_precision() {
    let mut col = ColumnDateTime64::new(Type::datetime64(6, None)); // microsecond precision
    col.append(1640995200000000); // 2022-01-01 00:00:00.000000 UTC
    col.append(1640995200123456); // 2022-01-01 00:00:00.123456 UTC

    assert_eq!(col.len(), 2);
    assert_eq!(col.precision(), 6);
    assert_eq!(col.at(0), 1640995200000000);
    assert_eq!(col.at(1), 1640995200123456);
}

#[test]
fn test_datetime64_nanosecond_precision() {
    let mut col = ColumnDateTime64::new(Type::datetime64(9, None)); // nanosecond precision
    col.append(1640995200000000000); // 2022-01-01 00:00:00.000000000 UTC
    col.append(1640995200123456789); // 2022-01-01 00:00:00.123456789 UTC

    assert_eq!(col.len(), 2);
    assert_eq!(col.precision(), 9);
    assert_eq!(col.at(0), 1640995200000000000);
    assert_eq!(col.at(1), 1640995200123456789);
}

#[test]
fn test_datetime64_with_timezone() {
    let mut col =
        ColumnDateTime64::new(Type::datetime64(3, Some("UTC".to_string())));
    col.append(1640995200000);

    assert_eq!(col.timezone(), Some("UTC"));
    assert_eq!(col.precision(), 3);
    assert_eq!(col.at(0), 1640995200000);
}

#[test]
fn test_datetime64_serialization() {
    let mut col = ColumnDateTime64::new(Type::datetime64(3, None));
    col.append(1640995200000);
    col.append(1640995200123);

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();

    // DateTime64 is stored as Int64 (8 bytes)
    assert_eq!(buffer.len(), 16);

    let mut col2 = ColumnDateTime64::new(Type::datetime64(3, None));
    let mut buffer_slice = &buffer[..];
    col2.load_from_buffer(&mut buffer_slice, 2).unwrap();

    assert_eq!(col2.len(), 2);
    assert_eq!(col2.at(0), 1640995200000);
    assert_eq!(col2.at(1), 1640995200123);
}

#[test]
fn test_datetime64_slice() {
    let mut col = ColumnDateTime64::new(Type::datetime64(3, None));
    col.append(1640995200000);
    col.append(1640995200123);
    col.append(1640995200456);

    let sliced = col.slice(1, 2).unwrap();
    let sliced_col =
        sliced.as_any().downcast_ref::<ColumnDateTime64>().unwrap();

    assert_eq!(sliced_col.len(), 2);
    assert_eq!(sliced_col.precision(), 3);
    assert_eq!(sliced_col.at(0), 1640995200123);
    assert_eq!(sliced_col.at(1), 1640995200456);
}

#[test]
fn test_datetime64_zero_precision() {
    let mut col = ColumnDateTime64::new(Type::datetime64(0, None)); // second precision
    col.append(1640995200); // 2022-01-01 00:00:00 UTC

    assert_eq!(col.precision(), 0);
    assert_eq!(col.at(0), 1640995200);
}

// ============================================================================
// Nothing Column Tests
// ============================================================================

fn void_type() -> Type {
    Type::Simple(TypeCode::Void)
}

#[test]
fn test_nothing_append() {
    let mut col = ColumnNothing::new(void_type());
    col.append();
    col.append();
    col.append();

    assert_eq!(col.len(), 3);
    assert_eq!(col.at(0), None);
    assert_eq!(col.at(1), None);
    assert_eq!(col.at(2), None);
}

#[test]
fn test_nothing_with_size() {
    let col = ColumnNothing::new(void_type()).with_size(10);
    assert_eq!(col.len(), 10);
    assert!(!col.is_empty());
}

#[test]
fn test_nothing_empty() {
    let col = ColumnNothing::new(void_type());
    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
}

#[test]
fn test_nothing_slice() {
    let col = ColumnNothing::new(void_type()).with_size(10);
    let sliced = col.slice(2, 5).unwrap();
    let sliced_col = sliced.as_any().downcast_ref::<ColumnNothing>().unwrap();

    assert_eq!(sliced_col.len(), 5);
}

#[test]
fn test_nothing_append_column() {
    use std::sync::Arc;

    let mut col1 = ColumnNothing::new(void_type()).with_size(5);
    let col2 = Arc::new(ColumnNothing::new(void_type()).with_size(3));

    col1.append_column(col2).unwrap();
    assert_eq!(col1.len(), 8);
}

#[test]
fn test_nothing_clear() {
    let mut col = ColumnNothing::new(void_type()).with_size(10);
    assert_eq!(col.len(), 10);

    col.clear();
    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
}

#[test]
fn test_nothing_load_from_buffer() {
    let mut col = ColumnNothing::new(void_type());
    let data = [0u8; 5]; // 5 bytes of nothing
    let mut buffer = &data[..];

    col.load_from_buffer(&mut buffer, 5).unwrap();
    assert_eq!(col.len(), 5);
    assert_eq!(buffer.len(), 0); // All bytes consumed
}

#[test]
fn test_nothing_save_fails() {
    let col = ColumnNothing::new(void_type()).with_size(5);
    let mut buffer = BytesMut::new();

    // SaveBody should fail for Nothing column
    let result = col.save_to_buffer(&mut buffer);
    assert!(result.is_err());
}
