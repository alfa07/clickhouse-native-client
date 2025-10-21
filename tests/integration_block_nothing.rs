/// Integration tests for Nothing column
///
/// IMPORTANT: The Nothing type is special in ClickHouse and CANNOT be used
/// as a regular table column. From ClickHouse server:
/// "Data type Nothing of column 'value' cannot be used in tables"
///
/// The Nothing type is used internally for:
/// - Empty result sets
/// - Null-only columns (via Nullable(Nothing))
/// - Certain internal operations
///
/// Therefore, we don't test Nothing columns like other types (no table
/// creation, no block insertion). The Column interface is tested via unit
/// tests in src/column/nothing.rs instead.
mod common;

use clickhouse_client::{
    column::nothing::ColumnNothing,
    types::{
        Type,
        TypeCode,
    },
};
use std::sync::Arc;

fn nothing_type() -> Type {
    Type::Simple(TypeCode::Void)
}

/// Test that we can create and use ColumnNothing programmatically
/// (even though we can't use it in actual ClickHouse tables)
#[tokio::test]
#[ignore]
async fn test_nothing_column_interface() {
    use clickhouse_client::column::Column;

    // Test basic column operations
    let mut col = ColumnNothing::new(nothing_type());

    // Append some "nothing" values
    col.append();
    col.append();
    col.append();

    assert_eq!(col.len(), 3);
    assert_eq!(col.at(0), None);
    assert_eq!(col.at(1), None);
    assert_eq!(col.at(2), None);

    // Test append_column
    let col2 = Arc::new(ColumnNothing::new(nothing_type()).with_size(2));
    col.append_column(col2).expect("Failed to append column");
    assert_eq!(col.len(), 5);

    // Test slice
    let sliced = col.slice(1, 3).expect("Failed to slice");
    let sliced_col = sliced
        .as_any()
        .downcast_ref::<ColumnNothing>()
        .expect("Invalid column type");
    assert_eq!(sliced_col.len(), 3);

    // Test clear
    col.clear();
    assert_eq!(col.len(), 0);
    assert!(col.is_empty());
}

/// Test load_from_buffer (reading Nothing column data)
#[test]
fn test_nothing_load_from_buffer() {
    use clickhouse_client::column::Column;

    let mut col = ColumnNothing::new(nothing_type());

    // Nothing columns consume 1 byte per row in the wire format
    let mut buffer: &[u8] = &[0, 0, 0, 0, 0]; // 5 bytes for 5 rows
    col.load_from_buffer(&mut buffer, 5).expect("Failed to load from buffer");

    assert_eq!(col.len(), 5);
    assert!(buffer.is_empty()); // All bytes should be consumed
}

/// Test save_to_buffer (should error - Nothing columns can't be saved)
#[test]
fn test_nothing_save_to_buffer_not_supported() {
    use bytes::BytesMut;
    use clickhouse_client::column::Column;

    let col = ColumnNothing::new(nothing_type()).with_size(3);
    let mut buffer = BytesMut::new();

    // According to C++ implementation and ClickHouse semantics,
    // Nothing columns cannot be saved/serialized
    let result = col.save_to_buffer(&mut buffer);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("not supported for Nothing"));
}

// Note: No property-based tests or actual database integration tests
// because Nothing columns cannot be used in ClickHouse tables.
// The unit tests above verify the Column trait implementation works correctly.
