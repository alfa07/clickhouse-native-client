/// Integration tests for numeric column types
/// Tests: Int8, Int16, Int32, Int64, Int128, UInt8, UInt16, UInt32, UInt64,
/// UInt128, Float32, Float64, Bool
mod common;

use clickhouse_client::{
    column::numeric::*,
    types::Type,
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};
use proptest::prelude::*;
use std::sync::Arc;

// ============================================================================
// UInt8 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_uint8_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("uint8_roundtrip")
        .await
        .expect("Failed to create test client");

    // Create table
    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt8) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Create block with boundary values
    let mut block = Block::new();
    let mut col = ColumnUInt8::new();
    col.append(0); // Min value
    col.append(127); // Mid value
    col.append(255); // Max value
    col.append(1); // Small value
    col.append(254); // Near max
    block.append_column("id", Arc::new(col)).expect("Failed to append column");

    // Insert block
    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    // Select and verify
    let result = client
        .query(format!("SELECT id FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 5);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnUInt8>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 0);
    assert_eq!(result_col.at(1), 1);
    assert_eq!(result_col.at(2), 127);
    assert_eq!(result_col.at(3), 254);
    assert_eq!(result_col.at(4), 255);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// UInt16 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_uint16_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("uint16_roundtrip")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt16) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnUInt16::new();
    col.append(0); // Min
    col.append(32767); // Mid
    col.append(65535); // Max
    col.append(1000); // Regular value
    block.append_column("id", Arc::new(col)).expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT id FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnUInt16>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 0);
    assert_eq!(result_col.at(1), 1000);
    assert_eq!(result_col.at(2), 32767);
    assert_eq!(result_col.at(3), 65535);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// UInt32 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_uint32_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("uint32_roundtrip")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnUInt32::new();
    col.append(0); // Min
    col.append(2147483647); // Mid
    col.append(4294967295); // Max
    col.append(1000000); // Regular value
    block.append_column("id", Arc::new(col)).expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT id FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnUInt32>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 0);
    assert_eq!(result_col.at(1), 1000000);
    assert_eq!(result_col.at(2), 2147483647);
    assert_eq!(result_col.at(3), 4294967295);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// UInt64 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_uint64_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("uint64_roundtrip")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt64) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnUInt64::new();
    col.append(0); // Min
    col.append(9223372036854775807); // Mid
    col.append(18446744073709551615); // Max
    col.append(1000000000); // Regular value
    block.append_column("id", Arc::new(col)).expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT id FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnUInt64>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 0);
    assert_eq!(result_col.at(1), 1000000000);
    assert_eq!(result_col.at(2), 9223372036854775807);
    assert_eq!(result_col.at(3), 18446744073709551615);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// UInt128 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_uint128_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("uint128_roundtrip")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt128) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnUInt128::new();
    col.append(0); // Min
    col.append(170141183460469231731687303715884105727); // Mid
    col.append(340282366920938463463374607431768211455); // Max
    col.append(1000000000000000000); // Regular value
    block.append_column("id", Arc::new(col)).expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT id FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnUInt128>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 0);
    assert_eq!(result_col.at(1), 1000000000000000000);
    assert_eq!(result_col.at(2), 170141183460469231731687303715884105727);
    assert_eq!(result_col.at(3), 340282366920938463463374607431768211455);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Int8 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_int8_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("int8_roundtrip")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id Int8) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnInt8::new();
    col.append(-128); // Min
    col.append(-1); // Negative
    col.append(0); // Zero
    col.append(1); // Positive
    col.append(127); // Max
    block.append_column("id", Arc::new(col)).expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT id FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 5);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnInt8>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), -128);
    assert_eq!(result_col.at(1), -1);
    assert_eq!(result_col.at(2), 0);
    assert_eq!(result_col.at(3), 1);
    assert_eq!(result_col.at(4), 127);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Int16 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_int16_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("int16_roundtrip")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id Int16) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnInt16::new();
    col.append(-32768); // Min
    col.append(-1000); // Negative
    col.append(0); // Zero
    col.append(1000); // Positive
    col.append(32767); // Max
    block.append_column("id", Arc::new(col)).expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT id FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 5);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnInt16>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), -32768);
    assert_eq!(result_col.at(1), -1000);
    assert_eq!(result_col.at(2), 0);
    assert_eq!(result_col.at(3), 1000);
    assert_eq!(result_col.at(4), 32767);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Int32 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_int32_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("int32_roundtrip")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id Int32) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnInt32::new();
    col.append(-2147483648); // Min
    col.append(-1000000); // Negative
    col.append(0); // Zero
    col.append(1000000); // Positive
    col.append(2147483647); // Max
    block.append_column("id", Arc::new(col)).expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT id FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 5);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnInt32>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), -2147483648);
    assert_eq!(result_col.at(1), -1000000);
    assert_eq!(result_col.at(2), 0);
    assert_eq!(result_col.at(3), 1000000);
    assert_eq!(result_col.at(4), 2147483647);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Int64 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_int64_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("int64_roundtrip")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id Int64) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnInt64::new();
    col.append(-9223372036854775808); // Min (i64::MIN)
    col.append(-1000000000); // Negative
    col.append(0); // Zero
    col.append(1000000000); // Positive
    col.append(9223372036854775807); // Max (i64::MAX)
    block.append_column("id", Arc::new(col)).expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT id FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 5);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnInt64>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), -9223372036854775808);
    assert_eq!(result_col.at(1), -1000000000);
    assert_eq!(result_col.at(2), 0);
    assert_eq!(result_col.at(3), 1000000000);
    assert_eq!(result_col.at(4), 9223372036854775807);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Int128 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_int128_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("int128_roundtrip")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id Int128) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnInt128::new();
    col.append(-170141183460469231731687303715884105728); // Min
    col.append(-1000000000000000000); // Negative
    col.append(0); // Zero
    col.append(1000000000000000000); // Positive
    col.append(170141183460469231731687303715884105727); // Max
    block.append_column("id", Arc::new(col)).expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT id FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 5);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnInt128>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), -170141183460469231731687303715884105728);
    assert_eq!(result_col.at(1), -1000000000000000000);
    assert_eq!(result_col.at(2), 0);
    assert_eq!(result_col.at(3), 1000000000000000000);
    assert_eq!(result_col.at(4), 170141183460469231731687303715884105727);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Float32 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_float32_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("float32_roundtrip")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Float32) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnFloat32::new();
    col.append(-3.4028235e38); // Near min
    col.append(-1.5); // Negative
    col.append(0.0); // Zero
    col.append(1.5); // Positive
    col.append(3.4028235e38); // Near max
    col.append(std::f32::consts::PI); // Pi
    block
        .append_column("value", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!(
            "SELECT value FROM {}.test_table ORDER BY value",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 6);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnFloat32>()
        .expect("Invalid column type");

    // Check approximate equality for floats
    assert!((result_col.at(0) - (-3.4028235e38)).abs() < 1e30);
    assert!((result_col.at(1) - (-1.5)).abs() < 0.001);
    assert_eq!(result_col.at(2), 0.0);
    assert!((result_col.at(3) - 1.5).abs() < 0.001);
    assert!((result_col.at(4) - std::f32::consts::PI).abs() < 0.001);
    assert!((result_col.at(5) - 3.4028235e38).abs() < 1e30);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Float64 Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_float64_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("float64_roundtrip")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Float64) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnFloat64::new();
    col.append(-1.7976931348623157e308); // Near min
    col.append(-2.5); // Negative
    col.append(0.0); // Zero
    col.append(2.5); // Positive
    col.append(1.7976931348623157e308); // Near max
    col.append(std::f64::consts::PI); // Pi
    col.append(std::f64::consts::E); // E
    block
        .append_column("value", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!(
            "SELECT value FROM {}.test_table ORDER BY value",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 7);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnFloat64>()
        .expect("Invalid column type");

    // Check approximate equality for doubles
    assert!((result_col.at(0) - (-1.7976931348623157e308)).abs() < 1e300);
    assert!((result_col.at(1) - (-2.5)).abs() < 0.0001);
    assert_eq!(result_col.at(2), 0.0);
    assert!((result_col.at(3) - 2.5).abs() < 0.0001);
    assert!((result_col.at(4) - std::f64::consts::E).abs() < 0.0001);
    assert!((result_col.at(5) - std::f64::consts::PI).abs() < 0.0001);
    assert!((result_col.at(6) - 1.7976931348623157e308).abs() < 1e300);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Bool Tests (Bool is UInt8 with 0/1 values)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_bool_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("bool_roundtrip")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (flag Bool) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnUInt8::new();
    col.append(0); // False
    col.append(1); // True
    col.append(0); // False
    col.append(1); // True
    block
        .append_column("flag", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT flag FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnUInt8>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 0);
    assert_eq!(result_col.at(1), 1);
    assert_eq!(result_col.at(2), 0);
    assert_eq!(result_col.at(3), 1);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Property-based tests with proptest
// ============================================================================

proptest! {
    #[test]
    #[ignore]
    fn prop_test_uint32_values(values in prop::collection::vec(any::<u32>(), 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("prop_uint32")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();
            let mut col = ColumnUInt32::new();
            for &val in &values {
                col.append(val);
            }
            block
                .append_column("id", Arc::new(col))
                .expect("Failed to append column");

            client
                .insert(&format!("{}.test_table", db_name), block)
                .await
                .expect("Failed to insert block");

            let result = client
                .query(format!("SELECT id FROM {}.test_table", db_name))
                .await
                .expect("Failed to select");

            prop_assert_eq!(result.total_rows(), values.len());

            cleanup_test_database(&db_name).await;
            Ok(())
        })?;
    }

    #[test]
    #[ignore]
    fn prop_test_int64_values(values in prop::collection::vec(any::<i64>(), 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("prop_int64")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id Int64) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();
            let mut col = ColumnInt64::new();
            for &val in &values {
                col.append(val);
            }
            block
                .append_column("id", Arc::new(col))
                .expect("Failed to append column");

            client
                .insert(&format!("{}.test_table", db_name), block)
                .await
                .expect("Failed to insert block");

            let result = client
                .query(format!("SELECT id FROM {}.test_table", db_name))
                .await
                .expect("Failed to select");

            prop_assert_eq!(result.total_rows(), values.len());

            cleanup_test_database(&db_name).await;
            Ok(())
        })?;
    }

    #[test]
    #[ignore]
    fn prop_test_float64_values(values in prop::collection::vec(any::<f64>().prop_filter("filter NaN/Inf", |x| x.is_finite()), 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("prop_float64")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (value Float64) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();
            let mut col = ColumnFloat64::new();
            for &val in &values {
                col.append(val);
            }
            block
                .append_column("value", Arc::new(col))
                .expect("Failed to append column");

            client
                .insert(&format!("{}.test_table", db_name), block)
                .await
                .expect("Failed to insert block");

            let result = client
                .query(format!("SELECT value FROM {}.test_table", db_name))
                .await
                .expect("Failed to select");

            prop_assert_eq!(result.total_rows(), values.len());

            cleanup_test_database(&db_name).await;
            Ok(())
        })?;
    }
}
