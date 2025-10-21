/// Integration tests for Date, Date32, DateTime, and DateTime64 types
mod common;

use clickhouse_client::{
    column::date::*,
    types::Type,
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};
use std::sync::Arc;

// ============================================================================
// Date Tests (UInt16 - days since 1970-01-01)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_date_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("date_roundtrip")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (date Date) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnDate::new(Type::date());
    col.append(0); // 1970-01-01 (min)
    col.append(18993); // 2022-01-01
    col.append(19358); // 2023-01-01
    col.append(65535); // 2149-06-06 (max for Date)
    block
        .append_column("date", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!(
            "SELECT date FROM {}.test_table ORDER BY date",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnDate>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 0);
    assert_eq!(result_col.at(1), 18993);
    assert_eq!(result_col.at(2), 19358);
    assert_eq!(result_col.at(3), 65535);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Date32 Tests (Int32 - days since 1900-01-01)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_date32_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("date32_roundtrip")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (date Date32) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnDate32::new(Type::date32());
    col.append(-25567); // 1900-01-02 (near min)
    col.append(0); // 1900-01-01 + 0 days = 1900-01-01
    col.append(25567); // 1970-01-01
    col.append(44927); // 2023-01-01
    col.append(100000); // Far future
    block
        .append_column("date", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!(
            "SELECT date FROM {}.test_table ORDER BY date",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 5);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnDate32>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), -25567);
    assert_eq!(result_col.at(1), 0);
    assert_eq!(result_col.at(2), 25567);
    assert_eq!(result_col.at(3), 44927);
    assert_eq!(result_col.at(4), 100000);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// DateTime Tests (UInt32 - seconds since Unix epoch)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_datetime_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("datetime_roundtrip")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (dt DateTime) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnDateTime::new(Type::datetime(None));
    col.append(0); // 1970-01-01 00:00:00
    col.append(1672531200); // 2023-01-01 00:00:00
    col.append(1704067200); // 2024-01-01 00:00:00
    col.append(4294967295); // 2106-02-07 06:28:15 (max)
    block.append_column("dt", Arc::new(col)).expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT dt FROM {}.test_table ORDER BY dt", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnDateTime>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 0);
    assert_eq!(result_col.at(1), 1672531200);
    assert_eq!(result_col.at(2), 1704067200);
    assert_eq!(result_col.at(3), 4294967295);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// DateTime64 Tests (Int64 with precision)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_datetime64_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("datetime64_roundtrip")
            .await
            .expect("Failed to create test client");

    let precision = 3; // milliseconds
    client
        .query(format!(
            "CREATE TABLE {}.test_table (dt DateTime64({})) ENGINE = Memory",
            db_name, precision
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnDateTime64::new(Type::datetime64(precision, None));
    col.append(0); // 1970-01-01 00:00:00.000
    col.append(1672531200000); // 2023-01-01 00:00:00.000 (milliseconds)
    col.append(1704067200123); // 2024-01-01 00:00:00.123
    col.append(9223372036854775807); // Max i64
    block.append_column("dt", Arc::new(col)).expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT dt FROM {}.test_table ORDER BY dt", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnDateTime64>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 0);
    assert_eq!(result_col.at(1), 1672531200000);
    assert_eq!(result_col.at(2), 1704067200123);
    assert_eq!(result_col.at(3), 9223372036854775807);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_datetime64_various_precisions() {
    for precision in [0, 3, 6, 9] {
        let (mut client, db_name) =
            create_isolated_test_client(&format!("datetime64_p{}", precision))
                .await
                .expect("Failed to create test client");

        client
            .query(format!(
                "CREATE TABLE {}.test_table (dt DateTime64({})) ENGINE = Memory",
                db_name, precision
            ))
            .await
            .expect("Failed to create table");

        let mut block = Block::new();
        let mut col = ColumnDateTime64::new(Type::datetime64(precision, None));

        let scale = 10i64.pow(precision as u32);
        col.append(0);
        col.append(1672531200 * scale); // 2023-01-01 with appropriate precision
        col.append(1704067200 * scale + 123); // With some fractional part

        block
            .append_column("dt", Arc::new(col))
            .expect("Failed to append column");

        client
            .insert(&format!("{}.test_table", db_name), block)
            .await
            .expect("Failed to insert block");

        let result = client
            .query(format!(
                "SELECT dt FROM {}.test_table ORDER BY dt",
                db_name
            ))
            .await
            .expect("Failed to select");

        assert_eq!(result.total_rows(), 3);

        cleanup_test_database(&db_name).await;
    }
}
