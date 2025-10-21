/// Integration tests for Enum8 and Enum16 types
mod common;

use clickhouse_client::{
    column::numeric::{
        ColumnInt16,
        ColumnInt8,
    },
    types::Type,
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};
use std::sync::Arc;

// ============================================================================
// Enum8 Tests (stored as Int8)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_enum8_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("enum8_roundtrip")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (status Enum8('pending' = 1, 'active' = 2, 'inactive' = 3)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    // Enum8 is stored as Int8
    let mut col = ColumnInt8::new(Type::int8());
    col.append(1); // 'pending'
    col.append(2); // 'active'
    col.append(3); // 'inactive'
    col.append(1); // 'pending' again
    col.append(2); // 'active' again

    block
        .append_column("status", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT status FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 5);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnInt8>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 1);
    assert_eq!(result_col.at(1), 2);
    assert_eq!(result_col.at(2), 3);
    assert_eq!(result_col.at(3), 1);
    assert_eq!(result_col.at(4), 2);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Enum16 Tests (stored as Int16)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_enum16_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("enum16_roundtrip")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (priority Enum16('low' = 100, 'medium' = 200, 'high' = 300, 'critical' = 1000)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    // Enum16 is stored as Int16
    let mut col = ColumnInt16::new(Type::int16());
    col.append(100); // 'low'
    col.append(200); // 'medium'
    col.append(300); // 'high'
    col.append(1000); // 'critical'
    col.append(200); // 'medium' again

    block
        .append_column("priority", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT priority FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 5);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnInt16>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 100);
    assert_eq!(result_col.at(1), 200);
    assert_eq!(result_col.at(2), 300);
    assert_eq!(result_col.at(3), 1000);
    assert_eq!(result_col.at(4), 200);

    cleanup_test_database(&db_name).await;
}
