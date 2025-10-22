/// Integration tests for Enum8 and Enum16 types
mod common;

use clickhouse_client::{
    column::enum_column::{
        ColumnEnum16,
        ColumnEnum8,
    },
    types::{
        EnumItem,
        Type,
    },
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
    // Enum8 with proper enum type definition
    let items = [
        EnumItem { name: "pending".to_string(), value: 1 },
        EnumItem { name: "active".to_string(), value: 2 },
        EnumItem { name: "inactive".to_string(), value: 3 },
    ];
    let mut col = ColumnEnum8::new(Type::enum8(items));
    col.append_value(1); // 'pending'
    col.append_value(2); // 'active'
    col.append_value(3); // 'inactive'
    col.append_value(1); // 'pending' again
    col.append_value(2); // 'active' again

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
        .downcast_ref::<ColumnEnum8>()
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
    // Enum16 with proper enum type definition
    let items = [
        EnumItem { name: "low".to_string(), value: 100 },
        EnumItem { name: "medium".to_string(), value: 200 },
        EnumItem { name: "high".to_string(), value: 300 },
        EnumItem { name: "critical".to_string(), value: 1000 },
    ];
    let mut col = ColumnEnum16::new(Type::enum16(items));
    col.append_value(100); // 'low'
    col.append_value(200); // 'medium'
    col.append_value(300); // 'high'
    col.append_value(1000); // 'critical'
    col.append_value(200); // 'medium' again

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
        .downcast_ref::<ColumnEnum16>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), 100);
    assert_eq!(result_col.at(1), 200);
    assert_eq!(result_col.at(2), 300);
    assert_eq!(result_col.at(3), 1000);
    assert_eq!(result_col.at(4), 200);

    cleanup_test_database(&db_name).await;
}
