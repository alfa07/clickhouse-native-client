/// Integration tests for LowCardinality types
/// Tests: LowCardinality(String), LowCardinality(Int64), LowCardinality(UUID)
mod common;

use clickhouse_client::{
    column::{
        lowcardinality::ColumnLowCardinality,
        numeric::ColumnInt64,
        string::ColumnString,
        uuid::ColumnUuid,
    },
    types::Type,
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};
use std::sync::Arc;
use uuid::Uuid;

// ============================================================================
// LowCardinality(String)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_lowcardinality_string_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("lc_string")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (tag LowCardinality(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Use SQL for LowCardinality
    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES
            ('tag1'), ('tag2'), ('tag1'), ('tag3'), ('tag2'), ('tag1'), (''),
            ('tag1'), ('tag2'), ('tag3')",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT tag FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 10);

    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnLowCardinality>()
        .expect("Invalid column type");

    // LowCardinality should have deduplicated the values
    assert_eq!(result_col.len(), 10);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_lowcardinality_string_empty() {
    let (mut client, db_name) = create_isolated_test_client("lc_string_empty")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (tag LowCardinality(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES (''), (''), ('')",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT tag FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// LowCardinality(Int64)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_lowcardinality_int64_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("lc_int64")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (category LowCardinality(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES
            (1), (2), (1), (3), (2), (1), (0), (1), (2), (3),
            (100), (200), (100), (200), (0)",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT category FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 15);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_lowcardinality_int64_boundary_values() {
    let (mut client, db_name) =
        create_isolated_test_client("lc_int64_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value LowCardinality(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES
            ({}), (0), ({}), ({}), (0), ({})",
            i64::MIN,
            i64::MAX,
            i64::MIN,
            i64::MAX,
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT value FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 6);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// LowCardinality(UUID)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_lowcardinality_uuid_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("lc_uuid")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id LowCardinality(UUID)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let uuid1 = "550e8400-e29b-41d4-a716-446655440000";
    let uuid2 = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    let uuid3 = "00000000-0000-0000-0000-000000000000";

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES
            ('{}'), ('{}'), ('{}'), ('{}'), ('{}'),
            ('{}'), ('{}'), ('{}'), ('{}'), ('{}')",
            uuid1,
            uuid2,
            uuid1,
            uuid3,
            uuid2,
            uuid1,
            uuid2,
            uuid3,
            uuid1,
            uuid2,
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT id FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 10);

    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnLowCardinality>()
        .expect("Invalid column type");

    assert_eq!(result_col.len(), 10);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// LowCardinality with many unique values
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_lowcardinality_many_unique_values() {
    let (mut client, db_name) = create_isolated_test_client("lc_many_unique")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (tag LowCardinality(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Create many unique values (stress test for dictionary)
    let mut values = Vec::new();
    for i in 0..1000 {
        values.push(format!("('tag_{}')", i));
    }

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES {}",
            values.join(", "),
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT tag FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 1000);

    cleanup_test_database(&db_name).await;
}
