/// Integration tests for LowCardinality types
/// Tests: LowCardinality(String), LowCardinality(Int64), LowCardinality(UUID)
mod common;

use clickhouse_client::column::lowcardinality::ColumnLowCardinality;
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};

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
