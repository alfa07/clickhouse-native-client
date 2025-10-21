/// Integration tests for Nothing column using Block insertion
///
/// Note: The Nothing type is special - it doesn't store actual data,
/// only tracks size. According to the C++ implementation, SaveBody
/// is not supported for Nothing columns, so we can't insert them
/// directly. However, we can query them.
mod common;

use clickhouse_client::{
    column::nothing::ColumnNothing,
    types::{
        Type,
        TypeCode,
    },
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};

fn nothing_type() -> Type {
    Type::Simple(TypeCode::Void)
}

#[tokio::test]
#[ignore]
async fn test_nothing_block_query() {
    let (mut client, db_name) =
        create_isolated_test_client("nothing_block_query")
            .await
            .expect("Failed to create test client");

    // Create a table with a regular column to have some rows
    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Nothing) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Insert data using SQL (Nothing columns can't be inserted via Block)
    client
        .query(format!(
            "INSERT INTO {}.test_table (id) VALUES (1), (2), (3)",
            db_name
        ))
        .await
        .expect("Failed to insert via SQL");

    // Now query the Nothing column
    let result = client
        .query(format!("SELECT value FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);
    let blocks = result.blocks();
    let block = &blocks[0];
    let column = block.column(0).expect("Column not found");
    let result_col = column
        .as_any()
        .downcast_ref::<ColumnNothing>()
        .expect("Invalid column type");

    // Nothing column should have the same size as the number of rows
    assert_eq!(result_col.len(), 3);

    // All values should be None
    for i in 0..3 {
        assert_eq!(result_col.at(i), None);
    }

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_nothing_with_nullable() {
    let (mut client, db_name) =
        create_isolated_test_client("nothing_nullable_query")
            .await
            .expect("Failed to create test client");

    // Nothing is often used with Nullable for NULL-only columns
    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Nullable(Nothing)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Insert rows with NULL values
    client
        .query(format!(
            "INSERT INTO {}.test_table (id, value) VALUES (1, NULL), (2, NULL), (3, NULL)",
            db_name
        ))
        .await
        .expect("Failed to insert via SQL");

    // Query the Nullable(Nothing) column
    let result = client
        .query(format!("SELECT value FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_nothing_empty_table() {
    let (mut client, db_name) =
        create_isolated_test_client("nothing_empty_query")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Nothing) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Query empty table
    let result = client
        .query(format!("SELECT value FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 0);

    cleanup_test_database(&db_name).await;
}

// Note: We don't have property-based tests for Nothing because:
// 1. Nothing columns can't be inserted via Block (SaveBody not supported)
// 2. They don't contain actual data to test
// 3. They're mainly used for NULL-only columns or as placeholders
