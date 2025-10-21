/// Integration tests for Tuple compound types
/// Tests: Tuple(Float32, Float64), Tuple(Int32, Int64), Tuple(String, Int64),
///        Tuple(String, Int64, Array(String))
mod common;

use clickhouse_client::Block;
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};

// ============================================================================
// Tuple(Float32, Float64)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_tuple_float32_float64() {
    let (mut client, db_name) =
        create_isolated_test_client("tuple_float32_float64")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Tuple(Float32, Float64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Use SQL INSERT for tuples
    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ((1.5, 2.5)), ((0.0, 0.0)), ((-1.5, -2.5)), ((3.14159, 2.71828))",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT values FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Tuple(Int32, Int64)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_tuple_int32_int64() {
    let (mut client, db_name) =
        create_isolated_test_client("tuple_int32_int64")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Tuple(Int32, Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ((0, 0)), ((100, 1000)), ((-100, -1000)), ((2147483647, 9223372036854775807))",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT values FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Tuple(String, Int64)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_tuple_string_int64() {
    let (mut client, db_name) =
        create_isolated_test_client("tuple_string_int64")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (kv Tuple(String, Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES (('key1', 100)), (('key2', 200)), (('', 0)), (('test', -500))",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT kv FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Tuple(String, Int64, Array(String))
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_tuple_string_int64_array_string() {
    let (mut client, db_name) = create_isolated_test_client("tuple_complex")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (data Tuple(String, Int64, Array(String))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES
            (('item1', 100, ['tag1', 'tag2'])),
            (('item2', 200, [])),
            (('item3', 300, ['tag3', 'tag4', 'tag5'])),
            (('', 0, []))",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT data FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Tuple with empty arrays
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_tuple_with_empty_values() {
    let (mut client, db_name) =
        create_isolated_test_client("tuple_empty_values")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (data Tuple(String, Array(Int32))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES
            (('', [])),
            (('test', [])),
            (('', [1, 2, 3]))",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT data FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    cleanup_test_database(&db_name).await;
}
