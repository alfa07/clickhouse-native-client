/// Integration tests for Map compound types
/// Tests: Map(Int8, String), Map(String, Array(Array(Int8))),
///        Map(UUID, Nullable(String)), Map(UUID,
/// Nullable(LowCardinality(String)))
mod common;

use clickhouse_client::Block;
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};

// ============================================================================
// Map(Int8, String)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_map_int8_string() {
    let (mut client, db_name) = create_isolated_test_client("map_int8_string")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (data Map(Int8, String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Use SQL INSERT for maps
    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES
            ({{1: 'one', 2: 'two', 3: 'three'}}),
            ({{}}),
            ({{0: 'zero', -1: 'minus_one'}})",
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

// ============================================================================
// Map(String, Array(Array(Int8)))
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_map_string_nested_arrays() {
    let (mut client, db_name) =
        create_isolated_test_client("map_nested_arrays")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (data Map(String, Array(Array(Int8)))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES
            ({{'key1': [[1, 2], [3, 4]], 'key2': [[5, 6]]}}),
            ({{}}),
            ({{'empty': [], 'single': [[1]]}})",
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

// ============================================================================
// Map(UUID, Nullable(String))
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_map_uuid_nullable_string() {
    let (mut client, db_name) =
        create_isolated_test_client("map_uuid_nullable")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (data Map(UUID, Nullable(String))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES
            ({{'550e8400-e29b-41d4-a716-446655440000': 'test', '6ba7b810-9dad-11d1-80b4-00c04fd430c8': NULL}}),
            ({{}}),
            ({{'ffffffff-ffff-ffff-ffff-ffffffffffff': 'max'}})",
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

// ============================================================================
// Map(UUID, Nullable(LowCardinality(String)))
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_map_uuid_nullable_lowcardinality_string() {
    let (mut client, db_name) = create_isolated_test_client("map_uuid_lc")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (data Map(UUID, Nullable(LowCardinality(String)))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES
            ({{'550e8400-e29b-41d4-a716-446655440000': 'tag1', '6ba7b810-9dad-11d1-80b4-00c04fd430c8': 'tag2'}}),
            ({{}}),
            ({{'00000000-0000-0000-0000-000000000000': NULL}})",
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

// ============================================================================
// Map with empty maps
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_map_empty() {
    let (mut client, db_name) = create_isolated_test_client("map_empty")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (data Map(String, Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ({{}}), ({{}}), ({{'key': 100}})",
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
