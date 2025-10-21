/// Integration tests for UUID type
mod common;

use common::{
    cleanup_test_database,
    create_isolated_test_client,
};

#[tokio::test]
#[ignore]
async fn test_uuid_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("uuid_roundtrip")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UUID) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Use SQL INSERT for UUID
    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ('00000000-0000-0000-0000-000000000000'), ('550e8400-e29b-41d4-a716-446655440000'), ('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), ('ffffffff-ffff-ffff-ffff-ffffffffffff')",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT id FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_uuid_generated() {
    let (mut client, db_name) = create_isolated_test_client("uuid_generated")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UUID) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Use ClickHouse's generateUUIDv4() function
    client
        .query(format!(
            "INSERT INTO {}.test_table SELECT generateUUIDv4() FROM numbers(10)",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT id FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 10);

    cleanup_test_database(&db_name).await;
}
