/// Integration tests for IPv4 type
mod common;

use common::{
    cleanup_test_database,
    create_isolated_test_client,
};

#[tokio::test]
#[ignore]
async fn test_ipv4_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("ipv4_roundtrip")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (ip IPv4) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Use SQL INSERT for IPv4
    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ('0.0.0.0'), ('127.0.0.1'), ('192.168.1.1'), ('10.0.0.1'), ('8.8.8.8'), ('255.255.255.255')",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT ip FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 6);

    cleanup_test_database(&db_name).await;
}
