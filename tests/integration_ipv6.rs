/// Integration tests for IPv6 type
mod common;

use common::{
    cleanup_test_database,
    create_isolated_test_client,
};

#[tokio::test]
#[ignore]
async fn test_ipv6_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("ipv6_roundtrip")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (ip IPv6) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Use SQL INSERT for IPv6
    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ('::'), ('::1'), ('2001:db8::1'), ('fe80::1'), ('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT ip FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 5);

    cleanup_test_database(&db_name).await;
}
