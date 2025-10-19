//! Advanced Types Tests for ClickHouse Client
//!
//! These tests verify support for advanced ClickHouse types including Nothing,
//! Decimal128, IPv4/IPv6, Geo types, and Int128/UInt128.
//!
//! ## Prerequisites
//! 1. Start ClickHouse server: `just start-db`
//! 2. Run tests: `cargo test --test advanced_types_test -- --ignored
//!    --nocapture`
//!
//! ## Test Coverage
//! - Nothing type (Nullable(Nothing))
//! - Decimal128 with large precision
//! - IPv4 addresses
//! - IPv6 addresses
//! - Geo Point type
//! - Geo Polygon type
//! - Int128 values
//! - UInt128 values

use clickhouse_client::{
    Client,
    ClientOptions,
};

/// Helper to create a test client
async fn create_test_client() -> Result<Client, Box<dyn std::error::Error>> {
    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default")
        .password("");

    Ok(Client::connect(opts).await?)
}

#[tokio::test]
#[ignore] // Requires running ClickHouse server
async fn test_nothing_type() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // SELECT NULL returns Nullable(Nothing) type
    let result = client
        .query("SELECT NULL AS nothing_col")
        .await
        .expect("Query failed");

    println!("Nothing type query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 1);

    // Check column type
    if result.total_rows() > 0 {
        let blocks = result.blocks();
        let block = &blocks[0];
        if let Some(column) = block.column(0) {
            let col_type = column.column_type();
            println!("Column type: {}", col_type.name());
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_decimal128() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create table with Decimal128
    client.query("DROP TABLE IF EXISTS test_decimal128").await.ok();
    client.query("CREATE TABLE IF NOT EXISTS test_decimal128 (val Decimal128(20)) ENGINE = Memory")
        .await
        .expect("Failed to create table");

    // Insert decimal values that fit in Decimal128(20)
    // Decimal128(20) means 20 total digits of precision
    client
        .query("INSERT INTO test_decimal128 VALUES (12345678901234.56789)")
        .await
        .expect("INSERT failed");

    // Query back
    let result = client
        .query("SELECT * FROM test_decimal128")
        .await
        .expect("SELECT failed");

    println!("Decimal128 query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 1);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_decimal128").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_ipv4_column() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create table with IPv4
    client.query("DROP TABLE IF EXISTS test_ipv4").await.ok();
    client
        .query(
            "CREATE TABLE IF NOT EXISTS test_ipv4 (ip IPv4) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    // Insert IPv4 addresses
    client.query("INSERT INTO test_ipv4 VALUES ('127.0.0.1'), ('192.168.1.1'), ('8.8.8.8')")
        .await
        .expect("INSERT failed");

    // Query back
    let result = client
        .query("SELECT * FROM test_ipv4 ORDER BY ip")
        .await
        .expect("SELECT failed");

    println!("IPv4 query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 3);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_ipv4").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_ipv6_column() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create table with IPv6
    client.query("DROP TABLE IF EXISTS test_ipv6").await.ok();
    client
        .query(
            "CREATE TABLE IF NOT EXISTS test_ipv6 (ip IPv6) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    // Insert IPv6 addresses
    client.query("INSERT INTO test_ipv6 VALUES ('::1'), ('2001:db8::1'), ('fe80::1')")
        .await
        .expect("INSERT failed");

    // Query back
    let result =
        client.query("SELECT * FROM test_ipv6").await.expect("SELECT failed");

    println!("IPv6 query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 3);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_ipv6").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_point_type() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Point is Tuple(Float64, Float64)
    let result = client
        .query("SELECT (1.5, 2.5) AS point")
        .await
        .expect("Query failed");

    println!("Point type query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 1);
}

#[tokio::test]
#[ignore]
async fn test_polygon_type() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Polygon is Array(Array(Tuple(Float64, Float64)))
    // Create a simple triangle
    let query =
        "SELECT [[(0.0, 0.0), (1.0, 0.0), (0.5, 1.0), (0.0, 0.0)]] AS polygon";

    let result = client.query(query).await.expect("Polygon query failed");

    println!("Polygon type query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 1);
}

#[tokio::test]
#[ignore]
async fn test_int128_column() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create table with Int128
    client.query("DROP TABLE IF EXISTS test_int128").await.ok();
    client.query("CREATE TABLE IF NOT EXISTS test_int128 (val Int128) ENGINE = Memory")
        .await
        .expect("Failed to create table");

    // Insert large Int128 values
    client.query("INSERT INTO test_int128 VALUES (0), (-1), (1), (9223372036854775807), (-9223372036854775808)")
        .await
        .expect("INSERT failed");

    // Query back
    let result = client
        .query("SELECT * FROM test_int128 ORDER BY val")
        .await
        .expect("SELECT failed");

    println!("Int128 query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 5);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_int128").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_uint128_column() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create table with UInt128
    client.query("DROP TABLE IF EXISTS test_uint128").await.ok();
    client.query("CREATE TABLE IF NOT EXISTS test_uint128 (val UInt128) ENGINE = Memory")
        .await
        .expect("Failed to create table");

    // Insert large UInt128 values
    client
        .query(
            "INSERT INTO test_uint128 VALUES (0), (1), (18446744073709551615)",
        )
        .await
        .expect("INSERT failed");

    // Query back
    let result = client
        .query("SELECT * FROM test_uint128 ORDER BY val")
        .await
        .expect("SELECT failed");

    println!("UInt128 query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 3);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_uint128").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_nullable_nothing() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Test Nullable(Nothing) - ClickHouse doesn't allow this in table
    // definitions This test verifies that we correctly handle the expected
    // error
    client.query("DROP TABLE IF EXISTS test_nullable_nothing").await.ok();
    let result = client.query("CREATE TABLE IF NOT EXISTS test_nullable_nothing (val Nullable(Nothing)) ENGINE = Memory")
        .await;

    // ClickHouse should reject Nullable(Nothing) in table definitions
    assert!(
        result.is_err(),
        "Expected CREATE TABLE with Nullable(Nothing) to fail"
    );

    if let Err(error) = result {
        let error_msg = error.to_string();
        println!("Got expected error: {}", error_msg);

        // Verify the error is about Nullable(Nothing) not being allowed
        assert!(
            error_msg.contains("Nullable(Nothing)")
                || error_msg.contains("cannot be used in tables"),
            "Error should mention Nullable(Nothing) restriction, got: {}",
            error_msg
        );
    }

    // However, SELECT NULL (which returns Nullable(Nothing)) should work fine
    let result = client
        .query("SELECT NULL AS nothing_col")
        .await
        .expect("SELECT NULL should work");
    println!("SELECT NULL returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 1);
}

#[tokio::test]
#[ignore]
async fn test_mixed_advanced_types() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create table with multiple advanced types
    client.query("DROP TABLE IF EXISTS test_mixed_types").await.ok();
    let create_sql = r#"
        CREATE TABLE IF NOT EXISTS test_mixed_types (
            id UInt64,
            ip4 IPv4,
            ip6 IPv6,
            dec Decimal128(10),
            big_int Int128
        ) ENGINE = Memory
    "#;
    client.query(create_sql).await.expect("Failed to create table");

    // Insert mixed data
    let insert_sql = r#"
        INSERT INTO test_mixed_types VALUES
            (1, '127.0.0.1', '::1', 123.45, 1000000000000),
            (2, '192.168.1.1', '2001:db8::1', 678.90, -9999999999999)
    "#;
    client.query(insert_sql).await.expect("INSERT failed");

    // Query back
    let result = client
        .query("SELECT * FROM test_mixed_types ORDER BY id")
        .await
        .expect("SELECT failed");

    println!(
        "Mixed advanced types query returned {} rows",
        result.total_rows()
    );
    assert_eq!(result.total_rows(), 2);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_mixed_types").await.ok();
}
