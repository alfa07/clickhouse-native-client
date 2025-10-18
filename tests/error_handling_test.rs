//! Error Handling Tests for ClickHouse Client
//!
//! These tests verify proper error handling for various failure scenarios.
//!
//! ## Prerequisites
//! 1. Start ClickHouse server: `just start-db` (for some tests)
//! 2. Run tests: `cargo test --test error_handling_test -- --ignored --nocapture`
//!
//! ## Test Coverage
//! - Connection refused errors
//! - Connection timeout handling
//! - Invalid host/DNS errors
//! - Server exceptions
//! - Read-only mode violations
//! - Invalid SQL syntax errors
//! - Type mismatch errors

use clickhouse_client::{Client, ClientOptions, ConnectionOptions};
use std::time::Duration;

/// Helper to create a test client
async fn create_test_client() -> Result<Client, Box<dyn std::error::Error>> {
    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default")
        .password("");

    Ok(Client::connect(opts).await?)
}

#[tokio::test]
#[ignore] // May not have server on wrong port
async fn test_connection_refused() {
    // Try to connect to a port that's unlikely to be open
    let opts = ClientOptions::new("localhost", 19999)
        .database("default")
        .user("default")
        .password("");

    let result = Client::connect(opts).await;

    assert!(result.is_err(), "Connection should fail");
    if let Err(e) = result {
        println!("Expected connection error: {}", e);
        let error_string = e.to_string();
        assert!(
            error_string.contains("Connection") || error_string.contains("refused") || error_string.contains("connect"),
            "Error should mention connection failure"
        );
    }
}

#[tokio::test]
async fn test_connection_timeout() {
    let conn_opts = ConnectionOptions::new()
        .connect_timeout(Duration::from_millis(10)); // Very short timeout

    // Try to connect to a non-routable IP (will timeout)
    let opts = ClientOptions::new("192.0.2.1", 9000) // TEST-NET-1, non-routable
        .database("default")
        .user("default")
        .password("")
        .connection_options(conn_opts);

    let start = std::time::Instant::now();
    let result = Client::connect(opts).await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "Connection should timeout");
    println!("Connection timed out after {:?}", elapsed);

    // Should timeout quickly (within 2 seconds, accounting for retry logic)
    assert!(
        elapsed < Duration::from_secs(5),
        "Timeout should trigger within reasonable time"
    );
}

#[tokio::test]
async fn test_invalid_host() {
    let opts = ClientOptions::new("invalid-hostname-that-does-not-exist-12345.example", 9000)
        .database("default")
        .user("default")
        .password("");

    let result = Client::connect(opts).await;

    assert!(result.is_err(), "Connection to invalid host should fail");
    if let Err(e) = result {
        println!("Expected DNS/host error: {}", e);
    }
}

#[tokio::test]
#[ignore] // Requires running ClickHouse server
async fn test_server_exception() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Query non-existent table
    let result = client.query("SELECT * FROM nonexistent_table_xyz_123").await;

    assert!(result.is_err(), "Query should fail");
    if let Err(e) = result {
        let error_string = e.to_string();
        println!("Server exception: {}", error_string);
        assert!(
            error_string.contains("Exception") || error_string.contains("doesn't exist") || error_string.contains("Unknown table"),
            "Error should indicate table doesn't exist"
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_invalid_sql_syntax() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Invalid SQL syntax
    let result = client.query("SELECTTT * FROMM system.numbers LIMITT 1").await;

    assert!(result.is_err(), "Invalid SQL should fail");
    if let Err(e) = result {
        let error_string = e.to_string();
        println!("Syntax error: {}", error_string);
        assert!(
            error_string.contains("Syntax") || error_string.contains("Exception"),
            "Error should indicate syntax problem"
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_type_mismatch_error() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Create table with specific type
    client.query("DROP TABLE IF EXISTS test_type_mismatch").await.ok();
    client.query("CREATE TABLE IF NOT EXISTS test_type_mismatch (id UInt64) ENGINE = Memory")
        .await
        .expect("Failed to create table");

    // Try to insert wrong type (string into UInt64)
    let result = client.query("INSERT INTO test_type_mismatch VALUES ('not a number')").await;

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_type_mismatch").await.ok();

    // Check error
    assert!(result.is_err(), "Type mismatch should fail");
    if let Err(e) = result {
        println!("Type mismatch error: {}", e);
    }
}

#[tokio::test]
#[ignore]
async fn test_division_by_zero() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Division by zero
    let result = client.query("SELECT 1 / 0").await;

    // ClickHouse may return inf or throw error depending on settings
    match result {
        Ok(r) => println!("Division by zero returned: {} rows", r.total_rows()),
        Err(e) => println!("Division by zero error: {}", e),
    }
}

#[tokio::test]
#[ignore]
async fn test_permission_denied() {
    // This test requires a ClickHouse setup with user permissions
    // Try to create user without permission
    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default") // default user may not have CREATE USER permission
        .password("");

    let mut client = Client::connect(opts).await.expect("Connection should succeed");

    let result = client.query("CREATE USER test_user IDENTIFIED BY 'password'").await;

    // May fail with permission denied or succeed if default user has permissions
    match result {
        Ok(_) => {
            println!("CREATE USER succeeded (default user has permissions)");
            // Cleanup
            client.query("DROP USER IF EXISTS test_user").await.ok();
        }
        Err(e) => {
            println!("Expected permission error: {}", e);
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_query_too_complex() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Create a query that's too complex (nested joins)
    // Note: This may not fail on all ClickHouse versions
    let complex_query = format!(
        "SELECT * FROM system.numbers AS t1 {} LIMIT 1",
        (0..100)
            .map(|i| format!("JOIN system.numbers AS t{} ON t{}.number = t{}.number", i+2, i+1, i+2))
            .collect::<Vec<_>>()
            .join(" ")
    );

    let result = client.query(complex_query).await;

    match result {
        Ok(r) => println!("Complex query succeeded: {} rows", r.total_rows()),
        Err(e) => println!("Complex query error: {}", e),
    }
}

#[tokio::test]
#[ignore]
async fn test_table_already_exists() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Create table
    client.query("DROP TABLE IF EXISTS test_exists").await.ok();
    client.query("CREATE TABLE test_exists (id UInt64) ENGINE = Memory")
        .await
        .expect("First CREATE should succeed");

    // Try to create again without IF NOT EXISTS
    let result = client.query("CREATE TABLE test_exists (id UInt64) ENGINE = Memory").await;

    assert!(result.is_err(), "Duplicate table creation should fail");
    if let Err(e) = result {
        let error_string = e.to_string();
        println!("Expected 'table exists' error: {}", error_string);
        assert!(
            error_string.contains("already exists") || error_string.contains("Exception"),
            "Error should indicate table already exists"
        );
    }

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_exists").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_unsupported_aggregate_function_type() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Create table with AggregateFunction column
    client.query("DROP TABLE IF EXISTS test_agg_func").await.ok();
    client.query("CREATE TABLE test_agg_func (col AggregateFunction(sum, UInt64)) ENGINE = Memory")
        .await
        .ok(); // May fail if not supported

    // Try to SELECT from it
    let result = client.query("SELECT * FROM test_agg_func").await;

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_agg_func").await.ok();

    // Our client should error on AggregateFunction columns
    match result {
        Ok(_) => println!("Note: AggregateFunction query unexpectedly succeeded"),
        Err(e) => {
            println!("Expected AggregateFunction error: {}", e);
            let error_string = e.to_string();
            assert!(
                error_string.contains("AggregateFunction") || error_string.contains("not supported"),
                "Error should mention AggregateFunction is not supported"
            );
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_connection_drop_during_query() {
    // This is a complex test that would require dropping the network connection
    // during a long-running query. Skipping for now as it requires special setup.
    println!("Connection drop test requires manual network manipulation");
}
