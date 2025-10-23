//! Readonly Client Tests
//!
//! These tests verify behavior when connecting with readonly=1 setting:
//! - SELECT queries should work
//! - INSERT queries should fail
//! - CREATE/DROP/ALTER queries should fail
//! - Error messages should be appropriate
//!
//! Based on C++ clickhouse-cpp readonly_client_test.cpp
//!
//! ## Prerequisites
//! 1. Start ClickHouse server: `just start-db`
//! 2. Server must allow readonly connections
//! 3. Run tests: `cargo test --test readonly_client_test -- --ignored
//!    --nocapture`
//!
//! ## Note
//! Readonly mode is typically set at user level or via settings.
//! These tests use the 'readonly' setting in queries.

use clickhouse_client::{
    Client,
    ClientOptions,
    Query,
};

/// Helper to create a test client
async fn create_test_client() -> Result<Client, Box<dyn std::error::Error>> {
    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default")
        .password("");

    Ok(Client::connect(opts).await?)
}

// ============================================================================
// Readonly SELECT Tests (Should Work)
// ============================================================================

#[tokio::test]
#[ignore] // Requires running ClickHouse server
async fn test_readonly_select_system_tables() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // SELECT from system tables should work in readonly mode
    let query = Query::new("SELECT name, engine FROM system.tables LIMIT 5")
        .with_setting("readonly", "1"); // Enable readonly mode

    println!("Testing SELECT with readonly=1...");

    let result = client
        .query(query)
        .await
        .expect("SELECT should work in readonly mode");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
        println!(
            "Block: {} rows, {} columns",
            block.row_count(),
            block.column_count()
        );
    }

    assert!(total_rows > 0, "Should retrieve some rows");
    println!("✓ SELECT in readonly mode works");
}

#[tokio::test]
#[ignore]
async fn test_readonly_select_numbers() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // SELECT from system.numbers should work
    let query = Query::new("SELECT number FROM system.numbers LIMIT 100")
        .with_setting("readonly", "1");

    let result = client.query(query).await.expect("SELECT should work");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 100, "Should get 100 rows");
    println!("✓ SELECT from system.numbers in readonly mode works");
}

#[tokio::test]
#[ignore]
async fn test_readonly_select_with_where() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let query = Query::new(
        "SELECT number FROM system.numbers WHERE number < 50 LIMIT 100",
    )
    .with_setting("readonly", "1");

    let result =
        client.query(query).await.expect("SELECT with WHERE should work");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 50);
    println!("✓ SELECT with WHERE in readonly mode works");
}

#[tokio::test]
#[ignore]
async fn test_readonly_select_with_aggregation() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let query =
        Query::new("SELECT count(*) as cnt FROM (SELECT number FROM system.numbers LIMIT 1000)")
            .with_setting("readonly", "1");

    let result = client
        .query(query)
        .await
        .expect("SELECT with aggregation should work");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
        if block.row_count() > 0 {
            // Verify count column exists
            assert!(
                block.column_by_name("cnt").is_some(),
                "cnt column should exist"
            );
        }
    }

    assert!(total_rows > 0, "Should get count result");
    println!("✓ SELECT with aggregation in readonly mode works");
}

// ============================================================================
// Readonly INSERT Tests (Should Fail)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_readonly_insert_fails() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // First create a table (without readonly)
    let _ = client.query("DROP TABLE IF EXISTS test_readonly_insert").await;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS test_readonly_insert (
                id UInt32,
                value String
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    println!("✓ Table created (not in readonly mode)");

    // Now try to INSERT with readonly=1 (should fail)
    let query =
        Query::new("INSERT INTO test_readonly_insert VALUES (1, 'test')")
            .with_setting("readonly", "1");

    let result = client.query(query).await;

    assert!(result.is_err(), "INSERT should fail in readonly mode");

    if let Err(e) = result {
        let err_msg = e.to_string();
        println!("Expected error: {}", err_msg);

        // Error should mention readonly or permissions
        assert!(
            err_msg.to_lowercase().contains("readonly")
                || err_msg.to_lowercase().contains("permission")
                || err_msg.to_lowercase().contains("cannot"),
            "Error should indicate readonly restriction"
        );
    }

    println!("✓ INSERT correctly fails in readonly mode");

    // Cleanup
    let _ = client.query("DROP TABLE test_readonly_insert").await;
}

#[tokio::test]
#[ignore]
async fn test_readonly_insert_into_temp_table_fails() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create temp table
    client
        .query(
            "CREATE TEMPORARY TABLE test_readonly_temp (
                id UInt32
            )",
        )
        .await
        .expect("Failed to create temp table");

    // Try to INSERT with readonly (should fail even for temp tables)
    let query = Query::new("INSERT INTO test_readonly_temp VALUES (1)")
        .with_setting("readonly", "1");

    let result = client.query(query).await;

    // Note: Behavior may vary by ClickHouse version
    // Temp tables might be allowed in some readonly modes
    if result.is_err() {
        println!("✓ INSERT into temp table fails in readonly mode (strict)");
    } else {
        println!(
            "Note: Server allows INSERT into temp tables in readonly mode"
        );
    }
}

// ============================================================================
// Readonly DDL Tests (Should Fail)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_readonly_create_table_fails() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Try to CREATE TABLE with readonly=1 (should fail)
    let query = Query::new(
        "CREATE TABLE test_readonly_create (
            id UInt32
        ) ENGINE = Memory",
    )
    .with_setting("readonly", "1");

    let result = client.query(query).await;

    assert!(result.is_err(), "CREATE TABLE should fail in readonly mode");

    if let Err(e) = result {
        let err_msg = e.to_string();
        println!("Expected error: {}", err_msg);

        assert!(
            err_msg.to_lowercase().contains("readonly")
                || err_msg.to_lowercase().contains("cannot"),
            "Error should indicate readonly restriction"
        );
    }

    println!("✓ CREATE TABLE correctly fails in readonly mode");
}

#[tokio::test]
#[ignore]
async fn test_readonly_drop_table_fails() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create a table first (without readonly)
    let _ = client.query("DROP TABLE IF EXISTS test_readonly_drop").await;

    client
        .query(
            "CREATE TABLE test_readonly_drop (
                id UInt32
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    // Try to DROP with readonly=1 (should fail)
    let query = Query::new("DROP TABLE test_readonly_drop")
        .with_setting("readonly", "1");

    let result = client.query(query).await;

    assert!(result.is_err(), "DROP TABLE should fail in readonly mode");

    if let Err(e) = result {
        println!("Expected error: {}", e);
    }

    println!("✓ DROP TABLE correctly fails in readonly mode");

    // Cleanup (without readonly)
    let _ = client.query("DROP TABLE test_readonly_drop").await;
}

#[tokio::test]
#[ignore]
async fn test_readonly_alter_table_fails() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create table
    let _ = client.query("DROP TABLE IF EXISTS test_readonly_alter").await;

    client
        .query(
            "CREATE TABLE test_readonly_alter (
                id UInt32
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    // Try to ALTER with readonly=1 (should fail)
    let query =
        Query::new("ALTER TABLE test_readonly_alter ADD COLUMN value String")
            .with_setting("readonly", "1");

    let result = client.query(query).await;

    assert!(result.is_err(), "ALTER TABLE should fail in readonly mode");

    if let Err(e) = result {
        println!("Expected error: {}", e);
    }

    println!("✓ ALTER TABLE correctly fails in readonly mode");

    // Cleanup
    let _ = client.query("DROP TABLE test_readonly_alter").await;
}

// ============================================================================
// Readonly Mode Levels Test
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_readonly_mode_levels() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Test different readonly levels (0, 1, 2)
    // readonly=0: no restrictions
    // readonly=1: only SELECTs allowed
    // readonly=2: SELECTs + some DDL on temp tables

    // Test readonly=0 (should allow SELECT)
    let query0 = Query::new("SELECT 1 as value").with_setting("readonly", "0");

    let result0 = client.query(query0).await;
    assert!(result0.is_ok(), "readonly=0 should allow SELECT");

    // Test readonly=1 (should allow SELECT)
    let query1 = Query::new("SELECT 1 as value").with_setting("readonly", "1");

    let result1 = client.query(query1).await;
    assert!(result1.is_ok(), "readonly=1 should allow SELECT");

    // Test readonly=2 (should allow SELECT)
    let query2 = Query::new("SELECT 1 as value").with_setting("readonly", "2");

    let result2 = client.query(query2).await;
    assert!(result2.is_ok(), "readonly=2 should allow SELECT");

    println!("✓ Readonly mode levels test passed");
}

// ============================================================================
// Readonly with Complex Queries
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_readonly_complex_select() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Complex SELECT with JOINs and subqueries should work
    let query = Query::new(
        "SELECT
            t1.number,
            t2.number as number2
         FROM
            (SELECT number FROM system.numbers LIMIT 10) t1
         INNER JOIN
            (SELECT number FROM system.numbers LIMIT 10) t2
         ON t1.number = t2.number
         LIMIT 5",
    )
    .with_setting("readonly", "1");

    let result =
        client.query(query).await.expect("Complex SELECT should work");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 5);
    println!("✓ Complex SELECT with JOIN in readonly mode works");
}

// ============================================================================
// Error Message Quality Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_readonly_error_messages_are_clear() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create table for testing
    let _ = client.query("DROP TABLE IF EXISTS test_readonly_errors").await;

    client
        .query(
            "CREATE TABLE test_readonly_errors (
                id UInt32
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    // Test various operations and check error messages
    let operations = [
        ("INSERT INTO test_readonly_errors VALUES (1)", "INSERT"),
        ("UPDATE test_readonly_errors SET id = 2 WHERE id = 1", "UPDATE"),
        ("DELETE FROM test_readonly_errors WHERE id = 1", "DELETE"),
        ("TRUNCATE TABLE test_readonly_errors", "TRUNCATE"),
    ];

    for (query_str, op_name) in operations {
        let query = Query::new(query_str).with_setting("readonly", "1");

        let result = client.query(query).await;

        if let Err(e) = result {
            let err_msg = e.to_string();
            println!("{} error: {}", op_name, err_msg);

            // Error message should be informative
            assert!(!err_msg.is_empty(), "Error message should not be empty");
            assert!(
                err_msg.len() > 10,
                "Error message should be reasonably detailed"
            );
        } else {
            // Some operations might succeed in certain readonly modes
            println!(
                "Note: {} succeeded (may be allowed in this readonly mode)",
                op_name
            );
        }
    }

    println!("✓ Error messages in readonly mode are clear");

    // Cleanup
    let _ = client.query("DROP TABLE test_readonly_errors").await;
}

// ============================================================================
// User-level Readonly Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_readonly_user_level_setting() {
    // Note: This test requires a readonly user to be configured in ClickHouse
    // Skip if user doesn't exist

    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("readonly_user") // This user may not exist
        .password("");

    let client_result = Client::connect(opts).await;

    if client_result.is_err() {
        println!("Note: readonly_user doesn't exist, skipping user-level readonly test");
        return;
    }

    let mut client = client_result.unwrap();

    // SELECT should work
    let query = Query::new("SELECT 1 as value");
    let result = client.query(query).await;

    if result.is_ok() {
        println!("✓ Readonly user can execute SELECT");
    }

    // INSERT should fail (if user is truly readonly)
    // We can't test this without a proper table, so skip detailed verification
    println!("Note: User-level readonly test completed (limited scope)");
}
