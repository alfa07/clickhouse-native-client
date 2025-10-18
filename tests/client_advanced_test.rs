//! Advanced Client Features Tests
//!
//! These tests verify advanced client capabilities found in the C++ clickhouse-cpp implementation:
//! - Query settings and options
//! - Column name escaping and special characters
//! - Client information (name, version)
//! - Connection options and parameters
//! - Query cancellation (when implemented)
//!
//! ## Prerequisites
//! 1. Start ClickHouse server: `just start-db`
//! 2. Run tests: `cargo test --test client_advanced_test -- --ignored --nocapture`

use clickhouse_client::{Block, Client, ClientOptions};
use clickhouse_client::column::numeric::ColumnUInt64;
use clickhouse_client::column::string::ColumnString;
use clickhouse_client::types::Type;
use std::sync::Arc;

/// Helper to create a test client
async fn create_test_client() -> Result<Client, Box<dyn std::error::Error>> {
    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default")
        .password("");

    Ok(Client::connect(opts).await?)
}

// ============================================================================
// Client Options and Settings Tests
// ============================================================================

#[tokio::test]
#[ignore] // Requires running ClickHouse server
async fn test_client_with_custom_database() {
    // Create a client connected to a specific database
    let opts = ClientOptions::new("localhost", 9000)
        .database("system")
        .user("default")
        .password("");

    let mut client = Client::connect(opts).await.expect("Failed to connect");

    // Query from system database
    let result = client.query("SELECT name FROM tables LIMIT 1")
        .await
        .expect("Failed to query system.tables");

    assert!(result.total_rows() > 0, "Should have at least one table in system database");
}

#[tokio::test]
#[ignore]
async fn test_client_with_compression_lz4() {
    use clickhouse_client::protocol::CompressionMethod;

    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default")
        .password("")
        .compression(Some(CompressionMethod::LZ4));

    let mut client = Client::connect(opts).await.expect("Failed to connect");

    // Create table
    client.query("DROP TABLE IF EXISTS test_compression_lz4").await.ok();
    client.query("CREATE TABLE IF NOT EXISTS test_compression_lz4 (id UInt64, text String) ENGINE = Memory")
        .await
        .expect("Failed to create table");

    // Insert data with compression
    let mut block = Block::new();

    let mut id_col = ColumnUInt64::new(Type::uint64());
    let mut text_col = ColumnString::new(Type::string());

    for i in 0..100 {
        id_col.append(i);
        text_col.append(format!("Text row {}", i));
    }

    block.append_column("id", Arc::new(id_col)).unwrap();
    block.append_column("text", Arc::new(text_col)).unwrap();

    client.insert("test_compression_lz4", block).await.expect("Failed to insert with LZ4 compression");

    // Query back
    let result = client.query("SELECT COUNT(*) FROM test_compression_lz4")
        .await
        .expect("Failed to query");

    assert_eq!(result.total_rows(), 1);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_compression_lz4").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_query_with_settings() {
    let mut client = create_test_client().await.expect("Failed to connect");

    // Query with max_threads setting
    // Note: Settings are typically passed via Query object or as part of the query string
    let result = client.query("SELECT 1 SETTINGS max_threads = 1")
        .await
        .expect("Failed to query with settings");

    assert_eq!(result.total_rows(), 1);
}

// ============================================================================
// Column Name Escaping Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_column_name_escaping_backticks() {
    let mut client = create_test_client().await.expect("Failed to connect");

    // Create table with column name containing backticks
    client.query("DROP TABLE IF EXISTS test_escape_backticks").await.ok();
    client.query(r#"
        CREATE TABLE IF NOT EXISTS test_escape_backticks (
            `col``with``backticks` UInt64
        ) ENGINE = Memory
    "#).await.expect("Failed to create table");

    // Insert using block - column name should be escaped properly
    let mut block = Block::new();

    let mut col = ColumnUInt64::new(Type::uint64());
    col.append(42);

    // When appending column, backticks in name should be handled
    block.append_column("col`with`backticks", Arc::new(col)).unwrap();

    client.insert("test_escape_backticks", block).await.expect("Failed to insert");

    // Query back
    let result = client.query(r#"SELECT `col``with``backticks` FROM test_escape_backticks"#)
        .await
        .expect("Failed to query");

    assert_eq!(result.total_rows(), 1);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_escape_backticks").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_column_name_with_dots() {
    let mut client = create_test_client().await.expect("Failed to connect");

    // Create table with column name containing dots
    client.query("DROP TABLE IF EXISTS test_col_dots").await.ok();
    client.query(r#"
        CREATE TABLE IF NOT EXISTS test_col_dots (
            `column.with.dots` String
        ) ENGINE = Memory
    "#).await.expect("Failed to create table");

    // Insert data
    let mut block = Block::new();

    let mut col = ColumnString::new(Type::string());
    col.append("test".to_string());

    block.append_column("column.with.dots", Arc::new(col)).unwrap();

    client.insert("test_col_dots", block).await.expect("Failed to insert");

    // Query back with escaped name
    let result = client.query(r#"SELECT `column.with.dots` FROM test_col_dots"#)
        .await
        .expect("Failed to query");

    assert_eq!(result.total_rows(), 1);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_col_dots").await.ok();
}

// ============================================================================
// Query Object Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_query_object_basic() {
    let mut client = create_test_client().await.expect("Failed to connect");

    // Use Query object - currently we only support string queries via .query()
    // Query object is used internally but not exposed directly
    let result = client.query("SELECT 1 AS number").await.expect("Failed to query");

    assert_eq!(result.total_rows(), 1);
}

#[tokio::test]
#[ignore]
async fn test_query_with_multiple_statements() {
    let mut client = create_test_client().await.expect("Failed to connect");

    // Execute multiple SELECT statements
    let result1 = client.query("SELECT 1").await.expect("Failed to query");
    assert_eq!(result1.total_rows(), 1);

    let result2 = client.query("SELECT 2").await.expect("Failed to query");
    assert_eq!(result2.total_rows(), 1);
}

// ============================================================================
// Multiple Queries in Sequence
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_multiple_sequential_queries() {
    let mut client = create_test_client().await.expect("Failed to connect");

    // Execute multiple queries sequentially
    for i in 0..10 {
        let query = format!("SELECT {} AS value", i);
        let result = client.query(query.as_str()).await.expect("Failed to query");
        assert_eq!(result.total_rows(), 1);
    }
}

#[tokio::test]
#[ignore]
async fn test_mixed_ddl_and_select() {
    let mut client = create_test_client().await.expect("Failed to connect");

    // DDL
    client.query("DROP TABLE IF EXISTS test_mixed_ops").await.ok();
    client.query("CREATE TABLE IF NOT EXISTS test_mixed_ops (id UInt64) ENGINE = Memory")
        .await
        .expect("Failed to create table");

    // Insert
    client.query("INSERT INTO test_mixed_ops VALUES (1), (2), (3)")
        .await
        .expect("Failed to insert");

    // Select
    let result = client.query("SELECT COUNT(*) FROM test_mixed_ops")
        .await
        .expect("Failed to query");
    assert_eq!(result.total_rows(), 1);

    // More DDL
    client.query("DROP TABLE IF EXISTS test_mixed_ops").await.ok();
}

// ============================================================================
// Connection Stability Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_connection_reuse() {
    let mut client = create_test_client().await.expect("Failed to connect");

    // Ping to verify connection
    client.ping().await.expect("Initial ping failed");

    // Execute query
    let _result = client.query("SELECT 1").await.expect("First query failed");

    // Ping again
    client.ping().await.expect("Second ping failed");

    // Execute another query
    let _result = client.query("SELECT 2").await.expect("Second query failed");

    // Connection should still be valid
    client.ping().await.expect("Final ping failed");
}

#[tokio::test]
#[ignore]
async fn test_large_number_of_queries() {
    let mut client = create_test_client().await.expect("Failed to connect");

    // Execute many queries on same connection
    for i in 0..100 {
        let query = format!("SELECT {}", i);
        let result = client.query(query.as_str())
            .await
            .expect("Query failed");
        assert_eq!(result.total_rows(), 1);
    }
}

// ============================================================================
// Empty Results Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_query_returning_empty_result() {
    let mut client = create_test_client().await.expect("Failed to connect");

    // Create empty table
    client.query("DROP TABLE IF EXISTS test_empty_result").await.ok();
    client.query("CREATE TABLE IF NOT EXISTS test_empty_result (id UInt64) ENGINE = Memory")
        .await
        .expect("Failed to create table");

    // Query empty table
    let result = client.query("SELECT * FROM test_empty_result")
        .await
        .expect("Failed to query empty table");

    assert_eq!(result.total_rows(), 0);
    assert_eq!(result.blocks().len(), 0);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_empty_result").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_query_with_where_no_matches() {
    let mut client = create_test_client().await.expect("Failed to connect");

    // Create and populate table
    client.query("DROP TABLE IF EXISTS test_no_matches").await.ok();
    client.query("CREATE TABLE IF NOT EXISTS test_no_matches (id UInt64) ENGINE = Memory")
        .await
        .expect("Failed to create table");

    client.query("INSERT INTO test_no_matches VALUES (1), (2), (3)")
        .await
        .expect("Failed to insert");

    // Query with WHERE that matches nothing
    let result = client.query("SELECT * FROM test_no_matches WHERE id > 1000")
        .await
        .expect("Failed to query");

    assert_eq!(result.total_rows(), 0);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_no_matches").await.ok();
}

// ============================================================================
// Special Characters in Data Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_string_with_newlines() {
    let mut client = create_test_client().await.expect("Failed to connect");

    client.query("DROP TABLE IF EXISTS test_newlines").await.ok();
    client.query("CREATE TABLE IF NOT EXISTS test_newlines (text String) ENGINE = Memory")
        .await
        .expect("Failed to create table");

    // Insert string with newlines
    let mut block = Block::new();
    let mut col = ColumnString::new(Type::string());
    col.append("line1\nline2\nline3".to_string());
    col.append("single line".to_string());
    col.append("\n\n\n".to_string());

    block.append_column("text", Arc::new(col)).unwrap();

    client.insert("test_newlines", block).await.expect("Failed to insert");

    // Query back
    let result = client.query("SELECT * FROM test_newlines")
        .await
        .expect("Failed to query");

    assert_eq!(result.total_rows(), 3);

    // Verify data
    let blocks = result.blocks();
    let first_block = &blocks[0];
    if let Some(text_col) = first_block.column(0) {
        let text_str = text_col.as_any().downcast_ref::<ColumnString>().unwrap();
        assert_eq!(text_str.at(0), "line1\nline2\nline3");
        assert_eq!(text_str.at(1), "single line");
        assert_eq!(text_str.at(2), "\n\n\n");
    }

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_newlines").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_string_with_unicode() {
    let mut client = create_test_client().await.expect("Failed to connect");

    client.query("DROP TABLE IF EXISTS test_unicode").await.ok();
    client.query("CREATE TABLE IF NOT EXISTS test_unicode (text String) ENGINE = Memory")
        .await
        .expect("Failed to create table");

    // Insert Unicode strings
    let mut block = Block::new();
    let mut col = ColumnString::new(Type::string());
    col.append("Hello 世界".to_string());
    col.append("Привет мир".to_string());
    col.append("🚀 rocket 🎉".to_string());
    col.append("مرحبا بالعالم".to_string());

    block.append_column("text", Arc::new(col)).unwrap();

    client.insert("test_unicode", block).await.expect("Failed to insert Unicode");

    // Query back
    let result = client.query("SELECT * FROM test_unicode")
        .await
        .expect("Failed to query");

    assert_eq!(result.total_rows(), 4);

    // Verify Unicode is preserved
    let blocks = result.blocks();
    let first_block = &blocks[0];
    if let Some(text_col) = first_block.column(0) {
        let text_str = text_col.as_any().downcast_ref::<ColumnString>().unwrap();
        assert_eq!(text_str.at(0), "Hello 世界");
        assert_eq!(text_str.at(1), "Привет мир");
        assert_eq!(text_str.at(2), "🚀 rocket 🎉");
        assert_eq!(text_str.at(3), "مرحبا بالعالم");
    }

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_unicode").await.ok();
}
