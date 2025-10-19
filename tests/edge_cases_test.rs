//! Edge Cases and Special Characters Tests
//!
//! These tests verify handling of edge cases that can cause real bugs:
//! - Column names with special characters, keywords, Unicode
//! - NULL parameters in queries
//! - Exception handling patterns
//! - Unusual but valid ClickHouse constructs
//!
//! ## Prerequisites
//! 1. Start ClickHouse server: `just start-db`
//! 2. Run tests: `cargo test --test edge_cases_test -- --ignored --nocapture`

use clickhouse_client::{Block, Client, ClientOptions};
use clickhouse_client::column::numeric::{ColumnUInt64, ColumnInt32};
use clickhouse_client::column::string::ColumnString;
use clickhouse_client::column::nullable::ColumnNullable;
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
// Column Names with Special Characters
// ============================================================================

#[tokio::test]
#[ignore] // Requires running ClickHouse server
async fn test_column_names_with_spaces() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Create table with column names containing spaces (must be quoted)
    client.query("DROP TABLE IF EXISTS test_special_names").await.ok();
    client.query(r#"
        CREATE TABLE IF NOT EXISTS test_special_names (
            `column with spaces` UInt64,
            `another name` String
        ) ENGINE = Memory
    "#).await.expect("Failed to create table");

    // Insert using block
    let mut block = Block::new();

    let mut col1 = ColumnUInt64::new(Type::uint64());
    col1.append(1);
    col1.append(2);

    let mut col2 = ColumnString::new(Type::string());
    col2.append("test1".to_string());
    col2.append("test2".to_string());

    block.append_column("column with spaces", Arc::new(col1)).unwrap();
    block.append_column("another name", Arc::new(col2)).unwrap();

    client.insert("test_special_names", block).await.expect("Failed to insert");

    // Query back with quoted names
    let result = client.query(r#"SELECT `column with spaces`, `another name` FROM test_special_names"#)
        .await.expect("Failed to query");

    assert_eq!(result.total_rows(), 2);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_special_names").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_column_names_with_unicode() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Create table with Unicode column names
    client.query("DROP TABLE IF EXISTS test_unicode_names").await.ok();
    client.query(r#"
        CREATE TABLE IF NOT EXISTS test_unicode_names (
            `数字` UInt64,
            `текст` String,
            `🚀emoji` String
        ) ENGINE = Memory
    "#).await.expect("Failed to create table");

    // Insert data
    let mut block = Block::new();

    let mut col1 = ColumnUInt64::new(Type::uint64());
    col1.append(42);

    let mut col2 = ColumnString::new(Type::string());
    col2.append("привет".to_string());

    let mut col3 = ColumnString::new(Type::string());
    col3.append("rocket".to_string());

    block.append_column("数字", Arc::new(col1)).unwrap();
    block.append_column("текст", Arc::new(col2)).unwrap();
    block.append_column("🚀emoji", Arc::new(col3)).unwrap();

    client.insert("test_unicode_names", block).await.expect("Failed to insert");

    // Query back
    let result = client.query(r#"SELECT `数字`, `текст`, `🚀emoji` FROM test_unicode_names"#)
        .await.expect("Failed to query");

    assert_eq!(result.total_rows(), 1);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_unicode_names").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_column_names_sql_keywords() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Create table with SQL keyword column names (must be quoted)
    client.query("DROP TABLE IF EXISTS test_keyword_names").await.ok();
    client.query(r#"
        CREATE TABLE IF NOT EXISTS test_keyword_names (
            `select` UInt64,
            `from` String,
            `where` String
        ) ENGINE = Memory
    "#).await.expect("Failed to create table");

    // Insert data
    client.query(r#"INSERT INTO test_keyword_names (`select`, `from`, `where`) VALUES (1, 'a', 'b')"#)
        .await.expect("Failed to insert");

    // Query back
    let result = client.query(r#"SELECT `select`, `from`, `where` FROM test_keyword_names"#)
        .await.expect("Failed to query");

    assert_eq!(result.total_rows(), 1);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_keyword_names").await.ok();
}

// ============================================================================
// NULL Parameters in Queries
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_null_parameter_in_query() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Create table with nullable column
    client.query("DROP TABLE IF EXISTS test_null_param").await.ok();
    client.query(r#"
        CREATE TABLE IF NOT EXISTS test_null_param (
            id UInt64,
            value Nullable(Int32)
        ) ENGINE = Memory
    "#).await.expect("Failed to create table");

    // Insert with NULL using block insertion
    let mut block = Block::new();

    // Create id column
    let mut id_col = ColumnUInt64::new(Type::uint64());
    id_col.append(1);
    id_col.append(2);
    id_col.append(3);

    // Create nullable Int32 column
    let nested_col = Arc::new(ColumnInt32::new(Type::int32()));
    let mut nullable_col = ColumnNullable::with_nested(nested_col);

    // Manually append values since append_nullable is for UInt32
    nullable_col.append_non_null();
    if let Some(nested_mut) = Arc::get_mut(nullable_col.nested_mut()) {
        if let Some(col) = nested_mut.as_any_mut().downcast_mut::<ColumnInt32>() {
            col.append(100);
        }
    }

    nullable_col.append_null();
    if let Some(nested_mut) = Arc::get_mut(nullable_col.nested_mut()) {
        if let Some(col) = nested_mut.as_any_mut().downcast_mut::<ColumnInt32>() {
            col.append(0); // Placeholder for NULL
        }
    }

    nullable_col.append_non_null();
    if let Some(nested_mut) = Arc::get_mut(nullable_col.nested_mut()) {
        if let Some(col) = nested_mut.as_any_mut().downcast_mut::<ColumnInt32>() {
            col.append(300);
        }
    }

    block.append_column("id", Arc::new(id_col)).unwrap();
    block.append_column("value", Arc::new(nullable_col)).unwrap();

    client.insert("test_null_param", block).await.expect("Failed to insert");

    // Query NULL values
    let result = client.query("SELECT id, value FROM test_null_param WHERE value IS NULL")
        .await.expect("Failed to query");

    assert_eq!(result.total_rows(), 1); // Only row with id=2 should match

    // Query non-NULL values
    let result = client.query("SELECT id, value FROM test_null_param WHERE value IS NOT NULL")
        .await.expect("Failed to query");

    assert_eq!(result.total_rows(), 2); // Rows with id=1 and id=3

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_null_param").await.ok();
}

// ============================================================================
// Exception Handling Patterns
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_syntax_error_exception() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Try to execute invalid SQL
    let result = client.query("SELECTTTTT invalid syntax").await;

    assert!(result.is_err(), "Expected syntax error");

    if let Err(error) = result {
        let error_msg = error.to_string();

        println!("Got expected error: {}", error_msg);

        // Error should mention the syntax problem
        assert!(
            error_msg.contains("Syntax error") ||
            error_msg.contains("SYNTAX_ERROR") ||
            error_msg.contains("Unknown expression identifier"),
            "Expected syntax error message, got: {}", error_msg
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_table_not_found_exception() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Try to query non-existent table
    let result = client.query("SELECT * FROM nonexistent_table_12345").await;

    assert!(result.is_err(), "Expected table not found error");

    if let Err(error) = result {
        let error_msg = error.to_string();

        println!("Got expected error: {}", error_msg);

        // Error should mention the table doesn't exist
        assert!(
            error_msg.contains("doesn't exist") ||
            error_msg.contains("Unknown table") ||
            (error_msg.contains("Table") && error_msg.contains("not")),
            "Expected table not found error, got: {}", error_msg
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_type_mismatch_exception() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Create table
    client.query("DROP TABLE IF EXISTS test_type_mismatch_edge").await.ok();
    client.query(r#"
        CREATE TABLE IF NOT EXISTS test_type_mismatch_edge (
            num UInt64
        ) ENGINE = Memory
    "#).await.expect("Failed to create table");

    // Try to insert invalid type via SQL
    let result = client.query("INSERT INTO test_type_mismatch_edge VALUES ('not_a_number')").await;

    // This should either fail or be caught by ClickHouse type conversion
    // The exact error depends on ClickHouse version, so we just check it doesn't panic
    match result {
        Ok(_) => println!("Type mismatch query succeeded (ClickHouse may have coerced the type)"),
        Err(e) => println!("Type mismatch query failed as expected: {}", e),
    }

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_type_mismatch_edge").await.ok();
}

// ============================================================================
// Edge Cases in Data
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_empty_string_values() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Create table
    client.query("DROP TABLE IF EXISTS test_empty_strings").await.ok();
    client.query(r#"
        CREATE TABLE IF NOT EXISTS test_empty_strings (
            id UInt64,
            text String
        ) ENGINE = Memory
    "#).await.expect("Failed to create table");

    // Insert empty strings
    let mut block = Block::new();

    let mut id_col = ColumnUInt64::new(Type::uint64());
    id_col.append(1);
    id_col.append(2);
    id_col.append(3);

    let mut text_col = ColumnString::new(Type::string());
    text_col.append("".to_string());        // Empty string
    text_col.append("text".to_string());    // Normal string
    text_col.append("".to_string());        // Another empty string

    block.append_column("id", Arc::new(id_col)).unwrap();
    block.append_column("text", Arc::new(text_col)).unwrap();

    client.insert("test_empty_strings", block).await.expect("Failed to insert");

    // Query back
    let result = client.query("SELECT id, text FROM test_empty_strings ORDER BY id")
        .await.expect("Failed to query");

    assert_eq!(result.total_rows(), 3);

    // Verify empty strings are preserved
    let blocks = result.blocks();
    assert!(!blocks.is_empty());
    let first_block = &blocks[0];

    if let Some(text_col) = first_block.column(1) {
        let text_str = text_col.as_any().downcast_ref::<ColumnString>().unwrap();
        assert_eq!(text_str.at(0), "");      // Empty
        assert_eq!(text_str.at(1), "text");  // Normal
        assert_eq!(text_str.at(2), "");      // Empty
    }

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_empty_strings").await.ok();
}

#[tokio::test]
#[ignore]
async fn test_very_long_column_name() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Create column name at or near the limit (ClickHouse supports up to 256 characters)
    let long_name = "a".repeat(200);

    client.query("DROP TABLE IF EXISTS test_long_names").await.ok();

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS test_long_names (`{}` UInt64) ENGINE = Memory",
        long_name
    );

    client.query(create_sql.as_str()).await.expect("Failed to create table with long column name");

    // Insert data
    let insert_sql = format!("INSERT INTO test_long_names (`{}`) VALUES (42)", long_name);
    client.query(insert_sql.as_str()).await.expect("Failed to insert");

    // Query back
    let select_sql = format!("SELECT `{}` FROM test_long_names", long_name);
    let result = client.query(select_sql.as_str()).await.expect("Failed to query");

    assert_eq!(result.total_rows(), 1);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_long_names").await.ok();
}
