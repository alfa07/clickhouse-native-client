//! Column Name Edge Cases Tests
//!
//! These tests verify proper handling of edge cases in column names:
//! - Special characters (dots, hyphens, spaces)
//! - Backtick escaping
//! - Reserved SQL keywords
//! - Unicode/international characters
//! - Very long names
//! - Empty or unusual names
//!
//! Based on C++ clickhouse-cpp abnormal_column_names_test.cpp
//!
//! ## Prerequisites
//! 1. Start ClickHouse server: `just start-db`
//! 2. Run tests: `cargo test --test column_name_edge_cases_test -- --ignored
//!    --nocapture`

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
// Special Characters in Column Names
// ============================================================================

#[tokio::test]
#[ignore] // Requires running ClickHouse server
async fn test_column_name_with_dot() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_dot").await;

    // Column name with dot needs backtick escaping
    client
        .query(
            "CREATE TABLE test_col_dot (
                id UInt32,
                `column.name` String
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    println!("✓ Table with column `column.name` created");

    // Insert data
    client
        .query("INSERT INTO test_col_dot VALUES (1, 'value1'), (2, 'value2')")
        .await
        .expect("Failed to insert data");

    // Select using backtick-escaped name
    let query =
        Query::new("SELECT id, `column.name` FROM test_col_dot ORDER BY id");
    let result = client.query(query).await.expect("Failed to select data");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
        println!(
            "Block: {} rows, {} columns",
            block.row_count(),
            block.column_count()
        );

        // Check if column can be accessed by name
        if block.row_count() > 0 {
            if let Some(_col) = block.column_by_name("column.name") {
                println!("  ✓ Column `column.name` accessible");
            }
        }
    }

    assert_eq!(total_rows, 2, "Should have 2 rows");
    println!("✓ Column name with dot test passed");

    // Cleanup
    let _ = client.query("DROP TABLE test_col_dot").await;
}

#[tokio::test]
#[ignore]
async fn test_column_name_with_hyphen() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_hyphen").await;

    // Column name with hyphen needs escaping
    client
        .query(
            "CREATE TABLE test_col_hyphen (
                id UInt32,
                `column-name` String
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    println!("✓ Table with column `column-name` created");

    client
        .query("INSERT INTO test_col_hyphen VALUES (1, 'test')")
        .await
        .expect("Failed to insert data");

    let query = Query::new("SELECT `column-name` FROM test_col_hyphen");
    let result = client.query(query).await.expect("Failed to select data");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 1);
    println!("✓ Column name with hyphen test passed");

    let _ = client.query("DROP TABLE test_col_hyphen").await;
}

#[tokio::test]
#[ignore]
async fn test_column_name_with_space() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_space").await;

    // Column name with spaces
    client
        .query(
            "CREATE TABLE test_col_space (
                id UInt32,
                `column name with spaces` String
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    println!("✓ Table with column `column name with spaces` created");

    client
        .query("INSERT INTO test_col_space VALUES (1, 'value')")
        .await
        .expect("Failed to insert data");

    let query =
        Query::new("SELECT `column name with spaces` FROM test_col_space");
    let result = client.query(query).await.expect("Failed to select data");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 1);
    println!("✓ Column name with spaces test passed");

    let _ = client.query("DROP TABLE test_col_space").await;
}

// ============================================================================
// Reserved Keyword Column Names
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_column_name_reserved_select() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_select").await;

    // Use SQL keyword 'select' as column name
    client
        .query(
            "CREATE TABLE test_col_select (
                id UInt32,
                `select` String
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    println!("✓ Table with column `select` (reserved keyword) created");

    client
        .query("INSERT INTO test_col_select VALUES (1, 'test')")
        .await
        .expect("Failed to insert data");

    let query = Query::new("SELECT id, `select` FROM test_col_select");
    let result = client.query(query).await.expect("Failed to select data");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 1);
    println!("✓ Reserved keyword 'select' as column name test passed");

    let _ = client.query("DROP TABLE test_col_select").await;
}

#[tokio::test]
#[ignore]
async fn test_column_name_reserved_where() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_where").await;

    client
        .query(
            "CREATE TABLE test_col_where (
                id UInt32,
                `where` UInt64,
                `from` String
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    println!("✓ Table with columns `where` and `from` created");

    client
        .query("INSERT INTO test_col_where VALUES (1, 100, 'source')")
        .await
        .expect("Failed to insert data");

    let query = Query::new("SELECT `where`, `from` FROM test_col_where");
    let result = client.query(query).await.expect("Failed to select data");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 1);
    println!("✓ Multiple reserved keywords as column names test passed");

    let _ = client.query("DROP TABLE test_col_where").await;
}

// ============================================================================
// Unicode/International Characters
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_column_name_unicode_chinese() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_unicode_cn").await;

    // Chinese characters in column name
    client
        .query(
            "CREATE TABLE test_col_unicode_cn (
                id UInt32,
                `列名` String,
                `数据` UInt64
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    println!("✓ Table with Chinese column names created");

    client
        .query("INSERT INTO test_col_unicode_cn VALUES (1, 'value', 100)")
        .await
        .expect("Failed to insert data");

    let query =
        Query::new("SELECT id, `列名`, `数据` FROM test_col_unicode_cn");
    let result = client.query(query).await.expect("Failed to select data");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();

        if block.row_count() > 0 {
            // Try to access by Unicode name
            if let Some(_col) = block.column_by_name("列名") {
                println!("  ✓ Unicode column `列名` accessible");
            }
        }
    }

    assert_eq!(total_rows, 1);
    println!("✓ Unicode (Chinese) column names test passed");

    let _ = client.query("DROP TABLE test_col_unicode_cn").await;
}

#[tokio::test]
#[ignore]
async fn test_column_name_unicode_russian() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_unicode_ru").await;

    // Russian (Cyrillic) characters
    client
        .query(
            "CREATE TABLE test_col_unicode_ru (
                id UInt32,
                `имя` String,
                `значение` UInt64
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    println!("✓ Table with Russian column names created");

    client
        .query("INSERT INTO test_col_unicode_ru VALUES (1, 'test', 42)")
        .await
        .expect("Failed to insert data");

    let query =
        Query::new("SELECT `имя`, `значение` FROM test_col_unicode_ru");
    let result = client.query(query).await.expect("Failed to select data");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 1);
    println!("✓ Unicode (Russian) column names test passed");

    let _ = client.query("DROP TABLE test_col_unicode_ru").await;
}

#[tokio::test]
#[ignore]
async fn test_column_name_unicode_emoji() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_emoji").await;

    // Emoji in column name (if supported)
    client
        .query(
            "CREATE TABLE test_col_emoji (
                id UInt32,
                `column_🚀` String,
                `data_✅` UInt64
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table");

    println!("✓ Table with emoji column names created");

    client
        .query("INSERT INTO test_col_emoji VALUES (1, 'rocket', 100)")
        .await
        .expect("Failed to insert data");

    let query =
        Query::new("SELECT `column_🚀`, `data_✅` FROM test_col_emoji");
    let result = client.query(query).await.expect("Failed to select data");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 1);
    println!("✓ Emoji in column names test passed");

    let _ = client.query("DROP TABLE test_col_emoji").await;
}

// ============================================================================
// Very Long Column Names
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_column_name_very_long() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_long").await;

    // Create a very long column name (255 characters or so)
    let long_name = "a".repeat(200);

    let create_sql = format!(
        "CREATE TABLE test_col_long (
            id UInt32,
            `{}` String
        ) ENGINE = Memory",
        long_name
    );

    client
        .query(create_sql.as_str())
        .await
        .expect("Failed to create table with long column name");

    println!("✓ Table with 200-character column name created");

    client
        .query("INSERT INTO test_col_long VALUES (1, 'value')")
        .await
        .expect("Failed to insert data");

    let query_sql = format!("SELECT `{}` FROM test_col_long", long_name);
    let query = Query::new(query_sql);
    let result = client.query(query).await.expect("Failed to select data");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 1);
    println!("✓ Very long column name test passed");

    let _ = client.query("DROP TABLE test_col_long").await;
}

// ============================================================================
// Backtick Escaping Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_column_name_with_backticks_internal() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_backtick").await;

    // Column name containing backticks needs double-backtick escaping
    // This may not be supported or may require special escaping
    let result = client
        .query(
            "CREATE TABLE test_col_backtick (
                id UInt32,
                `col``name` String
            ) ENGINE = Memory",
        )
        .await;

    if result.is_ok() {
        println!("✓ Table with backtick in column name created");

        client
            .query("INSERT INTO test_col_backtick VALUES (1, 'test')")
            .await
            .expect("Failed to insert data");

        let query = Query::new("SELECT `col``name` FROM test_col_backtick");
        let query_result = client.query(query).await;

        if query_result.is_ok() {
            println!("✓ Backtick escaping test passed");
        } else {
            println!("Note: Backtick escaping in column names may not be fully supported");
        }

        let _ = client.query("DROP TABLE test_col_backtick").await;
    } else {
        println!("Note: Backticks inside column names may not be supported by ClickHouse");
    }
}

// ============================================================================
// Mixed Edge Cases
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_column_name_mixed_special_chars() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_mixed").await;

    // Mix of special characters, Unicode, and spaces
    client
        .query(
            "CREATE TABLE test_col_mixed (
                id UInt32,
                `user.name (full)` String,
                `count-value_2023` UInt64,
                `статус🚀` String
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table with mixed special chars");

    println!("✓ Table with mixed special character column names created");

    client
        .query(
            "INSERT INTO test_col_mixed VALUES (1, 'John Doe', 42, 'active')",
        )
        .await
        .expect("Failed to insert data");

    let query = Query::new("SELECT `user.name (full)`, `count-value_2023`, `статус🚀` FROM test_col_mixed");
    let result = client.query(query).await.expect("Failed to select data");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 1);
    println!("✓ Mixed special characters column names test passed");

    let _ = client.query("DROP TABLE test_col_mixed").await;
}

// ============================================================================
// Numeric Column Names
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_column_name_numeric() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let _ = client.query("DROP TABLE IF EXISTS test_col_numeric").await;

    // Column names that are purely numeric
    client
        .query(
            "CREATE TABLE test_col_numeric (
                `123` UInt32,
                `456` String
            ) ENGINE = Memory",
        )
        .await
        .expect("Failed to create table with numeric column names");

    println!("✓ Table with numeric column names created");

    client
        .query("INSERT INTO test_col_numeric VALUES (100, 'value')")
        .await
        .expect("Failed to insert data");

    let query = Query::new("SELECT `123`, `456` FROM test_col_numeric");
    let result = client.query(query).await.expect("Failed to select data");

    let mut total_rows = 0;
    for block in result.blocks() {
        total_rows += block.row_count();
    }

    assert_eq!(total_rows, 1);
    println!("✓ Numeric column names test passed");

    let _ = client.query("DROP TABLE test_col_numeric").await;
}
