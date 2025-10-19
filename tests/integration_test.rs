use clickhouse_client::{
    column::{
        nullable::ColumnNullable,
        numeric::ColumnUInt64,
    },
    types::Type,
    Block,
    Client,
    ClientOptions,
};
use std::{
    env,
    sync::Arc,
};

/// Get ClickHouse host from environment or default to localhost
fn get_clickhouse_host() -> String {
    env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".to_string())
}

/// Create a test client connection
async fn create_test_client() -> Result<Client, Box<dyn std::error::Error>> {
    let host = get_clickhouse_host();
    let opts = ClientOptions::new(host, 9000)
        .database("default")
        .user("default")
        .password("");

    Ok(Client::connect(opts).await?)
}

#[tokio::test]
#[ignore] // Only run with --ignored flag when ClickHouse is running
async fn test_connection_and_ping() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Test ping
    client.ping().await.expect("Ping failed");

    // Check server info
    let server_info = client.server_info();
    println!("Connected to ClickHouse: {}", server_info.name);
    println!(
        "Version: {}.{}.{}",
        server_info.version_major,
        server_info.version_minor,
        server_info.version_patch
    );
    println!("Revision: {}", server_info.revision);
    println!("Timezone: {}", server_info.timezone);

    assert!(!server_info.name.is_empty());
}

#[tokio::test]
#[ignore]
async fn test_create_database() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create test database
    let result = client
        .query("CREATE DATABASE IF NOT EXISTS test_db")
        .await
        .expect("Failed to create database");

    println!("Created database, result blocks: {}", result.blocks().len());
}

#[tokio::test]
#[ignore]
async fn test_create_table() {
    let (mut client, db_name) = create_isolated_test_client("create_table")
        .await
        .expect("Failed to create isolated test client");

    // Create table with String, UInt64, Float64 columns
    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.test_table (
            name String,
            count UInt64,
            price Float64
        ) ENGINE = MergeTree()
        ORDER BY count
    "#,
        db_name
    );

    client
        .query(create_table_sql.as_str())
        .await
        .expect("Failed to create table");

    println!("Table created successfully");

    // Verify table exists
    let result = client
        .query(format!("SHOW TABLES FROM {}", db_name))
        .await
        .expect("Failed to show tables");

    println!("Tables in {}: {} blocks", db_name, result.blocks().len());

    // Cleanup
    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_insert_and_select_data() {
    let (mut client, db_name) = create_isolated_test_client("insert_select")
        .await
        .expect("Failed to create isolated test client");

    // Create table
    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.data_table (
            name String,
            count UInt64,
            price Float64
        ) ENGINE = MergeTree()
        ORDER BY count
    "#,
        db_name
    );

    client
        .query(create_table_sql.as_str())
        .await
        .expect("Failed to create table");

    // Insert data using SQL INSERT
    let insert_sql = format!(
        r#"
        INSERT INTO {}.data_table (name, count, price) VALUES
        ('apple', 10, 1.50),
        ('banana', 25, 0.75),
        ('orange', 15, 2.00),
        ('grape', 30, 3.25),
        ('mango', 5, 2.50)
    "#,
        db_name
    );

    client.query(insert_sql.as_str()).await.expect("Failed to insert data");

    println!("Inserted 5 rows");

    // Select all data
    let result = client
        .query(format!(
            "SELECT name, count, price FROM {}.data_table ORDER BY count",
            db_name
        ))
        .await
        .expect("Failed to select data");

    println!("Query returned {} blocks", result.blocks().len());
    println!("Total rows: {}", result.total_rows());

    assert!(result.total_rows() >= 5, "Should have at least 5 rows");

    // Print the data
    for block in result.blocks() {
        println!(
            "Block: {} columns, {} rows",
            block.column_count(),
            block.row_count()
        );

        // Get column names
        for i in 0..block.column_count() {
            if let Some(name) = block.column_name(i) {
                print!("{}\t", name);
            }
        }
        println!();
    }

    // Cleanup
    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_select_with_where() {
    let (mut client, db_name) = create_isolated_test_client("select_where")
        .await
        .expect("Failed to create isolated test client");

    // Setup: Create and populate table
    setup_test_table(&mut client, &db_name, "data_table").await;

    // Select with WHERE clause
    let result = client
        .query(format!("SELECT name, count, price FROM {}.data_table WHERE count > 10 ORDER BY count", db_name))
        .await
        .expect("Failed to select with WHERE");

    println!("SELECT WHERE returned {} rows", result.total_rows());
    assert!(
        result.total_rows() >= 3,
        "Should have at least 3 rows with count > 10"
    );

    // Cleanup
    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_aggregation_queries() {
    let (mut client, db_name) = create_isolated_test_client("aggregation")
        .await
        .expect("Failed to create isolated test client");

    // Setup: Create and populate table
    setup_test_table(&mut client, &db_name, "data_table").await;

    // COUNT query
    let count_result = client
        .query(format!("SELECT COUNT(*) as total FROM {}.data_table", db_name))
        .await
        .expect("Failed to count rows");

    println!("COUNT result: {} blocks", count_result.blocks().len());
    assert_eq!(count_result.total_rows(), 1, "COUNT should return 1 row");

    // SUM and AVG query
    let agg_result = client
        .query(format!("SELECT SUM(count) as total_count, AVG(price) as avg_price FROM {}.data_table", db_name))
        .await
        .expect("Failed to aggregate");

    println!("Aggregation result: {} rows", agg_result.total_rows());
    assert_eq!(agg_result.total_rows(), 1, "Aggregation should return 1 row");

    // Cleanup
    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_insert_block() {
    let (mut client, db_name) = create_isolated_test_client("insert_block")
        .await
        .expect("Failed to create isolated test client");

    use clickhouse_client::{
        column::{
            numeric::ColumnUInt64,
            string::ColumnString,
        },
        types::Type,
    };

    // Create table
    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.data_table (
            name String,
            value UInt64
        ) ENGINE = MergeTree()
        ORDER BY value
    "#,
        db_name
    );

    client
        .query(create_table_sql.as_str())
        .await
        .expect("Failed to create table");

    // Create a block with data
    let mut block = Block::new();

    // Add String column
    let mut name_col = ColumnString::new(Type::string());
    name_col.append("test1".to_string());
    name_col.append("test2".to_string());
    name_col.append("test3".to_string());
    block
        .append_column("name", Arc::new(name_col))
        .expect("Failed to append name column");

    // Add UInt64 column
    let mut value_col = ColumnUInt64::new(Type::uint64());
    value_col.append(100);
    value_col.append(200);
    value_col.append(300);
    block
        .append_column("value", Arc::new(value_col))
        .expect("Failed to append value column");

    println!("Created block with {} rows", block.row_count());

    // Insert the block
    let table_ref = format!("{}.data_table", db_name);
    client.insert(&table_ref, block).await.expect("Failed to insert block");

    println!("Block inserted successfully");

    // Verify the data
    let result = client
        .query(format!("SELECT COUNT(*) FROM {}.data_table", db_name))
        .await
        .expect("Failed to count rows");

    println!("Inserted rows verified: {} blocks", result.blocks().len());

    // Cleanup
    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_cleanup() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Drop test tables (but keep database for other tests)
    let _ =
        client.query("DROP TABLE IF EXISTS test_db.test_table_create").await;
    let _ = client
        .query("DROP TABLE IF EXISTS test_db.test_table_insert_select")
        .await;
    let _ =
        client.query("DROP TABLE IF EXISTS test_db.test_table_where").await;
    let _ = client.query("DROP TABLE IF EXISTS test_db.test_table_agg").await;
    let _ =
        client.query("DROP TABLE IF EXISTS test_db.test_block_insert").await;
    // Note: Not dropping database to avoid conflicts with parallel tests
    // let _ = client.query("DROP DATABASE IF EXISTS test_db").await;

    println!("Cleanup completed");
}

// ============================================================================
// Exception Handling Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_exception_handling_syntax_error() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Execute invalid SQL - should return error
    let result = client.query("SELECTTTT invalid syntax").await;

    assert!(result.is_err(), "Expected syntax error");

    if let Err(error) = result {
        let error_msg = error.to_string();
        println!("Got expected error: {}", error_msg);

        // Error should mention syntax problem
        assert!(
            error_msg.contains("Syntax error")
                || error_msg.contains("SYNTAX_ERROR")
                || error_msg.contains("Unknown expression identifier"),
            "Expected syntax error message, got: {}",
            error_msg
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_exception_handling_table_not_found() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Query non-existent table
    let result =
        client.query("SELECT * FROM nonexistent_table_xyz_12345").await;

    assert!(result.is_err(), "Expected table not found error");

    if let Err(error) = result {
        let error_msg = error.to_string();
        println!("Got expected error: {}", error_msg);

        assert!(
            error_msg.contains("does not exist")
                || error_msg.contains("doesn't exist")
                || error_msg.contains("Unknown table"),
            "Expected table not found error, got: {}",
            error_msg
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_exception_recovery() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Execute invalid query
    let _ = client.query("INVALID SQL").await;

    // Connection should still be usable after exception
    let result = client.ping().await;
    assert!(result.is_ok(), "Connection should be usable after exception");

    // Execute valid query after exception
    let result = client.query("SELECT 1").await;
    assert!(
        result.is_ok(),
        "Should be able to execute queries after exception"
    );
}

// ============================================================================
// NULL Parameter Handling Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_nullable_column_insertion() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_column_insertion")
            .await
            .expect("Failed to create isolated test client");

    // Create table with nullable column
    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.test_nullable (
            id UInt64,
            nullable_value Nullable(UInt64)
        ) ENGINE = Memory
    "#,
        db_name
    );

    client
        .query(create_table_sql.as_str())
        .await
        .expect("Failed to create table");

    // Verify table is accessible
    client
        .query(format!("SELECT count(*) FROM {}.test_nullable", db_name))
        .await
        .expect("Failed to verify table exists");

    // Insert data with NULL values using block insertion
    let mut block = Block::new();

    // Create id column
    let mut id_col = ColumnUInt64::new(Type::uint64());
    id_col.append(1);
    id_col.append(2);
    id_col.append(3);

    // Create nullable column
    let nested_col = Arc::new(ColumnUInt64::new(Type::uint64()));
    let mut nullable_col = ColumnNullable::with_nested(nested_col);

    // Append values: Some(100), None, Some(300)
    // Manually append since append_nullable is only for UInt32
    nullable_col.append_non_null();
    if let Some(nested_mut) = Arc::get_mut(nullable_col.nested_mut()) {
        if let Some(col) =
            nested_mut.as_any_mut().downcast_mut::<ColumnUInt64>()
        {
            col.append(100);
        }
    }

    nullable_col.append_null();
    if let Some(nested_mut) = Arc::get_mut(nullable_col.nested_mut()) {
        if let Some(col) =
            nested_mut.as_any_mut().downcast_mut::<ColumnUInt64>()
        {
            col.append(0); // Placeholder for NULL
        }
    }

    nullable_col.append_non_null();
    if let Some(nested_mut) = Arc::get_mut(nullable_col.nested_mut()) {
        if let Some(col) =
            nested_mut.as_any_mut().downcast_mut::<ColumnUInt64>()
        {
            col.append(300);
        }
    }

    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append id column");
    block
        .append_column("nullable_value", Arc::new(nullable_col))
        .expect("Failed to append nullable column");

    let table_ref = format!("{}.test_nullable", db_name);
    client
        .insert(&table_ref, block)
        .await
        .expect("Failed to insert nullable data");

    // Query NULL values
    let result = client
        .query(format!(
            "SELECT id FROM {}.test_nullable WHERE nullable_value IS NULL",
            db_name
        ))
        .await
        .expect("Failed to query NULL values");

    println!("NULL query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 1, "Should have 1 NULL value");

    // Query non-NULL values
    let result = client
        .query(format!(
            "SELECT id FROM {}.test_nullable WHERE nullable_value IS NOT NULL",
            db_name
        ))
        .await
        .expect("Failed to query non-NULL values");

    println!("Non-NULL query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 2, "Should have 2 non-NULL values");

    // Cleanup
    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_select_null_literal() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // SELECT NULL should work
    let result = client
        .query("SELECT NULL AS null_col")
        .await
        .expect("Failed to SELECT NULL");

    println!("SELECT NULL returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 1);

    // SELECT with NULL and non-NULL columns (simpler test)
    let result = client
        .query("SELECT 1 AS num, NULL AS null_col")
        .await
        .expect("Failed to SELECT with NULL column");

    assert_eq!(result.total_rows(), 1);
    if let Some(block) = result.blocks().first() {
        assert_eq!(block.column_count(), 2, "Should have 2 columns");
    }
}

// ============================================================================
// Large Data Transfer Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_large_block_insert() {
    let (mut client, db_name) =
        create_isolated_test_client("large_block_insert")
            .await
            .expect("Failed to create isolated test client");

    use clickhouse_client::{
        column::{
            numeric::ColumnUInt64,
            string::ColumnString,
        },
        types::Type,
    };

    // Create table
    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.test_large_block (
            id UInt64,
            text String
        ) ENGINE = Memory
    "#,
        db_name
    );

    client
        .query(create_table_sql.as_str())
        .await
        .expect("Failed to create table");

    // Verify table is accessible
    client
        .query(format!("SELECT count(*) FROM {}.test_large_block", db_name))
        .await
        .expect("Failed to verify table exists");

    // Create large block with 10,000 rows
    let mut block = Block::new();

    let mut id_col = ColumnUInt64::new(Type::uint64());
    let mut text_col = ColumnString::new(Type::string());

    for i in 0..10000 {
        id_col.append(i);
        text_col.append(format!("Row number {}", i));
    }

    block.append_column("id", Arc::new(id_col)).unwrap();
    block.append_column("text", Arc::new(text_col)).unwrap();

    println!("Inserting block with {} rows", block.row_count());

    // Insert large block
    let table_ref = format!("{}.test_large_block", db_name);
    client
        .insert(&table_ref, block)
        .await
        .expect("Failed to insert large block");

    // Verify count
    let result = client
        .query(format!("SELECT COUNT(*) FROM {}.test_large_block", db_name))
        .await
        .expect("Failed to count rows");

    println!("Large block insert completed");
    assert_eq!(result.total_rows(), 1);

    // Cleanup
    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_large_result_set() {
    let (mut client, db_name) =
        create_isolated_test_client("large_result_set")
            .await
            .expect("Failed to create isolated test client");

    use clickhouse_client::{
        column::numeric::ColumnUInt64,
        types::Type,
    };

    // Create and populate table
    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.test_large_select (
            id UInt64
        ) ENGINE = Memory
    "#,
        db_name
    );

    client
        .query(create_table_sql.as_str())
        .await
        .expect("Failed to create table");

    // Verify table is accessible
    client
        .query(format!("SELECT count(*) FROM {}.test_large_select", db_name))
        .await
        .expect("Failed to verify table exists");

    // Insert 50,000 rows
    let mut block = Block::new();
    let mut id_col = ColumnUInt64::new(Type::uint64());

    for i in 0..50000 {
        id_col.append(i);
    }

    block.append_column("id", Arc::new(id_col)).unwrap();

    let table_ref = format!("{}.test_large_select", db_name);
    client.insert(&table_ref, block).await.expect("Failed to insert data");

    // Query large result set
    println!("Querying large result set...");
    let result = client
        .query(format!("SELECT * FROM {}.test_large_select", db_name))
        .await
        .expect("Failed to select large result");

    println!(
        "Received {} total rows in {} blocks",
        result.total_rows(),
        result.blocks().len()
    );

    assert!(result.total_rows() >= 50000, "Should receive all 50,000 rows");

    // Cleanup
    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Connection Persistence Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_multiple_queries_same_connection() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Execute 100 queries on the same connection
    for i in 0..100 {
        let query = format!("SELECT {} AS value", i);
        let result = client
            .query(query.as_str())
            .await
            .unwrap_or_else(|_| panic!("Failed on query {}", i));

        assert_eq!(result.total_rows(), 1);
    }

    println!("Executed 100 queries successfully on same connection");
}

#[tokio::test]
#[ignore]
async fn test_ping_between_queries() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Ping
    client.ping().await.expect("First ping failed");

    // Query
    let _ = client.query("SELECT 1").await.expect("Query failed");

    // Ping again
    client.ping().await.expect("Second ping failed");

    // Another query
    let _ = client.query("SELECT 2").await.expect("Second query failed");

    // Final ping
    client.ping().await.expect("Final ping failed");

    println!("Ping and query interleaving works correctly");
}

// ============================================================================
// Test Isolation Helpers
// ============================================================================

/// Generate unique database name for test isolation
/// Uses nanosecond timestamp to ensure uniqueness even in parallel execution
fn unique_database_name(test_name: &str) -> String {
    use std::time::{
        SystemTime,
        UNIX_EPOCH,
    };
    let timestamp =
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    // Use sanitized test name (replace invalid chars)
    let safe_name = test_name.replace("-", "_").replace(" ", "_");
    format!("test_{}_{}", safe_name, timestamp)
}

/// Create test client with its own isolated database
/// Returns (client, database_name) tuple
/// The client is already connected to the new database
async fn create_isolated_test_client(
    test_name: &str,
) -> Result<(Client, String), Box<dyn std::error::Error>> {
    let db_name = unique_database_name(test_name);

    // Connect to default database first to create our test database
    let mut temp_client = Client::connect(
        ClientOptions::new("localhost", 9000)
            .database("default")
            .user("default")
            .password(""),
    )
    .await?;

    // Create unique database
    temp_client.query(format!("CREATE DATABASE {}", db_name)).await?;

    // Now connect directly to the new database
    let client = Client::connect(
        ClientOptions::new("localhost", 9000)
            .database(&db_name)
            .user("default")
            .password(""),
    )
    .await?;

    Ok((client, db_name))
}

/// Cleanup: drop test database after test completes
async fn cleanup_test_database(db_name: &str) {
    // Connect to default database to drop the test database
    if let Ok(mut client) = Client::connect(
        ClientOptions::new("localhost", 9000)
            .database("default")
            .user("default")
            .password(""),
    )
    .await
    {
        let _ =
            client.query(format!("DROP DATABASE IF EXISTS {}", db_name)).await;
    }
}

// Helper function to setup test table with data (for isolated database tests)
async fn setup_test_table(
    client: &mut Client,
    db_name: &str,
    table_name: &str,
) {
    println!("[SETUP] Creating table: {}.{}", db_name, table_name);

    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.{} (
            name String,
            count UInt64,
            price Float64
        ) ENGINE = MergeTree()
        ORDER BY count
    "#,
        db_name, table_name
    );

    client
        .query(create_table_sql.as_str())
        .await
        .expect("Failed to create table");
    println!("[SETUP] Table created");

    let insert_sql = format!(
        r#"
        INSERT INTO {}.{} (name, count, price) VALUES
        ('apple', 10, 1.50),
        ('banana', 25, 0.75),
        ('orange', 15, 2.00),
        ('grape', 30, 3.25),
        ('mango', 5, 2.50)
    "#,
        db_name, table_name
    );

    client.query(insert_sql.as_str()).await.expect("Failed to insert data");
    println!(
        "[SETUP] Data inserted, setup complete for: {}.{}",
        db_name, table_name
    );
}
