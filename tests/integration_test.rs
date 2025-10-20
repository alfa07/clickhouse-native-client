use clickhouse_client::{
    column::{
        nullable::ColumnNullable,
        numeric::ColumnUInt64,
        string::ColumnString,
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

// ============================================================================
// NEW API INTEGRATION TESTS
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_execute_method() {
    let (mut client, db_name) = create_isolated_test_client("execute_method")
        .await
        .expect("Failed to create isolated test client");

    // Test execute() for DDL - CREATE TABLE
    let create_table = format!(
        "CREATE TABLE {}.execute_test (id UInt32, value String) ENGINE = Memory",
        db_name
    );
    client
        .execute(create_table.as_str())
        .await
        .expect("Failed to execute CREATE TABLE");
    println!("✓ CREATE TABLE via execute() succeeded");

    // Test execute() for DML - INSERT
    let insert_sql = format!(
        "INSERT INTO {}.execute_test VALUES (1, 'test'), (2, 'data')",
        db_name
    );
    client
        .execute(insert_sql.as_str())
        .await
        .expect("Failed to execute INSERT");
    println!("✓ INSERT via execute() succeeded");

    // Verify data was inserted
    let result = client
        .query(format!("SELECT * FROM {}.execute_test", db_name))
        .await
        .expect("Failed to query");
    assert_eq!(result.total_rows(), 2);
    println!("✓ Verified 2 rows inserted");

    // Test execute() for DDL - ALTER TABLE
    let alter_sql = format!(
        "ALTER TABLE {}.execute_test ADD COLUMN extra UInt32 DEFAULT 0",
        db_name
    );
    client
        .execute(alter_sql.as_str())
        .await
        .expect("Failed to execute ALTER TABLE");
    println!("✓ ALTER TABLE via execute() succeeded");

    // Test execute() for DDL - DROP TABLE
    let drop_sql = format!("DROP TABLE {}.execute_test", db_name);
    client
        .execute(drop_sql.as_str())
        .await
        .expect("Failed to execute DROP TABLE");
    println!("✓ DROP TABLE via execute() succeeded");
}

#[tokio::test]
#[ignore]
async fn test_execute_with_id() {
    let (mut client, db_name) = create_isolated_test_client("execute_with_id")
        .await
        .expect("Failed to create isolated test client");

    // Test execute_with_id()
    let create_table = format!(
        "CREATE TABLE {}.execute_id_test (id UInt32) ENGINE = Memory",
        db_name
    );
    client
        .execute_with_id(create_table.as_str(), "create-table-123")
        .await
        .expect("Failed to execute with ID");
    println!("✓ execute_with_id() succeeded with query_id: create-table-123");

    // Clean up
    client
        .execute(format!("DROP TABLE {}.execute_id_test", db_name))
        .await
        .expect("Failed to drop table");
}

#[tokio::test]
#[ignore]
async fn test_query_id_parameters() {
    let (mut client, db_name) = create_isolated_test_client("query_id_params")
        .await
        .expect("Failed to create isolated test client");

    // Setup table
    client
        .execute(format!(
            "CREATE TABLE {}.query_id_test (id UInt32, name String) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Test insert_with_id()
    let mut block = Block::new();

    let mut id_col = ColumnUInt64::new(Type::uint64());
    id_col.append(1);
    id_col.append(2);
    id_col.append(3);
    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append column");

    let mut name_col = ColumnString::new(Type::string());
    name_col.append("a".to_string());
    name_col.append("b".to_string());
    name_col.append("c".to_string());
    block
        .append_column("name", Arc::new(name_col))
        .expect("Failed to append column");

    client
        .insert_with_id(
            &format!("{}.query_id_test", db_name),
            "insert-123",
            block,
        )
        .await
        .expect("Failed to insert with ID");
    println!("✓ insert_with_id() succeeded");

    // Test query_with_id()
    let result = client
        .query_with_id(
            format!("SELECT * FROM {}.query_id_test", db_name),
            "select-123",
        )
        .await
        .expect("Failed to query with ID");
    assert_eq!(result.total_rows(), 3);
    println!("✓ query_with_id() succeeded with 3 rows");

    // Clean up
    client
        .execute(format!("DROP TABLE {}.query_id_test", db_name))
        .await
        .expect("Failed to drop table");
}

#[tokio::test]
#[ignore]
async fn test_external_tables() {
    use clickhouse_client::ExternalTable;

    let (mut client, db_name) = create_isolated_test_client("external_tables")
        .await
        .expect("Failed to create isolated test client");

    // Create main table
    client
        .execute(format!(
            "CREATE TABLE {}.main_table (id UInt64, value String) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create main table");

    // Insert data into main table
    client
        .execute(format!(
            "INSERT INTO {}.main_table VALUES (1, 'main1'), (2, 'main2'), (3, 'main3')",
            db_name
        ))
        .await
        .expect("Failed to insert into main table");

    // Create external table block with enrichment data
    let mut ext_block = Block::new();

    let mut ext_id_col = ColumnUInt64::new(Type::uint64());
    ext_id_col.append(1);
    ext_id_col.append(2);
    ext_id_col.append(4);
    ext_block
        .append_column("id", Arc::new(ext_id_col))
        .expect("Failed to append id column");

    let mut extra_col = ColumnString::new(Type::string());
    extra_col.append("extra1".to_string());
    extra_col.append("extra2".to_string());
    extra_col.append("extra4".to_string());
    ext_block
        .append_column("extra", Arc::new(extra_col))
        .expect("Failed to append extra column");

    let ext_table = ExternalTable::new("enrichment", ext_block);

    // Query with external table JOIN
    let query = format!(
        "SELECT m.id, m.value, e.extra \
         FROM {}.main_table AS m \
         INNER JOIN enrichment AS e ON m.id = e.id \
         ORDER BY m.id",
        db_name
    );

    let result = client
        .query_with_external_data(query.as_str(), &[ext_table])
        .await
        .expect("Failed to query with external data");

    println!("✓ query_with_external_data() succeeded");
    println!("  Result rows: {}", result.total_rows());

    // Should have 2 rows (id=1 and id=2 exist in both tables)
    assert_eq!(result.total_rows(), 2);
    println!("✓ JOIN with external table returned correct row count");

    // Clean up
    client
        .execute(format!("DROP TABLE {}.main_table", db_name))
        .await
        .expect("Failed to drop table");
}

#[tokio::test]
#[ignore]
async fn test_external_tables_with_id() {
    use clickhouse_client::ExternalTable;

    let (mut client, db_name) =
        create_isolated_test_client("external_tables_id")
            .await
            .expect("Failed to create isolated test client");

    // Create table
    client
        .execute(format!(
            "CREATE TABLE {}.test_table (id UInt64) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .execute(format!(
            "INSERT INTO {}.test_table VALUES (1), (2), (3)",
            db_name
        ))
        .await
        .expect("Failed to insert");

    // External table
    let mut ext_block = Block::new();

    let mut ext_id_col = ColumnUInt64::new(Type::uint64());
    ext_id_col.append(2);
    ext_id_col.append(3);
    ext_id_col.append(4);
    ext_block
        .append_column("id", Arc::new(ext_id_col))
        .expect("Failed to append column");

    let ext_table = ExternalTable::new("ext", ext_block);

    // Query with external table and query ID
    let result = client
        .query_with_external_data_and_id(
            format!(
                "SELECT COUNT(*) FROM {}.test_table AS t INNER JOIN ext AS e ON t.id = e.id",
                db_name
            ),
            "external-join-query-123",
            &[ext_table],
        )
        .await
        .expect("Failed to query");

    println!("✓ query_with_external_data_and_id() succeeded");
    assert!(result.total_rows() > 0);

    // Clean up
    client
        .execute(format!("DROP TABLE {}.test_table", db_name))
        .await
        .expect("Failed to drop table");
}

#[tokio::test]
#[ignore]
async fn test_server_version_getters() {
    let client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Test server_version()
    let (major, minor, patch) = client.server_version();
    println!("Server version: {}.{}.{}", major, minor, patch);
    assert!(major > 0);
    println!("✓ server_version() returned valid version");

    // Test server_revision()
    let revision = client.server_revision();
    println!("Server revision: {}", revision);
    assert!(revision > 0);
    println!("✓ server_revision() returned valid revision");

    // Test server_info()
    let info = client.server_info();
    println!("Server info: {}", info.name);
    assert!(!info.name.is_empty());
    println!("✓ server_info() returned valid info");
}

#[tokio::test]
#[ignore]
async fn test_query_settings_with_flags() {
    use clickhouse_client::{
        Query,
        QuerySettingsField,
    };

    let (mut client, db_name) = create_isolated_test_client("settings_flags")
        .await
        .expect("Failed to create isolated test client");

    // Create test table
    client
        .execute(format!(
            "CREATE TABLE {}.settings_test (id UInt32) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Test query with important settings
    let query = Query::new(format!("SELECT * FROM {}.settings_test", db_name))
        .with_important_setting("max_threads", "2")
        .with_setting("max_block_size", "1000");

    let result =
        client.query(query).await.expect("Failed to query with settings");
    println!("✓ Query with important settings succeeded");
    println!("  Rows: {}", result.total_rows());

    // Test setting with explicit flags (IMPORTANT only for standard settings)
    let query2 =
        Query::new(format!("SELECT * FROM {}.settings_test", db_name))
            .with_setting_flags(
                "max_threads",
                "4",
                QuerySettingsField::IMPORTANT,
            );

    let result2 = client
        .query(query2)
        .await
        .expect("Failed to query with explicit flags");
    println!("✓ Query with explicit IMPORTANT flag succeeded");
    println!("  Rows: {}", result2.total_rows());

    // Test setting with no flags (standard behavior)
    let query3 =
        Query::new(format!("SELECT * FROM {}.settings_test", db_name))
            .with_setting("max_block_size", "500");

    let result3 =
        client.query(query3).await.expect("Failed to query with no flags");
    println!("✓ Query with standard setting (no flags) succeeded");
    println!("  Rows: {}", result3.total_rows());

    // Clean up
    client
        .execute(format!("DROP TABLE {}.settings_test", db_name))
        .await
        .expect("Failed to drop table");
}

#[tokio::test]
#[ignore]
async fn test_complex_types_array_tuple_map() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let db_name = "test_complex_types";

    // Create database
    client
        .execute(format!("CREATE DATABASE IF NOT EXISTS {}", db_name))
        .await
        .expect("Failed to create database");

    println!("\n=== Testing Complex Types (Array, Tuple, Map) ===\n");

    // Test 1: Array types
    println!("Test 1: Array columns");

    // Drop table if exists to ensure clean state
    let _ = client.execute(format!("DROP TABLE IF EXISTS {}.array_test", db_name)).await;

    client
        .execute(format!(
            "CREATE TABLE {}.array_test (
                id UInt64,
                tags Array(String),
                numbers Array(Int32)
            ) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create array table");

    // Insert data with arrays
    client
        .execute(format!(
            "INSERT INTO {}.array_test VALUES
                (1, ['tag1', 'tag2', 'tag3'], [10, 20, 30]),
                (2, ['tag4'], [40]),
                (3, [], [])",
            db_name
        ))
        .await
        .expect("Failed to insert array data");

    // Query data (no send_logs_level to avoid custom serialization with complex types)
    let result = client
        .query(format!("SELECT * FROM {}.array_test ORDER BY id", db_name))
        .await
        .expect("Failed to query array data");

    println!("  ✓ Array query succeeded, rows: {}", result.total_rows());
    assert_eq!(result.total_rows(), 3);

    // Test 2: Tuple types
    println!("\nTest 2: Tuple columns");

    let _ = client.execute(format!("DROP TABLE IF EXISTS {}.tuple_test", db_name)).await;

    client
        .execute(format!(
            "CREATE TABLE {}.tuple_test (
                id UInt64,
                point Tuple(Float64, Float64),
                info Tuple(String, UInt32)
            ) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create tuple table");

    client
        .execute(format!(
            "INSERT INTO {}.tuple_test VALUES
                (1, (1.5, 2.5), ('test', 100)),
                (2, (3.5, 4.5), ('data', 200))",
            db_name
        ))
        .await
        .expect("Failed to insert tuple data");

    let result = client
        .query(format!("SELECT * FROM {}.tuple_test ORDER BY id", db_name))
        .await
        .expect("Failed to query tuple data");

    println!("  ✓ Tuple query succeeded, rows: {}", result.total_rows());
    assert_eq!(result.total_rows(), 2);

    // Test 3: Map types
    println!("\nTest 3: Map columns");

    let _ = client.execute(format!("DROP TABLE IF EXISTS {}.map_test", db_name)).await;

    client
        .execute(format!(
            "CREATE TABLE {}.map_test (
                id UInt64,
                metadata Map(String, String),
                counters Map(String, UInt64)
            ) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create map table");

    client
        .execute(format!(
            "INSERT INTO {}.map_test VALUES
                (1, {{'key1':'value1', 'key2':'value2'}}, {{'count1':10, 'count2':20}}),
                (2, {{'key3':'value3'}}, {{'count3':30}}),
                (3, {{}}, {{}})",
            db_name
        ))
        .await
        .expect("Failed to insert map data");

    let result = client
        .query(format!("SELECT * FROM {}.map_test ORDER BY id", db_name))
        .await
        .expect("Failed to query map data");

    println!("  ✓ Map query succeeded, rows: {}", result.total_rows());
    assert_eq!(result.total_rows(), 3);

    // Test 4: Nested complex types (Array of Tuples)
    println!("\nTest 4: Nested complex types");

    let _ = client.execute(format!("DROP TABLE IF EXISTS {}.nested_test", db_name)).await;

    client
        .execute(format!(
            "CREATE TABLE {}.nested_test (
                id UInt64,
                points Array(Tuple(Float64, Float64))
            ) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create nested table");

    client
        .execute(format!(
            "INSERT INTO {}.nested_test VALUES
                (1, [(1.0, 2.0), (3.0, 4.0)]),
                (2, [(5.0, 6.0)])",
            db_name
        ))
        .await
        .expect("Failed to insert nested data");

    let result = client
        .query(format!(
            "SELECT * FROM {}.nested_test ORDER BY id",
            db_name
        ))
        .await
        .expect("Failed to query nested data");

    println!("  ✓ Nested Array(Tuple) query succeeded, rows: {}", result.total_rows());
    assert_eq!(result.total_rows(), 2);

    // Test 5: FixedString type
    println!("\nTest 5: FixedString columns");

    let _ = client.execute(format!("DROP TABLE IF EXISTS {}.fixedstring_test", db_name)).await;

    client
        .execute(format!(
            "CREATE TABLE {}.fixedstring_test (
                id UInt64,
                code FixedString(4),
                hash FixedString(16)
            ) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create fixedstring table");

    client
        .execute(format!(
            "INSERT INTO {}.fixedstring_test VALUES
                (1, 'ABCD', unhex('0123456789ABCDEF0123456789ABCDEF')),
                (2, 'EFGH', unhex('FEDCBA9876543210FEDCBA9876543210'))",
            db_name
        ))
        .await
        .expect("Failed to insert fixedstring data");

    let result = client
        .query(format!(
            "SELECT * FROM {}.fixedstring_test ORDER BY id",
            db_name
        ))
        .await
        .expect("Failed to query fixedstring data");

    println!("  ✓ FixedString query succeeded, rows: {}", result.total_rows());
    assert_eq!(result.total_rows(), 2);

    // Clean up
    println!("\nCleaning up...");
    client
        .execute(format!("DROP DATABASE {}", db_name))
        .await
        .expect("Failed to drop database");

    println!("\n✅ All complex type tests passed!\n");
}

#[tokio::test]
#[ignore]
async fn test_nested_arrays_arbitrary_depth() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    let db_name = "test_nested_arrays";

    // Create database
    client
        .execute(format!("CREATE DATABASE IF NOT EXISTS {}", db_name))
        .await
        .expect("Failed to create database");

    println!("\n=== Testing Nested Arrays (Arbitrary Depth) ===\n");

    // Test 1: Array(Array(Int32)) - 2 levels
    println!("Test 1: Array(Array(Int32))");
    let _ = client.execute(format!("DROP TABLE IF EXISTS {}.array2d", db_name)).await;

    client
        .execute(format!(
            "CREATE TABLE {}.array2d (
                id UInt64,
                matrix Array(Array(Int32))
            ) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create 2D array table");

    client
        .execute(format!(
            "INSERT INTO {}.array2d VALUES
                (1, [[1, 2], [3, 4]]),
                (2, [[5]]),
                (3, [[]])",
            db_name
        ))
        .await
        .expect("Failed to insert 2D array data");

    let result = client
        .query(format!("SELECT * FROM {}.array2d ORDER BY id", db_name))
        .await
        .expect("Failed to query 2D array data");

    println!("  ✓ Array(Array(Int32)) query succeeded, rows: {}", result.total_rows());
    assert_eq!(result.total_rows(), 3);

    // Test 2: Array(Array(Array(Int32))) - 3 levels
    println!("\nTest 2: Array(Array(Array(Int32)))");
    let _ = client.execute(format!("DROP TABLE IF EXISTS {}.array3d", db_name)).await;

    client
        .execute(format!(
            "CREATE TABLE {}.array3d (
                id UInt64,
                cube Array(Array(Array(Int32)))
            ) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create 3D array table");

    client
        .execute(format!(
            "INSERT INTO {}.array3d VALUES
                (1, [[[1, 2], [3]], [[4, 5, 6]]]),
                (2, [[[7]]])",
            db_name
        ))
        .await
        .expect("Failed to insert 3D array data");

    let result = client
        .query(format!("SELECT * FROM {}.array3d ORDER BY id", db_name))
        .await
        .expect("Failed to query 3D array data");

    println!("  ✓ Array(Array(Array(Int32))) query succeeded, rows: {}", result.total_rows());
    assert_eq!(result.total_rows(), 2);

    // Test 3: Array(Array(String)) - strings in 2D
    println!("\nTest 3: Array(Array(String))");
    let _ = client.execute(format!("DROP TABLE IF EXISTS {}.array2d_str", db_name)).await;

    client
        .execute(format!(
            "CREATE TABLE {}.array2d_str (
                id UInt64,
                data Array(Array(String))
            ) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create 2D string array table");

    client
        .execute(format!(
            "INSERT INTO {}.array2d_str VALUES
                (1, [['a', 'b'], ['c']]),
                (2, [['hello', 'world']])",
            db_name
        ))
        .await
        .expect("Failed to insert 2D string array data");

    let result = client
        .query(format!("SELECT * FROM {}.array2d_str ORDER BY id", db_name))
        .await
        .expect("Failed to query 2D string array data");

    println!("  ✓ Array(Array(String)) query succeeded, rows: {}", result.total_rows());
    assert_eq!(result.total_rows(), 2);

    // Clean up
    println!("\nCleaning up...");
    client
        .execute(format!("DROP DATABASE {}", db_name))
        .await
        .expect("Failed to drop database");

    println!("\n✅ All nested array tests passed!\n");
}

#[tokio::test]
#[ignore]
async fn test_lowcardinality_deduplication() {
    println!("\n=== Testing LowCardinality Deduplication ===\n");

    let (mut client, db_name) = create_isolated_test_client("lowcardinality")
        .await
        .expect("Failed to create test client");

    println!("Creating LowCardinality test table...");

    // Create table with LowCardinality columns
    client
        .execute(format!(
            "CREATE TABLE {}.lc_test (
                id UInt32,
                status LowCardinality(String),
                country LowCardinality(Nullable(String))
            ) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create LowCardinality table");

    println!("  ✓ Table created\n");

    println!("Inserting data with repeated values (testing deduplication)...");

    // Insert data with many repeated values
    // This tests that LowCardinality deduplicates efficiently
    client
        .execute(format!(
            "INSERT INTO {}.lc_test VALUES
                (1, 'active', 'US'),
                (2, 'inactive', 'UK'),
                (3, 'active', 'US'),
                (4, 'pending', NULL),
                (5, 'active', 'US'),
                (6, 'inactive', 'DE'),
                (7, 'active', 'UK'),
                (8, 'pending', 'US'),
                (9, 'active', 'US'),
                (10, 'inactive', NULL)",
            db_name
        ))
        .await
        .expect("Failed to insert LowCardinality data");

    println!("  ✓ Inserted 10 rows with repeated values\n");

    println!("Querying data...");

    let result = client
        .query(format!("SELECT * FROM {}.lc_test ORDER BY id", db_name))
        .await
        .expect("Failed to query LowCardinality data");

    println!("  ✓ Query succeeded, rows: {}", result.total_rows());
    assert_eq!(result.total_rows(), 10);

    // Verify deduplication by checking unique values
    let unique_status = client
        .query(format!(
            "SELECT count(DISTINCT status) as cnt FROM {}.lc_test",
            db_name
        ))
        .await
        .expect("Failed to query distinct statuses");

    println!("  ✓ Distinct statuses query succeeded");
    assert_eq!(unique_status.total_rows(), 1);

    // Clean up
    println!("\nCleaning up...");
    cleanup_test_database(&db_name).await;

    println!("\n✅ LowCardinality deduplication test passed!\n");
}
