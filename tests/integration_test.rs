use clickhouse_client::{Block, Client, ClientOptions, Query};
use std::env;
use std::sync::Arc;

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
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Test ping
    client.ping().await.expect("Ping failed");

    // Check server info
    let server_info = client.server_info();
    println!("Connected to ClickHouse: {}", server_info.name);
    println!(
        "Version: {}.{}.{}",
        server_info.version_major, server_info.version_minor, server_info.version_patch
    );
    println!("Revision: {}", server_info.revision);
    println!("Timezone: {}", server_info.timezone);

    assert!(!server_info.name.is_empty());
}

#[tokio::test]
#[ignore]
async fn test_create_database() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

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
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Drop table if exists
    let _ = client
        .query("DROP TABLE IF EXISTS test_db.test_table")
        .await;

    // Create table with String, UInt64, Float64 columns
    let create_table_sql = r#"
        CREATE TABLE test_db.test_table (
            name String,
            count UInt64,
            price Float64
        ) ENGINE = MergeTree()
        ORDER BY count
    "#;

    client
        .query(create_table_sql)
        .await
        .expect("Failed to create table");

    println!("Table created successfully");

    // Verify table exists
    let result = client
        .query("SHOW TABLES FROM test_db")
        .await
        .expect("Failed to show tables");

    println!("Tables in test_db: {} blocks", result.blocks().len());
}

#[tokio::test]
#[ignore]
async fn test_insert_and_select_data() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Setup: Create table
    let _ = client
        .query("DROP TABLE IF EXISTS test_db.test_table")
        .await;

    let create_table_sql = r#"
        CREATE TABLE test_db.test_table (
            name String,
            count UInt64,
            price Float64
        ) ENGINE = MergeTree()
        ORDER BY count
    "#;

    client
        .query(create_table_sql)
        .await
        .expect("Failed to create table");

    // Insert data using SQL INSERT
    let insert_sql = r#"
        INSERT INTO test_db.test_table (name, count, price) VALUES
        ('apple', 10, 1.50),
        ('banana', 25, 0.75),
        ('orange', 15, 2.00),
        ('grape', 30, 3.25),
        ('mango', 5, 2.50)
    "#;

    client
        .query(insert_sql)
        .await
        .expect("Failed to insert data");

    println!("Inserted 5 rows");

    // Select all data
    let result = client
        .query("SELECT name, count, price FROM test_db.test_table ORDER BY count")
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
}

#[tokio::test]
#[ignore]
async fn test_select_with_where() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Setup: Create and populate table
    setup_test_table(&mut client).await;

    // Select with WHERE clause
    let result = client
        .query("SELECT name, count, price FROM test_db.test_table WHERE count > 10 ORDER BY count")
        .await
        .expect("Failed to select with WHERE");

    println!("SELECT WHERE returned {} rows", result.total_rows());
    assert!(
        result.total_rows() >= 3,
        "Should have at least 3 rows with count > 10"
    );
}

#[tokio::test]
#[ignore]
async fn test_aggregation_queries() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Setup: Create and populate table
    setup_test_table(&mut client).await;

    // COUNT query
    let count_result = client
        .query("SELECT COUNT(*) as total FROM test_db.test_table")
        .await
        .expect("Failed to count rows");

    println!("COUNT result: {} blocks", count_result.blocks().len());
    assert_eq!(count_result.total_rows(), 1, "COUNT should return 1 row");

    // SUM and AVG query
    let agg_result = client
        .query("SELECT SUM(count) as total_count, AVG(price) as avg_price FROM test_db.test_table")
        .await
        .expect("Failed to aggregate");

    println!("Aggregation result: {} rows", agg_result.total_rows());
    assert_eq!(
        agg_result.total_rows(),
        1,
        "Aggregation should return 1 row"
    );
}

#[tokio::test]
#[ignore]
async fn test_insert_block() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Setup: Create table
    let _ = client
        .query("DROP TABLE IF EXISTS test_db.test_block_insert")
        .await;

    let create_table_sql = r#"
        CREATE TABLE test_db.test_block_insert (
            name String,
            value UInt64
        ) ENGINE = MergeTree()
        ORDER BY value
    "#;

    client
        .query(create_table_sql)
        .await
        .expect("Failed to create table");

    // Create a block with data
    use clickhouse_client::column::numeric::ColumnUInt64;
    use clickhouse_client::column::string::ColumnString;
    use clickhouse_client::types::Type;

    let mut block = Block::new();

    // Add String column
    let mut name_col = ColumnString::new();
    name_col.append("test1");
    name_col.append("test2");
    name_col.append("test3");
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
    client
        .insert("test_db.test_block_insert", block)
        .await
        .expect("Failed to insert block");

    println!("Block inserted successfully");

    // Verify the data
    let result = client
        .query("SELECT COUNT(*) FROM test_db.test_block_insert")
        .await
        .expect("Failed to count rows");

    println!("Inserted rows verified: {} blocks", result.blocks().len());
}

#[tokio::test]
#[ignore]
async fn test_cleanup() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Drop test tables and database
    let _ = client
        .query("DROP TABLE IF EXISTS test_db.test_table")
        .await;
    let _ = client
        .query("DROP TABLE IF EXISTS test_db.test_block_insert")
        .await;
    let _ = client.query("DROP DATABASE IF EXISTS test_db").await;

    println!("Cleanup completed");
}

// Helper function to setup test table with data
async fn setup_test_table(client: &mut Client) {
    let _ = client
        .query("DROP TABLE IF EXISTS test_db.test_table")
        .await;

    let create_table_sql = r#"
        CREATE TABLE test_db.test_table (
            name String,
            count UInt64,
            price Float64
        ) ENGINE = MergeTree()
        ORDER BY count
    "#;

    client
        .query(create_table_sql)
        .await
        .expect("Failed to create table");

    let insert_sql = r#"
        INSERT INTO test_db.test_table (name, count, price) VALUES
        ('apple', 10, 1.50),
        ('banana', 25, 0.75),
        ('orange', 15, 2.00),
        ('grape', 30, 3.25),
        ('mango', 5, 2.50)
    "#;

    client
        .query(insert_sql)
        .await
        .expect("Failed to insert data");
}
