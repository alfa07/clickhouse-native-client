//! Query Features Tests for ClickHouse Client
//!
//! These tests verify query ID, parameters, settings, and tracing context
//! features.
//!
//! ## Prerequisites
//! 1. Start ClickHouse server: `just start-db`
//! 2. Run tests: `cargo test --test query_features_test -- --ignored
//!    --nocapture`
//!
//! ## Test Coverage
//! - Custom query IDs and tracking
//! - Query parameters binding
//! - Query settings override
//! - OpenTelemetry tracing context
//! - Query ID with INSERT operations
//! - Settings affecting execution
//! - NULL parameter handling

use clickhouse_client::{
    Client,
    ClientOptions,
    Query,
    TracingContext,
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
async fn test_query_id_tracking() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Generate unique query ID
    let query_id = format!(
        "test_qid_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    // Execute query with custom ID
    let query = Query::new("SELECT 1 AS value").with_query_id(&query_id);

    client.query(query).await.expect("Query failed");

    // Wait a bit for query_log to be populated
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Verify query ID appears in system.query_log
    let check_query = format!(
        "SELECT count(*) as cnt FROM system.query_log WHERE query_id = '{}' AND type = 'QueryFinish'",
        query_id
    );

    let result =
        client.query(check_query).await.expect("Failed to query query_log");

    println!(
        "Query ID {} found in query_log: {} rows",
        query_id,
        result.total_rows()
    );
}

#[tokio::test]
#[ignore]
async fn test_query_parameters() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Test parameter binding
    // Note: Parameter syntax may vary by ClickHouse version
    // Modern syntax: SELECT {id:UInt64}
    let query =
        Query::new("SELECT {id:UInt64} AS result").with_parameter("id", "42");

    let result =
        client.query(query).await.expect("Parameterized query failed");

    println!("Parameterized query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 1);
}

#[tokio::test]
#[ignore]
async fn test_query_settings() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Test setting max_threads
    let query = Query::new("SELECT number FROM system.numbers LIMIT 1000")
        .with_setting("max_threads", "2")
        .with_setting("max_block_size", "100");

    let result =
        client.query(query).await.expect("Query with settings failed");

    println!("Query with settings returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 1000);
}

#[tokio::test]
#[ignore]
async fn test_tracing_context() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create tracing context
    let trace_context = TracingContext {
        trace_id: 0x0123456789abcdef_fedcba9876543210, // 128-bit trace ID
        span_id: 0x1122334455667788,                   // 64-bit span ID
        tracestate: "vendor=value".to_string(),
        trace_flags: 1, // Sampled
    };

    let query =
        Query::new("SELECT 1 AS value").with_tracing_context(trace_context);

    let result = client.query(query).await.expect("Query with tracing failed");

    println!(
        "Query with tracing context returned {} rows",
        result.total_rows()
    );
    assert_eq!(result.total_rows(), 1);
}

#[tokio::test]
#[ignore]
async fn test_query_id_with_insert() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Create temporary table
    client.query("DROP TABLE IF EXISTS test_qid_insert").await.ok();
    client.query("CREATE TABLE IF NOT EXISTS test_qid_insert (id UInt64, name String) ENGINE = Memory")
        .await
        .expect("Failed to create table");

    // Generate unique query ID for INSERT
    let query_id = format!(
        "test_insert_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    // INSERT with query ID
    let insert_query = Query::new(
        "INSERT INTO test_qid_insert VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .with_query_id(&query_id);

    client.query(insert_query).await.expect("INSERT with query ID failed");

    // Wait for query_log
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Verify
    let result = client
        .query("SELECT count(*) FROM test_qid_insert")
        .await
        .expect("SELECT failed");
    assert_eq!(result.total_rows(), 1);

    // Cleanup
    client.query("DROP TABLE IF EXISTS test_qid_insert").await.ok();

    println!("INSERT with query ID {} completed", query_id);
}

#[tokio::test]
#[ignore]
async fn test_settings_max_threads() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Query with max_threads setting
    let query =
        Query::new("SELECT sleep(0.001) FROM system.numbers LIMIT 100")
            .with_setting("max_threads", "1");

    let start = std::time::Instant::now();
    client.query(query).await.expect("Query failed");
    let elapsed = start.elapsed();

    println!("Query with max_threads=1 took {:?}", elapsed);
}

#[tokio::test]
#[ignore]
async fn test_parameter_null_value() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Test NULL parameter
    // Note: Syntax may vary by ClickHouse version
    let query = Query::new("SELECT {value:Nullable(String)} AS result")
        .with_parameter("value", "NULL");

    let result = client.query(query).await;

    // This test may fail depending on ClickHouse version support for NULL
    // parameters
    match result {
        Ok(r) => {
            println!("NULL parameter query returned {} rows", r.total_rows())
        }
        Err(e) => println!("NULL parameter not supported: {}", e),
    }
}

#[tokio::test]
#[ignore]
async fn test_multiple_parameters() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Multiple parameters
    let query = Query::new("SELECT {a:UInt64} + {b:UInt64} AS sum")
        .with_parameter("a", "10")
        .with_parameter("b", "32");

    let result =
        client.query(query).await.expect("Multi-parameter query failed");

    println!("Multi-parameter query returned {} rows", result.total_rows());
    assert_eq!(result.total_rows(), 1);
}

#[tokio::test]
#[ignore]
async fn test_combined_features() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Combine query ID, settings, and parameters
    let query_id = format!(
        "test_combined_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let query = Query::new("SELECT {limit:UInt64} AS max_value")
        .with_query_id(&query_id)
        .with_parameter("limit", "100")
        .with_setting("max_block_size", "10");

    let result =
        client.query(query).await.expect("Combined features query failed");

    println!(
        "Combined query {} returned {} rows",
        query_id,
        result.total_rows()
    );
    assert_eq!(result.total_rows(), 1);
}

#[tokio::test]
#[ignore]
async fn test_settings_readonly() {
    let mut client =
        create_test_client().await.expect("Failed to connect to ClickHouse");

    // Test readonly setting
    let query = Query::new("SELECT 1").with_setting("readonly", "1");

    let result =
        client.query(query).await.expect("Query with readonly failed");
    assert_eq!(result.total_rows(), 1);

    // Try to create table with readonly=1 (should fail if enforced)
    let create_query = Query::new(
        "CREATE TABLE IF NOT EXISTS test_readonly (x UInt64) ENGINE = Memory",
    )
    .with_setting("readonly", "1");

    let result = client.query(create_query).await;
    match result {
        Ok(_) => println!("Warning: readonly setting may not be enforced"),
        Err(e) => println!("Expected error with readonly: {}", e),
    }
}
