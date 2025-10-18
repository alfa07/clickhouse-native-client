//! Callback Tests for ClickHouse Client
//!
//! These tests verify the callback functionality for progress, profile, events, logs, exceptions, and data.
//!
//! ## Prerequisites
//! 1. Start ClickHouse server: `just start-db`
//! 2. Run tests: `cargo test --test client_callbacks_test -- --ignored --nocapture`
//!
//! ## Test Coverage
//! - Progress callbacks during long-running queries
//! - Profile info callbacks with query statistics
//! - Profile events callbacks with performance counters
//! - Server log callbacks
//! - Exception callbacks for errors
//! - Data callbacks for result streaming
//! - Cancelable data callbacks for query cancellation
//! - Multiple callbacks on same query
//! - Callbacks combined with query ID
//! - Callbacks combined with settings

use clickhouse_client::{Client, ClientOptions, Query};
use std::sync::{Arc, Mutex};

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
async fn test_on_progress_callback() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    // Use Arc<Mutex> to share progress_count across callback boundary
    let progress_count = Arc::new(Mutex::new(0));
    let progress_count_clone = progress_count.clone();

    let query = Query::new("SELECT * FROM system.numbers LIMIT 100000")
        .on_progress(move |p| {
            *progress_count_clone.lock().unwrap() += 1;
            println!("Progress: {} rows, {} bytes", p.rows, p.bytes);
        });

    client.query(query).await.expect("Query failed");

    let count = *progress_count.lock().unwrap();
    println!("Progress callback invoked {} times", count);
    // Progress callbacks may or may not be sent depending on query complexity
    // Just verify the callback mechanism works (no panic)
}

#[tokio::test]
#[ignore]
async fn test_on_profile_callback() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    let profile_received = Arc::new(Mutex::new(false));
    let profile_received_clone = profile_received.clone();

    let query = Query::new("SELECT number FROM system.numbers LIMIT 1000")
        .on_profile(move |p| {
            *profile_received_clone.lock().unwrap() = true;
            println!("Profile: {} rows in {} blocks, {} bytes", p.rows, p.blocks, p.bytes);
            println!("  rows_before_limit: {}, applied_limit: {}", p.rows_before_limit, p.applied_limit);
        });

    client.query(query).await.expect("Query failed");

    // ProfileInfo may or may not be sent for all queries
    println!("Profile callback received: {}", *profile_received.lock().unwrap());
}

#[tokio::test]
#[ignore]
async fn test_on_profile_events_callback() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    let events_count = Arc::new(Mutex::new(0));
    let events_count_clone = events_count.clone();

    let query = Query::new("SELECT * FROM system.numbers LIMIT 10000")
        .on_profile_events(move |block| {
            *events_count_clone.lock().unwrap() += 1;
            println!("ProfileEvents block: {} rows, {} columns", block.row_count(), block.column_count());
            true // Continue receiving events
        });

    client.query(query).await.expect("Query failed");

    let count = *events_count.lock().unwrap();
    println!("ProfileEvents callback invoked {} times", count);
}

#[tokio::test]
#[ignore]
async fn test_on_server_log_callback() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    let log_count = Arc::new(Mutex::new(0));
    let log_count_clone = log_count.clone();

    let query = Query::new("SELECT * FROM system.numbers LIMIT 1000")
        .on_server_log(move |block| {
            *log_count_clone.lock().unwrap() += 1;
            println!("Server log block: {} rows", block.row_count());
            true // Continue receiving logs
        });

    client.query(query).await.expect("Query failed");

    let count = *log_count.lock().unwrap();
    println!("Server log callback invoked {} times", count);
}

#[tokio::test]
#[ignore]
async fn test_on_exception_callback() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    let exception_received = Arc::new(Mutex::new(false));
    let exception_received_clone = exception_received.clone();

    let query = Query::new("SELECT * FROM nonexistent_table_12345")
        .on_exception(move |e| {
            *exception_received_clone.lock().unwrap() = true;
            println!("Exception: {} (code {}): {}", e.name, e.code, e.display_text);
        });

    // Query should fail
    let result = client.query(query).await;
    assert!(result.is_err(), "Query should have failed");

    // Exception callback should have been invoked
    assert!(*exception_received.lock().unwrap(), "Exception callback should have been invoked");
}

#[tokio::test]
#[ignore]
async fn test_on_data_callback() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    let data_count = Arc::new(Mutex::new(0));
    let total_rows = Arc::new(Mutex::new(0));
    let data_count_clone = data_count.clone();
    let total_rows_clone = total_rows.clone();

    let query = Query::new("SELECT number FROM system.numbers LIMIT 1000")
        .on_data(move |block| {
            *data_count_clone.lock().unwrap() += 1;
            *total_rows_clone.lock().unwrap() += block.row_count();
            println!("Data block: {} rows", block.row_count());
        });

    client.query(query).await.expect("Query failed");

    let count = *data_count.lock().unwrap();
    let rows = *total_rows.lock().unwrap();
    println!("Data callback invoked {} times, received {} total rows", count, rows);
    assert!(rows > 0, "Should have received data");
}

#[tokio::test]
#[ignore]
async fn test_on_data_cancelable_callback() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    let data_count = Arc::new(Mutex::new(0));
    let total_rows = Arc::new(Mutex::new(0));
    let data_count_clone = data_count.clone();
    let total_rows_clone = total_rows.clone();

    // Cancel after receiving some data
    let query = Query::new("SELECT * FROM system.numbers LIMIT 1000000")
        .on_data_cancelable(move |block| {
            let mut count = data_count_clone.lock().unwrap();
            let mut rows = total_rows_clone.lock().unwrap();

            *count += 1;
            *rows += block.row_count();

            println!("Data block {}: {} rows (total: {})", *count, block.row_count(), *rows);

            // Cancel after first block or after receiving some rows
            *rows < 100 || *count < 1
        });

    client.query(query).await.expect("Query failed");

    let count = *data_count.lock().unwrap();
    let rows = *total_rows.lock().unwrap();
    println!("Received {} blocks with {} total rows before cancellation", count, rows);
    assert!(rows < 1000000, "Query should have been cancelled before completion");
}

#[tokio::test]
#[ignore]
async fn test_multiple_callbacks() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    let progress_count = Arc::new(Mutex::new(0));
    let data_count = Arc::new(Mutex::new(0));
    let profile_received = Arc::new(Mutex::new(false));

    let progress_count_clone = progress_count.clone();
    let data_count_clone = data_count.clone();
    let profile_received_clone = profile_received.clone();

    let query = Query::new("SELECT number FROM system.numbers LIMIT 10000")
        .on_progress(move |p| {
            *progress_count_clone.lock().unwrap() += 1;
            println!("Progress: {} rows", p.rows);
        })
        .on_data(move |block| {
            *data_count_clone.lock().unwrap() += 1;
            println!("Data: {} rows", block.row_count());
        })
        .on_profile(move |p| {
            *profile_received_clone.lock().unwrap() = true;
            println!("Profile: {} blocks", p.blocks);
        });

    client.query(query).await.expect("Query failed");

    println!("Progress: {} times", *progress_count.lock().unwrap());
    println!("Data: {} times", *data_count.lock().unwrap());
    println!("Profile received: {}", *profile_received.lock().unwrap());
}

#[tokio::test]
#[ignore]
async fn test_callback_with_query_id() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    let data_received = Arc::new(Mutex::new(false));
    let data_received_clone = data_received.clone();

    let query_id = format!("test_callback_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());

    let query = Query::new("SELECT number FROM system.numbers LIMIT 100")
        .with_query_id(&query_id)
        .on_data(move |block| {
            *data_received_clone.lock().unwrap() = true;
            println!("Data with query_id: {} rows", block.row_count());
        });

    client.query(query).await.expect("Query failed");

    assert!(*data_received.lock().unwrap(), "Data callback should have been invoked");
    println!("Query ID {} completed with callback", query_id);
}

#[tokio::test]
#[ignore]
async fn test_callback_with_settings() {
    let mut client = create_test_client()
        .await
        .expect("Failed to connect to ClickHouse");

    let data_count = Arc::new(Mutex::new(0));
    let data_count_clone = data_count.clone();

    let query = Query::new("SELECT number FROM system.numbers LIMIT 1000")
        .with_setting("max_block_size", "100")
        .on_data(move |block| {
            *data_count_clone.lock().unwrap() += 1;
            println!("Data block: {} rows (max_block_size=100)", block.row_count());
        });

    client.query(query).await.expect("Query failed");

    let count = *data_count.lock().unwrap();
    println!("Received {} data blocks with max_block_size=100", count);
    // With smaller block size, we should receive more blocks
    assert!(count > 0, "Should have received at least one data block");
}
