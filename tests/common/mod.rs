#![allow(dead_code)]
/// Common test helpers for integration tests
use clickhouse_native_client::{
    Client,
    ClientOptions,
};
use std::env;

/// Get ClickHouse host from environment or default to localhost
pub fn get_clickhouse_host() -> String {
    env::var("CLICKHOUSE_HOST").unwrap_or_else(|_| "localhost".to_string())
}

/// Create a test client connection
pub async fn create_test_client() -> Result<Client, Box<dyn std::error::Error>>
{
    let host = get_clickhouse_host();
    let opts = ClientOptions::new(host, 9000)
        .database("default")
        .user("default")
        .password("");

    Ok(Client::connect(opts).await?)
}

/// Generate unique database name for test isolation
/// Uses nanosecond timestamp to ensure uniqueness even in parallel execution
pub fn unique_database_name(test_name: &str) -> String {
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

/// Create isolated test client with unique database
pub async fn create_isolated_test_client(
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
pub async fn cleanup_test_database(db_name: &str) {
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
