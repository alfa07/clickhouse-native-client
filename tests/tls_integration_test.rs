//! TLS Integration Tests for ClickHouse Client
//!
//! These tests verify TLS functionality with a real ClickHouse server.
//!
//! ## Prerequisites
//! 1. Generate certificates: `just generate-certs`
//! 2. Start TLS server: `just start-db-tls`
//! 3. Run tests: `cargo test --features tls --test tls_integration_test --
//!    --ignored --nocapture`
//!
//! ## Test Coverage
//! - Basic TLS connection with CA certificate
//! - Connection with system certificates (disabled)
//! - SNI (Server Name Indication) support
//! - Query execution over TLS
//! - Data insertion over TLS
//! - Ping over TLS connection
//! - Multiple endpoints with TLS failover
//! - Connection timeout behavior
//! - Mutual TLS (client certificate authentication)

#[cfg(feature = "tls")]
mod tls_tests {
    use clickhouse_client::{
        Client,
        ClientOptions,
        SSLOptions,
    };
    use std::{
        env,
        path::PathBuf,
    };

    /// Get ClickHouse TLS host from environment or default to localhost
    fn get_tls_host() -> String {
        env::var("CLICKHOUSE_TLS_HOST")
            .unwrap_or_else(|_| "localhost".to_string())
    }

    /// Get TLS port from environment or default to 9440
    fn get_tls_port() -> u16 {
        env::var("CLICKHOUSE_TLS_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(9440)
    }

    /// Create a TLS client with custom CA certificate
    async fn create_tls_client() -> Result<Client, Box<dyn std::error::Error>>
    {
        let host = get_tls_host();
        let port = get_tls_port();

        let ssl_opts = SSLOptions::new()
            .add_ca_cert(PathBuf::from("certs/ca/ca-cert.pem"))
            .use_system_certs(false)
            .use_sni(true);

        let opts = ClientOptions::new(host, port)
            .database("default")
            .user("default")
            .password("")
            .ssl_options(ssl_opts);

        Ok(Client::connect(opts).await?)
    }

    /// Create a TLS client without SNI
    async fn create_tls_client_no_sni(
    ) -> Result<Client, Box<dyn std::error::Error>> {
        let host = get_tls_host();
        let port = get_tls_port();

        let ssl_opts = SSLOptions::new()
            .add_ca_cert(PathBuf::from("certs/ca/ca-cert.pem"))
            .use_system_certs(false)
            .use_sni(false);

        let opts = ClientOptions::new(host, port)
            .database("default")
            .user("default")
            .password("")
            .ssl_options(ssl_opts);

        Ok(Client::connect(opts).await?)
    }

    /// Create a TLS client with client certificate (mutual TLS)
    async fn create_tls_client_mutual(
    ) -> Result<Client, Box<dyn std::error::Error>> {
        let host = get_tls_host();
        let port = get_tls_port();

        let ssl_opts = SSLOptions::new()
            .add_ca_cert(PathBuf::from("certs/ca/ca-cert.pem"))
            .use_system_certs(false)
            .use_sni(true)
            .client_cert(
                PathBuf::from("certs/client/client-cert.pem"),
                PathBuf::from("certs/client/client-key.pem"),
            );

        let opts = ClientOptions::new(host, port)
            .database("default")
            .user("default")
            .password("")
            .ssl_options(ssl_opts);

        Ok(Client::connect(opts).await?)
    }

    // ============================================================================
    // Test Cases
    // ============================================================================

    #[tokio::test]
    #[ignore] // Requires TLS-enabled ClickHouse server
    async fn test_tls_connection_basic() {
        let mut client = create_tls_client()
            .await
            .expect("Failed to connect to TLS ClickHouse");

        // Verify connection by pinging
        client.ping().await.expect("Ping failed over TLS");

        // Check server info
        let server_info = client.server_info();
        println!("Connected to ClickHouse via TLS: {}", server_info.name);
        println!(
            "Version: {}.{}.{}",
            server_info.version_major,
            server_info.version_minor,
            server_info.version_patch
        );
        println!("Revision: {}", server_info.revision);

        assert!(!server_info.name.is_empty());
    }

    #[tokio::test]
    #[ignore]
    async fn test_tls_connection_with_sni() {
        let mut client =
            create_tls_client().await.expect("Failed to connect with SNI");

        client.ping().await.expect("Ping failed with SNI");

        println!("✓ TLS connection with SNI successful");
    }

    #[tokio::test]
    #[ignore]
    async fn test_tls_connection_without_sni() {
        let mut client = create_tls_client_no_sni()
            .await
            .expect("Failed to connect without SNI");

        client.ping().await.expect("Ping failed without SNI");

        println!("✓ TLS connection without SNI successful");
    }

    #[tokio::test]
    #[ignore]
    async fn test_tls_query_execution() {
        let mut client = create_tls_client()
            .await
            .expect("Failed to connect for query test");

        // Execute a simple query over TLS
        let result = client
            .query("SELECT number, number * 2 FROM system.numbers LIMIT 10")
            .await
            .expect("Query failed over TLS");

        println!("Query returned {} rows over TLS", result.total_rows());
        assert_eq!(result.total_rows(), 10);

        println!("✓ Query execution over TLS successful");
    }

    #[tokio::test]
    #[ignore]
    async fn test_tls_create_table_and_insert() {
        let mut client = create_tls_client()
            .await
            .expect("Failed to connect for insert test");

        // Ensure clean state by dropping table first
        let _ = client.query("DROP TABLE IF EXISTS test_tls").await;

        // Create test table
        client
            .query("CREATE TABLE test_tls (id UInt64, name String) ENGINE = Memory")
            .await
            .expect("Failed to create table over TLS");

        println!("✓ Table created over TLS");

        // Insert data using SQL
        client
            .query("INSERT INTO test_tls VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
            .await
            .expect("Failed to insert data over TLS");

        println!("✓ Data inserted over TLS");

        // Query back the data
        let result = client
            .query("SELECT * FROM test_tls ORDER BY id")
            .await
            .expect("Failed to query data over TLS");

        println!("Retrieved {} rows over TLS", result.total_rows());
        assert_eq!(result.total_rows(), 3);

        // Cleanup
        client
            .query("DROP TABLE IF EXISTS test_tls")
            .await
            .expect("Failed to drop table over TLS");

        println!("✓ Table dropped over TLS");
    }

    #[tokio::test]
    #[ignore]
    async fn test_tls_ping() {
        let mut client = create_tls_client()
            .await
            .expect("Failed to connect for ping test");

        // Test ping multiple times
        for i in 1..=5 {
            client.ping().await.expect(&format!("Ping {} failed over TLS", i));
            println!("✓ Ping {} successful over TLS", i);
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_tls_multiple_queries() {
        let mut client = create_tls_client()
            .await
            .expect("Failed to connect for multiple query test");

        // Execute multiple queries in sequence
        let queries = vec![
            "SELECT 1",
            "SELECT 'Hello' AS message",
            "SELECT number FROM system.numbers LIMIT 5",
            "SELECT name FROM system.databases LIMIT 3",
        ];

        for (i, query_str) in queries.iter().enumerate() {
            let result = client
                .query(*query_str)
                .await
                .expect(&format!("Query {} failed over TLS", i + 1));

            println!(
                "✓ Query {} returned {} rows over TLS",
                i + 1,
                result.total_rows()
            );
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_tls_with_multiple_endpoints() {
        use std::time::Duration;

        let host = get_tls_host();
        let port = get_tls_port();

        let ssl_opts = SSLOptions::new()
            .add_ca_cert(PathBuf::from("certs/ca/ca-cert.pem"))
            .use_system_certs(false)
            .use_sni(true);

        // Configure with multiple endpoints (one valid, others invalid)
        let opts = ClientOptions::new(host.clone(), port)
            .database("default")
            .user("default")
            .password("")
            .add_endpoint("invalid-host-1.example.com", 9440)
            .add_endpoint(&host, port) // Valid endpoint
            .add_endpoint("invalid-host-2.example.com", 9440)
            .send_retries(2)
            .retry_timeout(Duration::from_secs(1))
            .ssl_options(ssl_opts);

        // Should eventually connect to the valid endpoint
        let mut client = Client::connect(opts)
            .await
            .expect("Failed to connect with multiple endpoints");

        client.ping().await.expect("Ping failed with multiple endpoints");

        println!("✓ Connected with multiple endpoints (failover worked)");
    }

    #[tokio::test]
    #[ignore]
    async fn test_tls_connection_timeout() {
        use clickhouse_client::ConnectionOptions;
        use std::time::Duration;

        let ssl_opts = SSLOptions::new()
            .add_ca_cert(PathBuf::from("certs/ca/ca-cert.pem"))
            .use_system_certs(false)
            .use_sni(true);

        let conn_opts = ConnectionOptions::new()
            .connect_timeout(Duration::from_millis(100));

        // Try to connect to a non-existent host with short timeout
        let opts = ClientOptions::new("invalid-host.example.com", 9440)
            .database("default")
            .user("default")
            .password("")
            .connection_options(conn_opts)
            .ssl_options(ssl_opts);

        let start = std::time::Instant::now();
        let result = Client::connect(opts).await;
        let elapsed = start.elapsed();

        // Should fail quickly due to timeout
        assert!(result.is_err(), "Connection should have failed");
        assert!(
            elapsed < Duration::from_secs(2),
            "Timeout should have triggered quickly"
        );

        println!("✓ Connection timeout worked correctly ({:?})", elapsed);
    }

    #[tokio::test]
    #[ignore]
    async fn test_tls_mutual_auth() {
        // This test requires the server to be configured for mutual TLS
        // In relaxed mode, the server accepts connections with or without
        // client cert

        let mut client = create_tls_client_mutual()
            .await
            .expect("Failed to connect with client certificate");

        client.ping().await.expect("Ping failed with client certificate");

        println!("✓ Mutual TLS (client certificate) connection successful");
    }

    #[tokio::test]
    #[ignore]
    async fn test_tls_aggregation_queries() {
        let mut client = create_tls_client()
            .await
            .expect("Failed to connect for aggregation test");

        // Create and populate test table
        client
            .query("CREATE TABLE IF NOT EXISTS test_tls_agg (id UInt64, value Float64) ENGINE = Memory")
            .await
            .expect("Failed to create table");

        client
            .query("INSERT INTO test_tls_agg VALUES (1, 10.5), (2, 20.3), (3, 15.7), (4, 30.2)")
            .await
            .expect("Failed to insert data");

        // Run aggregation queries
        let count_result = client
            .query("SELECT COUNT(*) FROM test_tls_agg")
            .await
            .expect("COUNT query failed");

        println!("✓ COUNT query returned {} rows", count_result.total_rows());

        let sum_result = client
            .query("SELECT SUM(value) FROM test_tls_agg")
            .await
            .expect("SUM query failed");

        println!("✓ SUM query returned {} rows", sum_result.total_rows());

        let avg_result = client
            .query("SELECT AVG(value) FROM test_tls_agg")
            .await
            .expect("AVG query failed");

        println!("✓ AVG query returned {} rows", avg_result.total_rows());

        // Cleanup
        client
            .query("DROP TABLE IF EXISTS test_tls_agg")
            .await
            .expect("Failed to drop table");

        println!("✓ Aggregation queries over TLS successful");
    }
}

// If TLS feature is not enabled, show a helpful message
#[cfg(not(feature = "tls"))]
#[test]
fn tls_tests_require_feature() {
    println!("\n==========================================================");
    println!("TLS tests are disabled!");
    println!("To run TLS tests, use: cargo test --features tls --test tls_integration_test -- --ignored");
    println!("==========================================================\n");
}
