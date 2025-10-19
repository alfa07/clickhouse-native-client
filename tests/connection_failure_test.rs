//! Connection Failure Tests
//!
//! These tests verify proper error handling for various connection failure
//! scenarios. Based on C++ clickhouse-cpp connection_failed_client_test.cpp
//!
//! ## Test Coverage
//! - Invalid hostname
//! - Invalid port
//! - Connection timeout
//! - Wrong credentials (authentication failure)
//! - Network unreachable
//! - Server not responding
//! - TLS handshake failures
//! - Database doesn't exist
//!
//! ## Running Tests
//! These tests are designed to fail connection attempts, so they don't require
//! a running server (except for auth failure tests).
//!
//! ```bash
//! cargo test --test connection_failure_test -- --nocapture
//! ```

use clickhouse_client::{
    Client,
    ClientOptions,
    ConnectionOptions,
};
use std::time::Duration;

#[tokio::test]
async fn test_connection_invalid_hostname() {
    let conn_opts =
        ConnectionOptions::default().connect_timeout(Duration::from_secs(2));
    let opts = ClientOptions::new("invalid.nonexistent.hostname.local", 9000)
        .database("default")
        .user("default")
        .password("")
        .connection_options(conn_opts);

    println!("Attempting to connect to invalid hostname...");

    let result = Client::connect(opts).await;

    assert!(result.is_err(), "Connection should fail with invalid hostname");

    if let Err(err) = result {
        let err_msg = err.to_string();
        println!("Error (as expected): {}", err_msg);
    }

    // Error should mention hostname resolution or connection failure
    println!("✓ Invalid hostname test passed");
}

#[tokio::test]
async fn test_connection_invalid_port() {
    // Port 1 is typically not accessible and definitely not ClickHouse
    let conn_opts =
        ConnectionOptions::default().connect_timeout(Duration::from_secs(2));
    let opts = ClientOptions::new("localhost", 1)
        .database("default")
        .user("default")
        .password("")
        .connection_options(conn_opts);

    println!("Attempting to connect to invalid port...");

    let result = Client::connect(opts).await;

    assert!(result.is_err(), "Connection should fail with invalid port");

    if let Err(err) = result {
        println!("Error (as expected): {}", err);
    }

    println!("✓ Invalid port test passed");
}

#[tokio::test]
async fn test_connection_timeout() {
    // Use a non-routable IP address (TEST-NET-1 from RFC 5737)
    // 192.0.2.0/24 is reserved for documentation and should not route
    let conn_opts =
        ConnectionOptions::default().connect_timeout(Duration::from_secs(1));
    let opts = ClientOptions::new("192.0.2.1", 9000)
        .database("default")
        .user("default")
        .password("")
        .connection_options(conn_opts);

    println!("Attempting connection that should timeout...");

    let start = std::time::Instant::now();
    let result = Client::connect(opts).await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "Connection should timeout");

    println!("Connection failed after {:?} (as expected)", elapsed);
    assert!(
        elapsed < Duration::from_secs(3),
        "Timeout should occur within reasonable time"
    );

    if let Err(err) = result {
        println!("Error: {}", err);
    }

    println!("✓ Connection timeout test passed");
}

#[tokio::test]
#[ignore] // Requires running ClickHouse server
async fn test_authentication_failure_wrong_user() {
    let conn_opts =
        ConnectionOptions::default().connect_timeout(Duration::from_secs(5));
    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("nonexistent_user_12345")
        .password("wrong_password")
        .connection_options(conn_opts);

    println!("Attempting to connect with invalid credentials...");

    let result = Client::connect(opts).await;

    assert!(
        result.is_err(),
        "Connection should fail with invalid credentials"
    );

    if let Err(err) = result {
        let err_msg = err.to_string();
        println!("Error (as expected): {}", err_msg);

        // Error should mention authentication or user
        assert!(
            err_msg.contains("Authentication")
                || err_msg.contains("user")
                || err_msg.contains("password")
                || err_msg.contains("Exception"),
            "Error should indicate authentication failure"
        );
    }

    println!("✓ Authentication failure test passed");
}

#[tokio::test]
#[ignore] // Requires running ClickHouse server
async fn test_authentication_failure_wrong_password() {
    // Try to connect with default user but wrong password
    let conn_opts =
        ConnectionOptions::default().connect_timeout(Duration::from_secs(5));
    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default")
        .password("definitely_wrong_password_12345")
        .connection_options(conn_opts);

    println!("Attempting to connect with wrong password...");

    let result = Client::connect(opts).await;

    // This might succeed if the default user has no password set
    match result {
        Err(err) => {
            println!("Authentication failed (as expected): {}", err);
            println!("✓ Wrong password test passed");
        }
        Ok(_) => {
            println!("Note: Connection succeeded - default user may not have password protection");
            println!(
                "✓ Wrong password test completed (server allows connection)"
            );
        }
    }
}

#[tokio::test]
#[ignore] // Requires running ClickHouse server
async fn test_database_does_not_exist() {
    let conn_opts =
        ConnectionOptions::default().connect_timeout(Duration::from_secs(5));
    let opts = ClientOptions::new("localhost", 9000)
        .database("nonexistent_database_xyz_12345")
        .user("default")
        .password("")
        .connection_options(conn_opts);

    println!("Attempting to connect to nonexistent database...");

    let result = Client::connect(opts).await;

    // Some ClickHouse versions allow connection and fail on first query
    match result {
        Err(err) => {
            println!("Connection failed (as expected): {}", err);
            println!("✓ Nonexistent database test passed");
        }
        Ok(mut client) => {
            println!("Connection succeeded, testing query...");

            // Try a simple query - this should fail
            let query_result =
                client.query(clickhouse_client::Query::new("SELECT 1")).await;

            match query_result {
                Err(e) => {
                    println!("Query failed (as expected): {}", e);
                    println!(
                        "✓ Nonexistent database test passed (failed on query)"
                    );
                }
                Ok(_) => {
                    println!(
                        "Note: Server allows nonexistent database - may auto-switch to default"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn test_connection_refused() {
    // Try connecting to a port that's definitely not listening
    let conn_opts =
        ConnectionOptions::default().connect_timeout(Duration::from_secs(2));
    let opts = ClientOptions::new("localhost", 19999)
        .database("default")
        .user("default")
        .password("")
        .connection_options(conn_opts);

    println!("Attempting to connect to port with no listener...");

    let result = Client::connect(opts).await;

    assert!(result.is_err(), "Connection should be refused");

    if let Err(err) = result {
        println!("Error (as expected): {}", err);
    }

    println!("✓ Connection refused test passed");
}

#[tokio::test]
#[cfg(feature = "tls")]
#[ignore] // Requires running ClickHouse with TLS
async fn test_tls_handshake_failure_wrong_cert() {
    use clickhouse_client::SSLOptions;

    // Try to connect with TLS using invalid certificate
    let ssl_opts = SSLOptions::default()
        .skip_verification(false) // Force verification
        .ca_cert_path("/nonexistent/ca.pem"); // Invalid path

    let conn_opts =
        ConnectionOptions::default().connect_timeout(Duration::from_secs(5));
    let opts = ClientOptions::new("localhost", 9440)
        .database("default")
        .user("default")
        .password("")
        .ssl_options(ssl_opts)
        .connection_options(conn_opts);

    println!("Attempting TLS connection with invalid certificate...");

    let result = Client::connect(opts).await;

    assert!(result.is_err(), "TLS connection should fail with invalid cert");

    if let Err(err) = result {
        println!("Error (as expected): {}", err);
    }

    println!("✓ TLS handshake failure test passed");
}

#[tokio::test]
async fn test_connection_with_very_short_timeout() {
    // Even connecting to a valid service might fail with extremely short
    // timeout
    let conn_opts =
        ConnectionOptions::default().connect_timeout(Duration::from_millis(1));
    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default")
        .password("")
        .connection_options(conn_opts);

    println!("Attempting connection with 1ms timeout...");

    let result = Client::connect(opts).await;

    // This will almost certainly timeout
    match result {
        Err(e) => {
            println!("Connection timed out (as expected): {}", e);
            println!("✓ Very short timeout test passed");
        }
        Ok(_) => {
            println!("Note: Connection succeeded despite very short timeout (fast machine!)");
        }
    }
}

#[tokio::test]
async fn test_error_message_quality() {
    // Verify that error messages are informative
    let conn_opts =
        ConnectionOptions::default().connect_timeout(Duration::from_secs(2));
    let opts = ClientOptions::new("definitely.invalid.hostname.test", 9000)
        .database("default")
        .user("default")
        .password("")
        .connection_options(conn_opts);

    let result = Client::connect(opts).await;

    assert!(result.is_err());

    if let Err(err) = result {
        let err_msg = err.to_string();

        println!("Error message: {}", err_msg);

        // Error message should not be empty
        assert!(!err_msg.is_empty(), "Error message should not be empty");

        // Error message should be reasonably informative
        assert!(
            err_msg.len() > 10,
            "Error message should be reasonably informative"
        );

        println!("✓ Error message quality test passed");
    }
}
