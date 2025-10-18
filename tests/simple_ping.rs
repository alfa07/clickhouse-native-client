use clickhouse_client::{Client, ClientOptions};
use std::time::Duration;

#[tokio::test]
#[ignore]
async fn simple_ping_with_timeout() {
    println!("Connecting to ClickHouse...");

    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default")
        .password("");

    let result = tokio::time::timeout(
        Duration::from_secs(3),
        Client::connect(opts)
    ).await;

    match result {
        Ok(Ok(mut client)) => {
            println!("Connected successfully!");
            println!("Server info: {:?}", client.server_info());

            println!("Sending ping...");
            let ping_result = tokio::time::timeout(
                Duration::from_secs(3),
                client.ping()
            ).await;

            match ping_result {
                Ok(Ok(())) => println!("✓ Ping successful!"),
                Ok(Err(e)) => println!("✗ Ping failed: {}", e),
                Err(_) => println!("✗ Ping timed out after 3s"),
            }
        },
        Ok(Err(e)) => {
            println!("✗ Connection failed: {}", e);
            panic!("Failed to connect");
        },
        Err(_) => {
            println!("✗ Connection timed out after 3s");
            panic!("Connection timeout");
        }
    }
}
