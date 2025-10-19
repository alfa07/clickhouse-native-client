use clickhouse_client::{
    connection::Connection,
    protocol::ClientCode,
};

#[tokio::test]
#[ignore]
async fn debug_connection() {
    println!("Attempting to connect to ClickHouse...");

    let mut conn = Connection::connect("localhost", 9000)
        .await
        .expect("Failed to create connection");

    println!("TCP connection established");

    // Try to send Hello manually
    println!("Sending Hello packet...");

    // Write client hello code
    conn.write_varint(ClientCode::Hello as u64).await.unwrap();
    println!("Sent hello code");

    // Write client name
    conn.write_string("clickhouse-rust").await.unwrap();
    println!("Sent client name");

    // Write versions
    conn.write_varint(1).await.unwrap(); // major
    conn.write_varint(0).await.unwrap(); // minor
    conn.write_varint(54449).await.unwrap(); // revision
    println!("Sent version info");

    // Write database, user, password
    conn.write_string("default").await.unwrap();
    conn.write_string("default").await.unwrap();
    conn.write_string("").await.unwrap();
    println!("Sent credentials");

    conn.flush().await.unwrap();
    println!("Flushed");

    // Try to read response
    println!("Reading response...");
    let packet_type = conn.read_varint().await.unwrap();
    println!("Received packet type: {}", packet_type);
}
