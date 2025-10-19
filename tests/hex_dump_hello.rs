use clickhouse_client::io::buffer_utils;
use tokio::{
    io::{
        AsyncWriteExt,
        BufWriter,
    },
    net::TcpStream,
};

#[tokio::test]
#[ignore]
async fn hex_dump_hello_packet() {
    // Connect to ClickHouse
    let stream = TcpStream::connect("localhost:9000").await.unwrap();
    stream.set_nodelay(true).unwrap();

    let (_, write_half) = tokio::io::split(stream);
    let mut writer = BufWriter::with_capacity(8192, write_half);

    // Collect all bytes we're going to send
    let mut packet_bytes = Vec::new();

    // Using buffer_utils from crate

    // Build hello packet
    buffer_utils::write_varint_to_vec(&mut packet_bytes, 0); // ClientCode::Hello
    buffer_utils::write_varint_to_vec(&mut packet_bytes, "clickhouse-cpp".len() as u64);
    packet_bytes.extend_from_slice(b"clickhouse-cpp"); // client name
    buffer_utils::write_varint_to_vec(&mut packet_bytes, 2); // major
    buffer_utils::write_varint_to_vec(&mut packet_bytes, 6); // minor
    buffer_utils::write_varint_to_vec(&mut packet_bytes, 54459); // revision
    buffer_utils::write_varint_to_vec(&mut packet_bytes, "default".len() as u64);
    packet_bytes.extend_from_slice(b"default"); // database
    buffer_utils::write_varint_to_vec(&mut packet_bytes, "default".len() as u64);
    packet_bytes.extend_from_slice(b"default"); // user
    buffer_utils::write_varint_to_vec(&mut packet_bytes, 0); // password (empty)

    println!("\n=== CLIENT HELLO PACKET ===");
    println!("Total bytes: {}", packet_bytes.len());
    println!("\nHex dump:");
    for (i, chunk) in packet_bytes.chunks(16).enumerate() {
        print!("{:04x}:  ", i * 16);
        for byte in chunk {
            print!("{:02x} ", byte);
        }
        // Pad if less than 16 bytes
        for _ in 0..(16 - chunk.len()) {
            print!("   ");
        }
        print!(" |  ");
        for byte in chunk {
            if *byte >= 32 && *byte <= 126 {
                print!("{}", *byte as char);
            } else {
                print!(".");
            }
        }
        println!("|");
    }

    println!("\nSending packet...");
    writer.write_all(&packet_bytes).await.unwrap();
    writer.flush().await.unwrap();
    println!("Packet sent successfully!");

    // Try to read response
    println!("\nTrying to read server response...");

    drop(writer); // Drop writer to get back the stream

    // We need to recreate the stream properly
    let stream2 = TcpStream::connect("localhost:9000").await.unwrap();
    stream2.set_nodelay(true).unwrap();
    let (mut read_half, mut write_half) = tokio::io::split(stream2);

    // Send hello again
    write_half.write_all(&packet_bytes).await.unwrap();
    write_half.flush().await.unwrap();
    println!("Resent packet to new connection...");

    // Now try to read
    use tokio::io::AsyncReadExt;
    let mut response_byte = [0u8; 1];
    match tokio::time::timeout(
        tokio::time::Duration::from_secs(2),
        read_half.read_exact(&mut response_byte),
    )
    .await
    {
        Ok(Ok(_)) => {
            println!("Got first response byte: 0x{:02x}", response_byte[0]);
        }
        Ok(Err(e)) => {
            println!("Error reading response: {}", e);
        }
        Err(_) => {
            println!("Timeout waiting for response");
        }
    }
}
