use clickhouse_client::{
    column::{ColumnNullable, string::ColumnString},
    types::Type,
    Block,
};
use std::sync::Arc;
use bytes::BytesMut;
use clickhouse_client::column::Column;

fn main() {
    // Create nullable string column with 3 values
    let mut col = ColumnNullable::with_nested(Arc::new(ColumnString::new(Type::string())));
    
    // Append: Some("hello"), None, Some("world")
    col.append(Some("hello".to_string()));
    col.append(None);
    col.append(Some("world".to_string()));
    
    println!("Column has {} rows", col.size());
    println!("Null bitmap: {:?}", col.nulls_bitmap());
    
    // Serialize and check
    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).expect("Failed to save");
    
    println!("Serialized {} bytes", buffer.len());
    println!("First 50 bytes (hex): {:02X?}", &buffer[..buffer.len().min(50)]);
}
