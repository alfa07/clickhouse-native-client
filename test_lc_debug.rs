use clickhouse_client::{
    column::{
        column_value::ColumnValue,
        string::ColumnString,
        ColumnLowCardinality,
    },
    types::Type,
    Block,
    ClientOptions,
    Client,
};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let opts = ClientOptions::new("localhost", 9000)
        .database("default")
        .user("default");
    
    let mut client = Client::connect(opts).await.expect("Failed to connect");
    
    // Create table
    client.query("CREATE DATABASE IF NOT EXISTS test_lc_debug".to_string())
        .await.expect("Failed to create DB");
    
    client.query("CREATE TABLE test_lc_debug.test_table (value LowCardinality(String)) ENGINE = Memory".to_string())
        .await.expect("Failed to create table");
    
    // Insert data
    let mut block = Block::new();
    let lc_type = Type::low_cardinality(Type::string());
    let mut lc_col = ColumnLowCardinality::new(lc_type);
    
    println!("Initial dictionary size: {}", lc_col.dictionary_size());
    
    lc_col.append_unsafe(&ColumnValue::from_string("status1")).expect("Failed to append");
    println!("After status1: dictionary size = {}", lc_col.dictionary_size());
    
    lc_col.append_unsafe(&ColumnValue::from_string("status2")).expect("Failed to append");
    println!("After status2: dictionary size = {}", lc_col.dictionary_size());
    
    lc_col.append_unsafe(&ColumnValue::from_string("status1")).expect("Failed to append");
    println!("After status1 (repeat): dictionary size = {}", lc_col.dictionary_size());
    
    lc_col.append_unsafe(&ColumnValue::from_string("status3")).expect("Failed to append");
    println!("After status3: dictionary size = {}", lc_col.dictionary_size());
    
    lc_col.append_unsafe(&ColumnValue::from_string("status2")).expect("Failed to append");
    println!("After status2 (repeat): dictionary size = {}", lc_col.dictionary_size());
    
    println!("\nBefore insert: dictionary size = {}", lc_col.dictionary_size());
    
    block.append_column("value", Arc::new(lc_col)).expect("Failed to append column");
    
    client.insert("test_lc_debug.test_table", block).await.expect("Failed to insert");
    
    // Query back
    let result = client.query("SELECT value FROM test_lc_debug.test_table".to_string())
        .await.expect("Failed to select");
    
    println!("\nTotal rows: {}", result.total_rows());
    let blocks = result.blocks();
    let col_ref = blocks[0].column(0).expect("Column not found");
    let result_col = col_ref.as_any().downcast_ref::<ColumnLowCardinality>().expect("Invalid type");
    
    println!("After query: dictionary size = {}", result_col.dictionary_size());
    println!("Column length: {}", result_col.len());
    
    // Print dictionary contents
    let dict = result_col.dictionary().as_any().downcast_ref::<ColumnString>().expect("Dict should be String");
    println!("\nDictionary contents:");
    for i in 0..dict.size() {
        println!("  [{}] = '{}'", i, dict.at(i));
    }
    
    client.query("DROP DATABASE test_lc_debug".to_string()).await.expect("Failed to drop");
}
