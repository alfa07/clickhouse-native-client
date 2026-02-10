/// Integration tests for FixedString column using Block insertion
mod common;

use clickhouse_client::{
    column::string::ColumnFixedString,
    types::Type,
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};
use std::sync::Arc;

#[tokio::test]
#[ignore]
async fn test_fixedstring_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("fixedstring_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value FixedString(10)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnFixedString::new(Type::fixed_string(10));
    col.append("hello".to_string());
    col.append("world".to_string());
    col.append("test".to_string());
    block
        .append_column("value", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!(
            "SELECT value FROM {}.test_table ORDER BY value",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);
    let blocks = result.blocks();
    let col_ref = blocks[0].column(0).expect("Column not found");
    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnFixedString>()
        .expect("Invalid column type");

    assert_eq!(result_col.len(), 3);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_fixedstring_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("fixedstring_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value FixedString(10)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = [
        ("Empty bytes", String::from_utf8(vec![0u8; 10]).unwrap()),
        ("Partial fill", {
            let mut v = b"hello".to_vec();
            v.resize(10, 0);
            String::from_utf8(v).unwrap()
        }),
        ("Full string", {
            let mut v = b"0123456789".to_vec();
            v.resize(10, 0);
            String::from_utf8(v).unwrap()
        }),
    ];

    let mut block = Block::new();
    let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new();
    let mut val_col = ColumnFixedString::new(Type::fixed_string(10));

    for (idx, (_desc, value)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);
        val_col.append(value.clone());
    }

    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append id column");
    block
        .append_column("value", Arc::new(val_col))
        .expect("Failed to append value column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT value FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), test_cases.len());

    cleanup_test_database(&db_name).await;
}
