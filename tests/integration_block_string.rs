/// Integration tests for String column using Block insertion
mod common;

use clickhouse_native_client::{
    column::string::ColumnString,
    types::Type,
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};
use proptest::prelude::*;
use std::sync::Arc;

#[tokio::test]
#[ignore]
async fn test_string_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("string_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value String) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnString::new(Type::string());
    col.append("hello");
    col.append("world");
    col.append("");
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
        .downcast_ref::<ColumnString>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), "");
    assert_eq!(result_col.at(1), "hello");
    assert_eq!(result_col.at(2), "world");

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_string_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("string_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value String) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let long_string = "x".repeat(1000);
    let test_cases = [
        ("Empty string", ""),
        ("Single char", "a"),
        ("Unicode", "Hello 世界 🌍"),
        ("Long string", long_string.as_str()),
        ("Special chars", "\n\t\"'"),
    ];

    let mut block = Block::new();
    let mut id_col = clickhouse_native_client::column::numeric::ColumnUInt32::new();
    let mut val_col = ColumnString::new(Type::string());

    for (idx, (_desc, value)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);
        val_col.append(*value);
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
    let blocks = result.blocks();
    let col_ref = blocks[0].column(0).expect("Column not found");
    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnString>()
        .expect("Invalid column type");

    for (idx, (_desc, expected)) in test_cases.iter().enumerate() {
        assert_eq!(result_col.at(idx), *expected);
    }

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_string_block_insert_random(values in prop::collection::vec(".*", 1..50)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("string_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value String) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_native_client::column::numeric::ColumnUInt32::new();
            let mut val_col = ColumnString::new(Type::string());

            for (idx, value) in values.iter().enumerate() {
                id_col.append(idx as u32);
                val_col.append(value.as_str());
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
                .query(format!(
                    "SELECT value FROM {}.test_table ORDER BY id",
                    db_name
                ))
                .await
                .expect("Failed to select");

            assert_eq!(result.total_rows(), values.len());
            let blocks = result.blocks();
            let col_ref = blocks[0].column(0).expect("Column not found");
            let result_col = col_ref
                .as_any()
                .downcast_ref::<ColumnString>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {
                assert_eq!(result_col.at(idx), expected.as_str());
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
