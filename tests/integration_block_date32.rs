/// Integration tests for Date32 column using Block insertion
mod common;

use clickhouse_client::{
    column::date::ColumnDate32,
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
async fn test_date32_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("date32_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Date32) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnDate32::new(Type::date32());
    col.append(-25567); // 1900-01-01
    col.append(0); // 1970-01-01
    col.append(19000); // 2022-01-05
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
    let block = &blocks[0];
    let column = block.column(0).expect("Column not found");
    let result_col = column
        .as_any()
        .downcast_ref::<ColumnDate32>()
        .expect("Invalid column type");

    let mut expected = vec![-25567, 0, 19000];
    expected.sort();

    for (idx, exp) in expected.iter().enumerate() {
        assert_eq!(result_col.at(idx), *exp);
    }

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_date32_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("date32_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Date32) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = [
        ("Historic date (1900-01-01)", -25567),
        ("Unix epoch (1970-01-01)", 0),
        ("Future date", 100000),
        ("Recent date", 19000),
    ];

    let mut block = Block::new();
    let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new();
    let mut val_col = ColumnDate32::new(Type::date32());

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
    let block = &blocks[0];
    let column = block.column(0).expect("Column not found");
    let result_col = column
        .as_any()
        .downcast_ref::<ColumnDate32>()
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
    fn test_date32_block_insert_random(values in prop::collection::vec(any::<i32>(), 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("date32_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Date32) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new();
            let mut val_col = ColumnDate32::new(Type::date32());

            for (idx, value) in values.iter().enumerate() {
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
                .query(format!(
                    "SELECT value FROM {}.test_table ORDER BY id",
                    db_name
                ))
                .await
                .expect("Failed to select");

            assert_eq!(result.total_rows(), values.len());
            let blocks = result.blocks();
            let block = &blocks[0];
            let column = block.column(0).expect("Column not found");
            let result_col = column
                .as_any()
                .downcast_ref::<ColumnDate32>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {
                assert_eq!(result_col.at(idx), *expected);
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
