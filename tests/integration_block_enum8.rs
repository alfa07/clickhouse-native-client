/// Integration tests for Enum8 column using Block insertion
mod common;

use clickhouse_native_client::{
    column::enum_column::ColumnEnum8,
    types::{
        EnumItem,
        Type,
    },
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};
use proptest::prelude::*;
use std::sync::Arc;

fn create_enum8_type() -> Type {
    Type::enum8(vec![
        EnumItem { name: "red".to_string(), value: 1 },
        EnumItem { name: "green".to_string(), value: 2 },
        EnumItem { name: "blue".to_string(), value: 3 },
    ])
}

#[tokio::test]
#[ignore]
async fn test_enum8_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("enum8_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Enum8('red' = 1, 'green' = 2, 'blue' = 3)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnEnum8::new(create_enum8_type());
    col.append_value(1); // red
    col.append_value(2); // green
    col.append_value(3); // blue
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
        .downcast_ref::<ColumnEnum8>()
        .expect("Invalid column type");

    let expected = [1, 2, 3];
    for (idx, exp) in expected.iter().enumerate() {
        assert_eq!(result_col.at(idx), *exp);
    }

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_enum8_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("enum8_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Enum8('min' = -128, 'zero' = 0, 'max' = 127)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let enum_type = Type::enum8(vec![
        EnumItem { name: "min".to_string(), value: -128 },
        EnumItem { name: "zero".to_string(), value: 0 },
        EnumItem { name: "max".to_string(), value: 127 },
    ]);

    let test_cases =
        [("Min value", -128), ("Zero value", 0), ("Max value", 127)];

    let mut block = Block::new();
    let mut id_col = clickhouse_native_client::column::numeric::ColumnUInt32::new();
    let mut val_col = ColumnEnum8::new(enum_type.clone());

    for (idx, (_desc, value)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);
        val_col.append_value(*value);
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
        .downcast_ref::<ColumnEnum8>()
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
    fn test_enum8_block_insert_random(values in prop::collection::vec(prop::sample::select(vec![1i8, 2i8, 3i8]), 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("enum8_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Enum8('red' = 1, 'green' = 2, 'blue' = 3)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_native_client::column::numeric::ColumnUInt32::new();
            let mut val_col = ColumnEnum8::new(create_enum8_type());

            for (idx, value) in values.iter().enumerate() {
                id_col.append(idx as u32);
                val_col.append_value(*value);
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
                .downcast_ref::<ColumnEnum8>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {
                assert_eq!(result_col.at(idx), *expected);
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
