/// Integration tests for Enum16 column using Block insertion
mod common;

use clickhouse_client::{
    column::enum_column::ColumnEnum16,
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

fn create_enum16_type() -> Type {
    Type::enum16(vec![
        EnumItem { name: "small".to_string(), value: 100 },
        EnumItem { name: "medium".to_string(), value: 1000 },
        EnumItem { name: "large".to_string(), value: 10000 },
    ])
}

#[tokio::test]
#[ignore]
async fn test_enum16_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("enum16_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Enum16('small' = 100, 'medium' = 1000, 'large' = 10000)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnEnum16::new(create_enum16_type());
    col.append_value(100); // small
    col.append_value(1000); // medium
    col.append_value(10000); // large
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
        .downcast_ref::<ColumnEnum16>()
        .expect("Invalid column type");

    let expected = vec![100, 1000, 10000];
    for (idx, exp) in expected.iter().enumerate() {
        assert_eq!(result_col.at(idx), *exp);
    }

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_enum16_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("enum16_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Enum16('min' = -32768, 'zero' = 0, 'max' = 32767)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let enum_type = Type::enum16(vec![
        EnumItem { name: "min".to_string(), value: -32768 },
        EnumItem { name: "zero".to_string(), value: 0 },
        EnumItem { name: "max".to_string(), value: 32767 },
    ]);

    let test_cases =
        vec![("Min value", -32768), ("Zero value", 0), ("Max value", 32767)];

    let mut block = Block::new();
    let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new();
    let mut val_col = ColumnEnum16::new(enum_type.clone());

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
        .downcast_ref::<ColumnEnum16>()
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
    fn test_enum16_block_insert_random(values in prop::collection::vec(prop::sample::select(vec![100i16, 1000i16, 10000i16]), 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("enum16_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Enum16('small' = 100, 'medium' = 1000, 'large' = 10000)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
                Type::uint32()
            );
            let mut val_col = ColumnEnum16::new(create_enum16_type());

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
                .downcast_ref::<ColumnEnum16>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {
                assert_eq!(result_col.at(idx), *expected);
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
