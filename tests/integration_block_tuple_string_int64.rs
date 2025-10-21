/// Integration tests for Tuple(String, Int64) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        numeric::ColumnInt64,
        string::ColumnString,
        ColumnTuple,
    },
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
async fn test_tuple_string_int64_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("tuple_string_int64_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Tuple(String, Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let mut col1 = ColumnString::new(Type::string());
    col1.append("hello");
    col1.append("world");
    col1.append("");

    let mut col2 = ColumnInt64::new(Type::int64());
    col2.append(100);
    col2.append(200);
    col2.append(300);

    let tuple_type =
        Type::Tuple { item_types: vec![Type::string(), Type::int64()] };
    let tuple_col =
        ColumnTuple::new(tuple_type, vec![Arc::new(col1), Arc::new(col2)]);

    block
        .append_column("value", Arc::new(tuple_col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!(
            "SELECT value FROM {}.test_table ORDER BY value.2",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnTuple>()
        .expect("Invalid column type");

    let result_col1 = result_col
        .column_at(0)
        .as_any()
        .downcast_ref::<ColumnString>()
        .expect("Invalid column type");
    let result_col2 = result_col
        .column_at(1)
        .as_any()
        .downcast_ref::<ColumnInt64>()
        .expect("Invalid column type");

    assert_eq!(result_col1.at(0), "hello");
    assert_eq!(result_col2.at(0), 100);
    assert_eq!(result_col1.at(1), "world");
    assert_eq!(result_col2.at(1), 200);
    assert_eq!(result_col1.at(2), "");
    assert_eq!(result_col2.at(2), 300);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_tuple_string_int64_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("tuple_string_int64_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Tuple(String, Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = vec![
        ("Empty string", "", 0i64),
        ("Unicode", "Hello 世界", i64::MAX),
        ("Long string", &"x".repeat(1000), i64::MIN),
        ("Special chars", "\n\t\"'", -42),
        ("Single char", "a", 1000),
    ];

    let mut block = Block::new();

    let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
        Type::uint32(),
    );
    let mut col1 = ColumnString::new(Type::string());
    let mut col2 = ColumnInt64::new(Type::int64());

    for (idx, (_desc, val1, val2)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);
        col1.append(*val1);
        col2.append(*val2);
    }

    let tuple_type =
        Type::Tuple { item_types: vec![Type::string(), Type::int64()] };
    let tuple_col =
        ColumnTuple::new(tuple_type, vec![Arc::new(col1), Arc::new(col2)]);

    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append id column");
    block
        .append_column("value", Arc::new(tuple_col))
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
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnTuple>()
        .expect("Invalid column type");

    let result_col1 = result_col
        .column_at(0)
        .as_any()
        .downcast_ref::<ColumnString>()
        .expect("Invalid column type");
    let result_col2 = result_col
        .column_at(1)
        .as_any()
        .downcast_ref::<ColumnInt64>()
        .expect("Invalid column type");

    for (idx, (_desc, expected1, expected2)) in test_cases.iter().enumerate() {
        assert_eq!(result_col1.at(idx), *expected1);
        assert_eq!(result_col2.at(idx), *expected2);
    }

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_tuple_string_int64_block_insert_random(
        values in prop::collection::vec((".*", any::<i64>()), 1..50)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) =
                create_isolated_test_client("tuple_string_int64_block_random")
                    .await
                    .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Tuple(String, Int64)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col =
                clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
            let mut col1 = ColumnString::new(Type::string());
            let mut col2 = ColumnInt64::new(Type::int64());

            for (idx, (val1, val2)) in values.iter().enumerate() {
                id_col.append(idx as u32);
                col1.append(val1.as_str());
                col2.append(*val2);
            }

            let tuple_type = Type::Tuple {
                item_types: vec![Type::string(), Type::int64()],
            };
            let tuple_col =
                ColumnTuple::new(tuple_type, vec![Arc::new(col1), Arc::new(col2)]);

            block
                .append_column("id", Arc::new(id_col))
                .expect("Failed to append id column");
            block
                .append_column("value", Arc::new(tuple_col))
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
            let result_col = result.blocks()[0]
                .column(0)
                .expect("Column not found")
                .as_any()
                .downcast_ref::<ColumnTuple>()
                .expect("Invalid column type");

            let result_col1 = result_col
                .column_at(0)
                .as_any()
                .downcast_ref::<ColumnString>()
                .expect("Invalid column type");
            let result_col2 = result_col
                .column_at(1)
                .as_any()
                .downcast_ref::<ColumnInt64>()
                .expect("Invalid column type");

            for (idx, (expected1, expected2)) in values.iter().enumerate() {
                assert_eq!(result_col1.at(idx), expected1.as_str());
                assert_eq!(result_col2.at(idx), *expected2);
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
