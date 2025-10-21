/// Integration tests for Array(String) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        array::ColumnArray,
        string::ColumnString,
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
async fn test_array_string_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("array_string_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create nested String column
    let mut nested = ColumnString::new(Type::string());
    nested.append("hello".to_string());
    nested.append("world".to_string());
    nested.append("test".to_string());

    // Create Array column with offsets: [2, 3] for arrays [["hello", "world"],
    // ["test"]]
    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_offset(2); // First array has 2 elements
    col.append_offset(3); // Second array has 1 element (total 3)

    block
        .append_column("values", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT values FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 2);

    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnArray>()
        .expect("Invalid column type");

    assert_eq!(result_col.size(), 2);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_array_string_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("array_string_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, values Array(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases: Vec<(&str, Vec<&str>)> = vec![
        ("Empty array", vec![]),
        ("Single element", vec!["single"]),
        ("Multiple elements", vec!["one", "two", "three", "four", "five"]),
        ("Empty strings", vec!["", "", ""]),
        ("Mixed empty and non-empty", vec!["hello", "", "world"]),
        ("Unicode strings", vec!["こんにちは", "世界", "🎉"]),
        (
            "Long strings",
            vec!["a".repeat(1000).as_str(), "b".repeat(500).as_str()],
        ),
        (
            "Special characters",
            vec!["tab\there", "newline\nhere", "quote\"here"],
        ),
    ];

    let mut block = Block::new();

    let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
        Type::uint32(),
    );
    let mut nested = ColumnString::new(Type::string());

    for (idx, (_desc, values)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);

        for &val in values {
            nested.append(val.to_string());
        }
    }

    let mut array_col = ColumnArray::with_nested(Arc::new(nested));
    let mut cumulative = 0u64;
    for (_desc, values) in &test_cases {
        cumulative += values.len() as u64;
        array_col.append_offset(cumulative);
    }

    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append id column");
    block
        .append_column("values", Arc::new(array_col))
        .expect("Failed to append values column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!(
            "SELECT values FROM {}.test_table ORDER BY id",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), test_cases.len());

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_array_string_block_insert_many_elements() {
    let (mut client, db_name) =
        create_isolated_test_client("array_string_block_many")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create a large array with 1000 elements
    let mut nested = ColumnString::new(Type::string());
    for i in 0..1000 {
        nested.append(format!("string_{}", i));
    }

    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_offset(1000);

    block
        .append_column("values", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT values FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 1);

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_array_string_block_insert_random(
        arrays in prop::collection::vec(
            prop::collection::vec("[a-zA-Z0-9 ]{0,20}", 0..20),
            1..10
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("array_string_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, values Array(String)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
                Type::uint32()
            );
            let mut nested = ColumnString::new(Type::string());

            for (idx, array) in arrays.iter().enumerate() {
                id_col.append(idx as u32);
                for val in array {
                    nested.append(val.clone());
                }
            }

            let mut array_col = ColumnArray::with_nested(Arc::new(nested));
            let mut cumulative = 0u64;
            for array in &arrays {
                cumulative += array.len() as u64;
                array_col.append_offset(cumulative);
            }

            block
                .append_column("id", Arc::new(id_col))
                .expect("Failed to append id column");
            block
                .append_column("values", Arc::new(array_col))
                .expect("Failed to append values column");

            client
                .insert(&format!("{}.test_table", db_name), block)
                .await
                .expect("Failed to insert block");

            let result = client
                .query(format!(
                    "SELECT values FROM {}.test_table ORDER BY id",
                    db_name
                ))
                .await
                .expect("Failed to select");

            assert_eq!(result.total_rows(), arrays.len());

            cleanup_test_database(&db_name).await;
        });
    }
}
