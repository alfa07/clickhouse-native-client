/// Integration tests for Array(Int64) column using Block insertion
mod common;

use clickhouse_native_client::{
    column::{
        array::ColumnArray,
        numeric::ColumnInt64,
    },
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
async fn test_array_int64_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("array_int64_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create nested Int64 column
    let mut nested = ColumnInt64::new();
    nested.append(-9223372036854775808);
    nested.append(0);
    nested.append(9223372036854775807);

    // Create Array column with offsets: [1, 3] for arrays
    // [[-9223372036854775808], [0, 9223372036854775807]]
    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_len(1); // First array has 1 element
    col.append_len(2); // Second array has 2 elements

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

    let blocks = result.blocks();
    let col_ref = blocks[0].column(0).expect("Column not found");
    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnArray>()
        .expect("Invalid column type");

    assert_eq!(result_col.len(), 2);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_array_int64_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("array_int64_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, values Array(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = [
        ("Empty array", vec![]),
        ("Single element", vec![1000000]),
        ("Multiple elements", vec![1, 2, 3, 4, 5]),
        ("Min value", vec![-9223372036854775808]),
        ("Max value", vec![9223372036854775807]),
        ("Min and max", vec![-9223372036854775808, 9223372036854775807]),
        ("Large negative", vec![-1000000000000, -2000000000000]),
        ("Large positive", vec![1000000000000, 2000000000000]),
        ("Mixed signs", vec![-1000, 0, 1000]),
    ];

    let mut block = Block::new();

    let mut id_col = clickhouse_native_client::column::numeric::ColumnUInt32::new();
    let mut nested = ColumnInt64::new();

    for (idx, (_desc, values)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);

        for &val in values {
            nested.append(val);
        }
    }

    let mut array_col = ColumnArray::with_nested(Arc::new(nested));
    for (_desc, values) in &test_cases {
        array_col.append_len(values.len() as u64);
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
async fn test_array_int64_block_insert_many_elements() {
    let (mut client, db_name) =
        create_isolated_test_client("array_int64_block_many")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create a large array with 1000 elements
    let mut nested = ColumnInt64::new();
    for i in 0..1000 {
        nested.append((i - 500) * 1000000); // Large values
    }

    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_len(1000);

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
    fn test_array_int64_block_insert_random(
        arrays in prop::collection::vec(
            prop::collection::vec(any::<i64>(), 0..20),
            1..10
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("array_int64_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, values Array(Int64)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_native_client::column::numeric::ColumnUInt32::new();
            let mut nested = ColumnInt64::new();

            for (idx, array) in arrays.iter().enumerate() {
                id_col.append(idx as u32);
                for &val in array {
                    nested.append(val);
                }
            }

            let mut array_col = ColumnArray::with_nested(Arc::new(nested));
            for array in &arrays {
                array_col.append_len(array.len() as u64);
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
