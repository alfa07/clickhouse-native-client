/// Integration tests for Array(Int32) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        array::ColumnArray,
        numeric::ColumnInt32,
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
async fn test_array_int32_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("array_int32_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(Int32)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create nested Int32 column
    let mut nested = ColumnInt32::new(Type::int32());
    nested.append(-2147483648);
    nested.append(-1);
    nested.append(0);
    nested.append(1);
    nested.append(2147483647);

    // Create Array column with offsets: [3, 5] for arrays [[-2147483648, -1,
    // 0], [1, 2147483647]]
    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_offset(3); // First array has 3 elements
    col.append_offset(5); // Second array has 2 elements (total 5)

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
async fn test_array_int32_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("array_int32_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, values Array(Int32)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = vec![
        ("Empty array", vec![]),
        ("Single element", vec![42]),
        ("Multiple elements", vec![1, 2, 3, 4, 5]),
        ("Min value", vec![-2147483648]),
        ("Max value", vec![2147483647]),
        ("Min and max", vec![-2147483648, 2147483647]),
        ("Negative values", vec![-100, -200, -300]),
        ("Mixed signs", vec![-10, 0, 10]),
    ];

    let mut block = Block::new();

    let mut id_col =
        clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
    let mut nested = ColumnInt32::new(Type::int32());

    for (idx, (_desc, values)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);

        for &val in values {
            nested.append(val);
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
async fn test_array_int32_block_insert_many_elements() {
    let (mut client, db_name) =
        create_isolated_test_client("array_int32_block_many")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(Int32)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create a large array with 1000 elements
    let mut nested = ColumnInt32::new(Type::int32());
    for i in 0..1000 {
        nested.append(i - 500); // Range from -500 to 499
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
    fn test_array_int32_block_insert_random(
        arrays in prop::collection::vec(
            prop::collection::vec(any::<i32>(), 0..20),
            1..10
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("array_int32_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, values Array(Int32)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
                Type::uint32()
            );
            let mut nested = ColumnInt32::new(Type::int32());

            for (idx, array) in arrays.iter().enumerate() {
                id_col.append(idx as u32);
                for &val in array {
                    nested.append(val);
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
