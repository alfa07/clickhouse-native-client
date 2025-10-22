/// Integration tests for Array(Float64) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        array::ColumnArray,
        numeric::ColumnFloat64,
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
async fn test_array_float64_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("array_float64_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(Float64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create nested Float64 column
    let mut nested = ColumnFloat64::new();
    nested.append(3.14159265358979);
    nested.append(2.71828182845905);
    nested.append(-1.618033988749);
    nested.append(0.0);

    // Create Array column with offsets: [2, 4] for arrays [[3.14..., 2.71...],
    // [-1.618..., 0.0]]
    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_len(2); // First array has 2 elements
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
async fn test_array_float64_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("array_float64_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, values Array(Float64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = [
        ("Empty array", vec![]),
        ("Single element", vec![3.14159265358979]),
        ("Multiple elements", vec![1.0, 2.0, 3.0, 4.0, 5.0]),
        ("Negative values", vec![-1.5, -2.5, -3.5]),
        ("Mixed signs", vec![-1.0, 0.0, 1.0]),
        ("Very small and large", vec![1.0e-100, 1.0e100, -1.0e100]),
        ("High precision", vec![0.1234567890123456, 0.9876543210987654]),
    ];

    let mut block = Block::new();

    let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new();
    let mut nested = ColumnFloat64::new();

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
async fn test_array_float64_block_insert_many_elements() {
    let (mut client, db_name) =
        create_isolated_test_client("array_float64_block_many")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(Float64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create a large array with 1000 elements
    let mut nested = ColumnFloat64::new();
    for i in 0..1000 {
        nested.append((i as f64) * 0.123456789);
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
    fn test_array_float64_block_insert_random(
        arrays in prop::collection::vec(
            prop::collection::vec(any::<f64>().prop_filter("No NaN or Inf", |v| v.is_finite()), 0..20),
            1..10
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("array_float64_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, values Array(Float64)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new();
            let mut nested = ColumnFloat64::new();

            let mut cumulative = 0u64;
            for (idx, array) in arrays.iter().enumerate() {
                id_col.append(idx as u32);
                for &val in array {
                    nested.append(val);
                }
                cumulative += array.len() as u64;
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
