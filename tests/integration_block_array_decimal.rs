/// Integration tests for Array(Decimal(10, 2)) column using Block insertion
mod common;

use clickhouse_native_client::{
    column::{
        array::ColumnArray,
        decimal::ColumnDecimal,
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
async fn test_array_decimal_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("array_decimal_block_basic")
            .await
            .expect("Failed to create test client");

    let precision = 10;
    let scale = 2;

    client
        .query(format!(
            "CREATE TABLE {}.test_table (prices Array(Decimal({}, {}))) ENGINE = Memory",
            db_name, precision, scale
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create nested Decimal column (Decimal(10, 2) stores values as integers *
    // 100)
    let mut nested = ColumnDecimal::new(Type::decimal(precision, scale));
    nested.append(12345); // Represents 123.45
    nested.append(67890); // Represents 678.90
    nested.append(0); // Represents 0.00
    nested.append(-12345); // Represents -123.45

    // Create Array column with offsets: [2, 4] for arrays [[123.45, 678.90],
    // [0.00, -123.45]]
    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_len(2); // First array has 2 elements
    col.append_len(2); // Second array has 2 elements

    block
        .append_column("prices", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT prices FROM {}.test_table", db_name))
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
async fn test_array_decimal_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("array_decimal_block_boundary")
            .await
            .expect("Failed to create test client");

    let precision = 10;
    let scale = 2;

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, prices Array(Decimal({}, {}))) ENGINE = Memory",
            db_name, precision, scale
        ))
        .await
        .expect("Failed to create table");

    let test_cases = [
        ("Empty array", vec![]),
        ("Single element", vec![12345]), // 123.45
        ("Multiple elements", vec![100, 200, 300]), // 1.00, 2.00, 3.00
        ("Zero values", vec![0, 0, 0]),
        ("Negative values", vec![-12345, -67890]), // -123.45, -678.90
        ("Mixed signs", vec![-100, 0, 100]),       // -1.00, 0.00, 1.00
        ("Max precision", vec![99999999, -99999999]), /* Near max for
                                                    * Decimal(10, 2) */
        ("Small fractions", vec![1, 10, 99]), // 0.01, 0.10, 0.99
    ];

    let mut block = Block::new();

    let mut id_col = clickhouse_native_client::column::numeric::ColumnUInt32::new();
    let mut nested = ColumnDecimal::new(Type::decimal(precision, scale));

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
        .append_column("prices", Arc::new(array_col))
        .expect("Failed to append prices column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!(
            "SELECT prices FROM {}.test_table ORDER BY id",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), test_cases.len());

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_array_decimal_block_insert_many_elements() {
    let (mut client, db_name) =
        create_isolated_test_client("array_decimal_block_many")
            .await
            .expect("Failed to create test client");

    let precision = 10;
    let scale = 2;

    client
        .query(format!(
            "CREATE TABLE {}.test_table (prices Array(Decimal({}, {}))) ENGINE = Memory",
            db_name, precision, scale
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create a large array with 1000 elements
    let mut nested = ColumnDecimal::new(Type::decimal(precision, scale));
    for i in 0..1000 {
        nested.append(i * 100); // 0.00, 1.00, 2.00, ..., 999.00
    }

    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_len(1000);

    block
        .append_column("prices", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT prices FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 1);

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_array_decimal_block_insert_random(
        arrays in prop::collection::vec(
            prop::collection::vec(-100000000i64..100000000i64, 0..20),  // Valid range for Decimal(10, 2)
            1..10
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("array_decimal_block_random")
                .await
                .expect("Failed to create test client");

            let precision = 10;
            let scale = 2;

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, prices Array(Decimal({}, {}))) ENGINE = Memory",
                    db_name, precision, scale
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_native_client::column::numeric::ColumnUInt32::new();
            let mut nested = ColumnDecimal::new(Type::decimal(precision, scale));

            for (idx, array) in arrays.iter().enumerate() {
                id_col.append(idx as u32);
                for &val in array {
                    nested.append(val as i128);
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
                .append_column("prices", Arc::new(array_col))
                .expect("Failed to append prices column");

            client
                .insert(&format!("{}.test_table", db_name), block)
                .await
                .expect("Failed to insert block");

            let result = client
                .query(format!(
                    "SELECT prices FROM {}.test_table ORDER BY id",
                    db_name
                ))
                .await
                .expect("Failed to select");

            assert_eq!(result.total_rows(), arrays.len());

            cleanup_test_database(&db_name).await;
        });
    }
}
