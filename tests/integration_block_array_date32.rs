/// Integration tests for Array(Date32) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        array::ColumnArray,
        date::ColumnDate32,
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
async fn test_array_date32_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("array_date32_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (dates Array(Date32)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create nested Date32 column (Date32 is stored as days since 1900-01-01)
    let mut nested = ColumnDate32::new(Type::date32());
    nested.append(0); // 1900-01-01
    nested.append(25567); // 1970-01-01
    nested.append(44562); // 2022-01-01

    // Create Array column with offsets: [2, 3] for arrays [[0, 25567],
    // [44562]]
    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_offset(2); // First array has 2 elements
    col.append_offset(3); // Second array has 1 element (total 3)

    block
        .append_column("dates", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT dates FROM {}.test_table", db_name))
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
async fn test_array_date32_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("array_date32_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, dates Array(Date32)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = vec![
        ("Empty array", vec![]),
        ("Single element", vec![44562]), // 2022-01-01
        ("Multiple elements", vec![0, 25567, 44562]), /* 1900-01-01,
                                                       * 1970-01-01,
                                                       * 2022-01-01 */
        ("Min date", vec![0]), // 1900-01-01
        ("Recent dates", vec![44927, 44928, 44929]), /* 2023-01-01 to
                                                      * 2023-01-03 */
        ("Historical dates", vec![1, 365, 730]), // Early 1900s
        ("Future dates", vec![50000, 51000, 52000]),
    ];

    for (idx, (_desc, values)) in test_cases.iter().enumerate() {
        let mut block = Block::new();

        let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
            Type::uint32(),
        );
        id_col.append(idx as u32);

        let mut nested = ColumnDate32::new(Type::date32());
        for &val in values {
            nested.append(val);
        }

        let mut array_col = ColumnArray::with_nested(Arc::new(nested));
        array_col.append_offset(values.len() as u64);

        block
            .append_column("id", Arc::new(id_col))
            .expect("Failed to append id column");
        block
            .append_column("dates", Arc::new(array_col))
            .expect("Failed to append dates column");

        client
            .insert(&format!("{}.test_table", db_name), block)
            .await
            .expect("Failed to insert block");
    }

    let result = client
        .query(format!("SELECT dates FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), test_cases.len());

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_array_date32_block_insert_many_elements() {
    let (mut client, db_name) =
        create_isolated_test_client("array_date32_block_many")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (dates Array(Date32)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create a large array with 365 consecutive days starting from 1970-01-01
    let mut nested = ColumnDate32::new(Type::date32());
    let start_date = 25567; // 1970-01-01
    for i in 0..365 {
        nested.append(start_date + i);
    }

    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_offset(365);

    block
        .append_column("dates", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT dates FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 1);

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_array_date32_block_insert_random(
        arrays in prop::collection::vec(
            prop::collection::vec(0i32..60000i32, 0..20),  // Valid Date32 range
            1..10
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("array_date32_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, dates Array(Date32)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
                Type::uint32()
            );
            let mut nested = ColumnDate32::new(Type::date32());

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
                .append_column("dates", Arc::new(array_col))
                .expect("Failed to append dates column");

            client
                .insert(&format!("{}.test_table", db_name), block)
                .await
                .expect("Failed to insert block");

            let result = client
                .query(format!(
                    "SELECT dates FROM {}.test_table ORDER BY id",
                    db_name
                ))
                .await
                .expect("Failed to select");

            assert_eq!(result.total_rows(), arrays.len());

            cleanup_test_database(&db_name).await;
        });
    }
}
