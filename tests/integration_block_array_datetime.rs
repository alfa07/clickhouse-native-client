/// Integration tests for Array(DateTime) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        array::ColumnArray,
        date::ColumnDateTime,
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
async fn test_array_datetime_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("array_datetime_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (timestamps Array(DateTime)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create nested DateTime column (DateTime is stored as Unix timestamp in
    // seconds)
    let mut nested = ColumnDateTime::new(Type::datetime(None));
    nested.append(0); // 1970-01-01 00:00:00
    nested.append(1640995200); // 2022-01-01 00:00:00
    nested.append(1672531200); // 2023-01-01 00:00:00

    // Create Array column with offsets: [2, 3] for arrays [[0, 1640995200],
    // [1672531200]]
    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_offset(2); // First array has 2 elements
    col.append_offset(3); // Second array has 1 element (total 3)

    block
        .append_column("timestamps", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT timestamps FROM {}.test_table", db_name))
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
async fn test_array_datetime_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("array_datetime_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, timestamps Array(DateTime)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = vec![
        ("Empty array", vec![]),
        ("Single element", vec![1640995200]), // 2022-01-01 00:00:00
        ("Multiple elements", vec![0, 1640995200, 1672531200]), /* Mixed timestamps */
        ("Min timestamp", vec![0]), // 1970-01-01 00:00:00
        ("Recent timestamps", vec![1672531200, 1672617600, 1672704000]), /* 2023-01-01 to 2023-01-03 */
        ("Same timestamps", vec![1640995200, 1640995200, 1640995200]),
        ("Sequential hours", vec![1640995200, 1640998800, 1641002400]), /* Hour intervals */
    ];

    for (idx, (_desc, values)) in test_cases.iter().enumerate() {
        let mut block = Block::new();

        let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
            Type::uint32(),
        );
        id_col.append(idx as u32);

        let mut nested = ColumnDateTime::new(Type::datetime(None));
        for &val in values {
            nested.append(val);
        }

        let mut array_col = ColumnArray::with_nested(Arc::new(nested));
        array_col.append_offset(values.len() as u64);

        block
            .append_column("id", Arc::new(id_col))
            .expect("Failed to append id column");
        block
            .append_column("timestamps", Arc::new(array_col))
            .expect("Failed to append timestamps column");

        client
            .insert(&format!("{}.test_table", db_name), block)
            .await
            .expect("Failed to insert block");
    }

    let result = client
        .query(format!(
            "SELECT timestamps FROM {}.test_table ORDER BY id",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), test_cases.len());

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_array_datetime_block_insert_many_elements() {
    let (mut client, db_name) =
        create_isolated_test_client("array_datetime_block_many")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (timestamps Array(DateTime)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create a large array with 1000 elements (hourly timestamps for ~41 days)
    let mut nested = ColumnDateTime::new(Type::datetime(None));
    let start_timestamp = 1640995200u32; // 2022-01-01 00:00:00
    for i in 0..1000 {
        nested.append(start_timestamp + i * 3600); // Add one hour each time
    }

    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_offset(1000);

    block
        .append_column("timestamps", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT timestamps FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 1);

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_array_datetime_block_insert_random(
        arrays in prop::collection::vec(
            prop::collection::vec(0u32..2000000000u32, 0..20),  // Valid DateTime range
            1..10
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("array_datetime_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, timestamps Array(DateTime)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
                Type::uint32()
            );
            let mut nested = ColumnDateTime::new(Type::datetime(None));

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
                .append_column("timestamps", Arc::new(array_col))
                .expect("Failed to append timestamps column");

            client
                .insert(&format!("{}.test_table", db_name), block)
                .await
                .expect("Failed to insert block");

            let result = client
                .query(format!(
                    "SELECT timestamps FROM {}.test_table ORDER BY id",
                    db_name
                ))
                .await
                .expect("Failed to select");

            assert_eq!(result.total_rows(), arrays.len());

            cleanup_test_database(&db_name).await;
        });
    }
}
