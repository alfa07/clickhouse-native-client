/// Integration tests for LowCardinality(Int64) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        column_value::ColumnValue,
        ColumnLowCardinality,
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
async fn test_lowcardinality_int64_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("lowcardinality_int64_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value LowCardinality(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let lc_type = Type::lowcardinality(Type::int64());
    let mut lc_col = ColumnLowCardinality::new(lc_type);

    // Add some values with repetition
    lc_col
        .append_unsafe(&ColumnValue::from_i64(100))
        .expect("Failed to append");
    lc_col
        .append_unsafe(&ColumnValue::from_i64(200))
        .expect("Failed to append");
    lc_col
        .append_unsafe(&ColumnValue::from_i64(100))
        .expect("Failed to append"); // Repeated
    lc_col
        .append_unsafe(&ColumnValue::from_i64(300))
        .expect("Failed to append");
    lc_col
        .append_unsafe(&ColumnValue::from_i64(200))
        .expect("Failed to append"); // Repeated

    block
        .append_column("value", Arc::new(lc_col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT value FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 5);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnLowCardinality>()
        .expect("Invalid column type");

    // Dictionary should have only 3 unique values
    assert_eq!(result_col.dictionary_size(), 3);
    assert_eq!(result_col.len(), 5);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_lowcardinality_int64_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("lowcardinality_int64_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value LowCardinality(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases: Vec<(&str, Vec<i64>)> = vec![
        ("Single unique", vec![42, 42, 42]),
        ("All different", vec![1, 2, 3, 4]),
        ("Min/Max values", vec![i64::MIN, i64::MAX, i64::MIN]),
        ("Zero values", vec![0, 0, 0]),
        ("Mixed", vec![-100, 0, 100, -100]),
    ];

    for (idx, (_desc, values)) in test_cases.iter().enumerate() {
        let mut block = Block::new();

        let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
            Type::uint32(),
        );
        id_col.append(idx as u32);

        let lc_type = Type::lowcardinality(Type::int64());
        let mut lc_col = ColumnLowCardinality::new(lc_type);

        for value in values {
            lc_col
                .append_unsafe(&ColumnValue::from_i64(*value))
                .expect("Failed to append");
        }

        block
            .append_column("id", Arc::new(id_col))
            .expect("Failed to append id column");
        block
            .append_column("value", Arc::new(lc_col))
            .expect("Failed to append value column");

        client
            .insert(&format!("{}.test_table", db_name), block)
            .await
            .expect("Failed to insert block");
    }

    let result = client
        .query(format!("SELECT value FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), test_cases.len());

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_lowcardinality_int64_block_insert_high_cardinality() {
    let (mut client, db_name) =
        create_isolated_test_client("lowcardinality_int64_block_high_card")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value LowCardinality(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let lc_type = Type::lowcardinality(Type::int64());
    let mut lc_col = ColumnLowCardinality::new(lc_type);

    // Create many entries with few unique values
    let values = vec![10i64, 20, 30, 40, 50];
    for i in 0..100 {
        let value = values[i % values.len()];
        lc_col
            .append_unsafe(&ColumnValue::from_i64(value))
            .expect("Failed to append");
    }

    block
        .append_column("value", Arc::new(lc_col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT value FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 100);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnLowCardinality>()
        .expect("Invalid column type");

    // Dictionary should have only 5 unique values despite 100 rows
    assert_eq!(result_col.dictionary_size(), 5);

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5))]

    #[test]
    #[ignore]
    fn test_lowcardinality_int64_block_insert_random(
        values in prop::collection::vec(
            prop::sample::select(vec![10i64, 20, 30, 40, 50]),
            10..50
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) =
                create_isolated_test_client("lowcardinality_int64_block_random")
                    .await
                    .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value LowCardinality(Int64)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col =
                clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
            let lc_type = Type::lowcardinality(Type::int64());
            let mut lc_col = ColumnLowCardinality::new(lc_type);

            for (idx, value) in values.iter().enumerate() {
                id_col.append(idx as u32);
                lc_col
                    .append_unsafe(&ColumnValue::from_i64(*value))
                    .expect("Failed to append");
            }

            block
                .append_column("id", Arc::new(id_col))
                .expect("Failed to append id column");
            block
                .append_column("value", Arc::new(lc_col))
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
                .downcast_ref::<ColumnLowCardinality>()
                .expect("Invalid column type");

            // Dictionary should have at most 5 unique values
            assert!(result_col.dictionary_size() <= 5);

            cleanup_test_database(&db_name).await;
        });
    }
}
