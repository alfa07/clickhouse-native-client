/// Integration tests for LowCardinality(String) column using Block insertion
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
async fn test_lowcardinality_string_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("lowcardinality_string_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value LowCardinality(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let lc_type = Type::lowcardinality(Type::string());
    let mut lc_col = ColumnLowCardinality::new(lc_type);

    // Add some values with repetition (good for low cardinality)
    lc_col
        .append_unsafe(&ColumnValue::from_string("status1"))
        .expect("Failed to append");
    lc_col
        .append_unsafe(&ColumnValue::from_string("status2"))
        .expect("Failed to append");
    lc_col
        .append_unsafe(&ColumnValue::from_string("status1"))
        .expect("Failed to append"); // Repeated
    lc_col
        .append_unsafe(&ColumnValue::from_string("status3"))
        .expect("Failed to append");
    lc_col
        .append_unsafe(&ColumnValue::from_string("status2"))
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
async fn test_lowcardinality_string_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("lowcardinality_string_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value LowCardinality(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases: Vec<(&str, Vec<&str>)> = vec![
        ("Single unique", vec!["same", "same", "same"]),
        ("All different", vec!["a", "b", "c", "d"]),
        ("Empty string", vec!["", "test", ""]),
        ("Unicode", vec!["Hello", "世界", "Hello"]),
        ("Long strings", vec![&"x".repeat(100), "short", &"x".repeat(100)]),
    ];

    for (idx, (_desc, values)) in test_cases.iter().enumerate() {
        let mut block = Block::new();

        let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
            Type::uint32(),
        );
        id_col.append(idx as u32);

        let lc_type = Type::lowcardinality(Type::string());
        let mut lc_col = ColumnLowCardinality::new(lc_type);

        for value in values {
            lc_col
                .append_unsafe(&ColumnValue::from_string(*value))
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
async fn test_lowcardinality_string_block_insert_high_cardinality() {
    let (mut client, db_name) =
        create_isolated_test_client("lowcardinality_string_block_high_card")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value LowCardinality(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let lc_type = Type::lowcardinality(Type::string());
    let mut lc_col = ColumnLowCardinality::new(lc_type);

    // Create many entries with few unique values (ideal for LowCardinality)
    let statuses =
        vec!["active", "inactive", "pending", "archived", "deleted"];
    for i in 0..100 {
        let status = statuses[i % statuses.len()];
        lc_col
            .append_unsafe(&ColumnValue::from_string(status))
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
    fn test_lowcardinality_string_block_insert_random(
        values in prop::collection::vec(
            prop::sample::select(vec!["status1", "status2", "status3", "status4"]),
            10..50
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) =
                create_isolated_test_client("lowcardinality_string_block_random")
                    .await
                    .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value LowCardinality(String)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col =
                clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
            let lc_type = Type::lowcardinality(Type::string());
            let mut lc_col = ColumnLowCardinality::new(lc_type);

            for (idx, value) in values.iter().enumerate() {
                id_col.append(idx as u32);
                lc_col
                    .append_unsafe(&ColumnValue::from_string(value))
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

            // Dictionary should have at most 4 unique values
            assert!(result_col.dictionary_size() <= 4);

            cleanup_test_database(&db_name).await;
        });
    }
}
