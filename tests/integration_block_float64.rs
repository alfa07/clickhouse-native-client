/// Integration tests for Float64 column using Block insertion
mod common;

use clickhouse_client::{
    column::numeric::ColumnFloat64,
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
async fn test_float64_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("float64_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Float64) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnFloat64::new();
    col.append(3.141592653589793);
    col.append(f64::MIN);
    col.append(f64::MAX);
    block
        .append_column("value", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!(
            "SELECT value FROM {}.test_table ORDER BY value",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);
    let blocks = result.blocks();
    let col_ref = blocks[0].column(0).expect("Column not found");
    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnFloat64>()
        .expect("Invalid column type");

    let mut expected = vec![3.141592653589793, f64::MIN, f64::MAX];
    expected
        .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    for (idx, exp) in expected.iter().enumerate() {
        assert!((result_col.at(idx) - *exp).abs() < 1e-6);
    }

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_float64_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("float64_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Float64) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = [
        ("Min value", f64::MIN),
        ("Max value", f64::MAX),
        ("Mid value", 0.0),
        ("Test value", 3.141592653589793),
    ];

    let mut block = Block::new();
    let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new();
    let mut val_col = ColumnFloat64::new();

    for (idx, (_desc, value)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);
        val_col.append(*value);
    }

    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append id column");
    block
        .append_column("value", Arc::new(val_col))
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
    let blocks = result.blocks();
    let col_ref = blocks[0].column(0).expect("Column not found");
    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnFloat64>()
        .expect("Invalid column type");

    for (idx, (_desc, expected)) in test_cases.iter().enumerate() {
        assert!((result_col.at(idx) - *expected).abs() < 1e-6);
    }

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_float64_block_insert_random(values in prop::collection::vec(any::<f64>(), 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("float64_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Float64) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new();
            let mut val_col = ColumnFloat64::new();

            for (idx, value) in values.iter().enumerate() {
                id_col.append(idx as u32);
                val_col.append(*value);
            }

            block
                .append_column("id", Arc::new(id_col))
                .expect("Failed to append id column");
            block
                .append_column("value", Arc::new(val_col))
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
            let blocks = result.blocks();
            let col_ref = blocks[0].column(0).expect("Column not found");
            let result_col = col_ref
                .as_any()
                .downcast_ref::<ColumnFloat64>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {
                assert!((result_col.at(idx) - *expected).abs() < 1e-6);
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
