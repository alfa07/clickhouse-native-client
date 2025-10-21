/// Integration tests for Tuple(Float32, Float64) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        numeric::{
            ColumnFloat32,
            ColumnFloat64,
        },
        ColumnTuple,
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
async fn test_tuple_float32_float64_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("tuple_float32_float64_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Tuple(Float32, Float64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let mut col1 = ColumnFloat32::new(Type::float32());
    col1.append(1.5f32);
    col1.append(2.5f32);
    col1.append(3.5f32);

    let mut col2 = ColumnFloat64::new(Type::float64());
    col2.append(10.5f64);
    col2.append(20.5f64);
    col2.append(30.5f64);

    let tuple_type =
        Type::Tuple { item_types: vec![Type::float32(), Type::float64()] };
    let tuple_col =
        ColumnTuple::new(tuple_type, vec![Arc::new(col1), Arc::new(col2)]);

    block
        .append_column("value", Arc::new(tuple_col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!(
            "SELECT value FROM {}.test_table ORDER BY value.1",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);
    let blocks = result.blocks();
    let result_col = blocks[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnTuple>()
        .expect("Invalid column type");

    let col1_arc = result_col.column_at(0);
    let result_col1 = col1_arc
        .as_any()
        .downcast_ref::<ColumnFloat32>()
        .expect("Invalid column type");
    let col2_arc = result_col.column_at(1);
    let result_col2 = col2_arc
        .as_any()
        .downcast_ref::<ColumnFloat64>()
        .expect("Invalid column type");

    assert_eq!(result_col1.at(0), 1.5f32);
    assert_eq!(result_col2.at(0), 10.5f64);
    assert_eq!(result_col1.at(1), 2.5f32);
    assert_eq!(result_col2.at(1), 20.5f64);
    assert_eq!(result_col1.at(2), 3.5f32);
    assert_eq!(result_col2.at(2), 30.5f64);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_tuple_float32_float64_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("tuple_float32_float64_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Tuple(Float32, Float64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = vec![
        ("Zero values", 0.0f32, 0.0f64),
        ("Negative values", -1.5f32, -10.5f64),
        ("Large values", 1e10f32, 1e100f64),
        ("Small values", 1e-10f32, 1e-100f64),
        ("NaN and Inf", f32::NAN, f64::INFINITY),
    ];

    let mut block = Block::new();

    let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
        Type::uint32(),
    );
    let mut col1 = ColumnFloat32::new(Type::float32());
    let mut col2 = ColumnFloat64::new(Type::float64());

    for (idx, (_desc, val1, val2)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);
        col1.append(*val1);
        col2.append(*val2);
    }

    let tuple_type =
        Type::Tuple { item_types: vec![Type::float32(), Type::float64()] };
    let tuple_col =
        ColumnTuple::new(tuple_type, vec![Arc::new(col1), Arc::new(col2)]);

    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append id column");
    block
        .append_column("value", Arc::new(tuple_col))
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
    let result_col = blocks[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnTuple>()
        .expect("Invalid column type");

    let col1_arc = result_col.column_at(0);
    let result_col1 = col1_arc
        .as_any()
        .downcast_ref::<ColumnFloat32>()
        .expect("Invalid column type");
    let col2_arc = result_col.column_at(1);
    let result_col2 = col2_arc
        .as_any()
        .downcast_ref::<ColumnFloat64>()
        .expect("Invalid column type");

    for (idx, (_desc, expected1, expected2)) in test_cases.iter().enumerate() {
        let actual1 = result_col1.at(idx);
        let actual2 = result_col2.at(idx);

        if expected1.is_nan() {
            assert!(actual1.is_nan());
        } else {
            assert_eq!(actual1, *expected1);
        }

        if expected2.is_nan() {
            assert!(actual2.is_nan());
        } else if expected2.is_infinite() {
            assert!(actual2.is_infinite());
        } else {
            assert_eq!(actual2, *expected2);
        }
    }

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_tuple_float32_float64_block_insert_random(
        values in prop::collection::vec((any::<f32>(), any::<f64>()), 1..50)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) =
                create_isolated_test_client("tuple_float32_float64_block_random")
                    .await
                    .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Tuple(Float32, Float64)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col =
                clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
            let mut col1 = ColumnFloat32::new(Type::float32());
            let mut col2 = ColumnFloat64::new(Type::float64());

            for (idx, (val1, val2)) in values.iter().enumerate() {
                id_col.append(idx as u32);
                col1.append(*val1);
                col2.append(*val2);
            }

            let tuple_type = Type::Tuple {
                item_types: vec![Type::float32(), Type::float64()],
            };
            let tuple_col =
                ColumnTuple::new(tuple_type, vec![Arc::new(col1), Arc::new(col2)]);

            block
                .append_column("id", Arc::new(id_col))
                .expect("Failed to append id column");
            block
                .append_column("value", Arc::new(tuple_col))
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
            let result_col = blocks[0]
                .column(0)
                .expect("Column not found")
                .as_any()
                .downcast_ref::<ColumnTuple>()
                .expect("Invalid column type");

            let col1_arc = result_col.column_at(0);
            let result_col1 = col1_arc
                .as_any()
                .downcast_ref::<ColumnFloat32>()
                .expect("Invalid column type");
            let col2_arc = result_col.column_at(1);
            let result_col2 = col2_arc
                .as_any()
                .downcast_ref::<ColumnFloat64>()
                .expect("Invalid column type");

            for (idx, (expected1, expected2)) in values.iter().enumerate() {
                let actual1 = result_col1.at(idx);
                let actual2 = result_col2.at(idx);

                if expected1.is_nan() {
                    assert!(actual1.is_nan());
                } else {
                    assert_eq!(actual1, *expected1);
                }

                if expected2.is_nan() {
                    assert!(actual2.is_nan());
                } else {
                    assert_eq!(actual2, *expected2);
                }
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
