#!/usr/bin/env python3
"""Generate integration_block_* test files"""

import os

NUMERIC_TYPES = [
    ("u8", "uint8", "ColumnUInt8", "UInt8", "0", "255", "127", "42"),
    ("u16", "uint16", "ColumnUInt16", "UInt16", "0", "65535", "32767", "1000"),
    ("u32", "uint32", "ColumnUInt32", "UInt32", "0", "4294967295", "2147483647", "100000"),
    ("u64", "uint64", "ColumnUInt64", "UInt64", "0", "18446744073709551615", "9223372036854775807", "1000000000"),
    ("u128", "uint128", "ColumnUInt128", "UInt128", "0", "340282366920938463463374607431768211455", "170141183460469231731687303715884105727", "1000000000000"),
    ("i8", "int8", "ColumnInt8", "Int8", "-128", "127", "0", "42"),
    ("i16", "int16", "ColumnInt16", "Int16", "-32768", "32767", "0", "1000"),
    ("i32", "int32", "ColumnInt32", "Int32", "-2147483648", "2147483647", "0", "100000"),
    ("i64", "int64", "ColumnInt64", "Int64", "-9223372036854775808", "9223372036854775807", "0", "1000000000"),
    ("i128", "int128", "ColumnInt128", "Int128", "-170141183460469231731687303715884105728", "170141183460469231731687303715884105727", "0", "1000000000000"),
    ("f32", "float32", "ColumnFloat32", "Float32", "f32::MIN", "f32::MAX", "0.0", "3.14159"),
    ("f64", "float64", "ColumnFloat64", "Float64", "f64::MIN", "f64::MAX", "0.0", "3.141592653589793"),
]

def generate_numeric_test(rust_type, type_lower, column_type, ch_type, min_val, max_val, mid_val, test_val):
    is_float = rust_type in ["f32", "f64"]
    comparison = "assert!((result_col.at(idx) - *expected).abs() < 1e-6);" if is_float else "assert_eq!(result_col.at(idx), *expected);"

    return f'''/// Integration tests for {ch_type} column using Block insertion
mod common;

use clickhouse_client::{{
    column::numeric::{column_type},
    types::Type,
    Block,
}};
use common::{{
    cleanup_test_database,
    create_isolated_test_client,
}};
use proptest::prelude::*;
use std::sync::Arc;

#[tokio::test]
#[ignore]
async fn test_{type_lower}_block_insert_basic() {{
    let (mut client, db_name) = create_isolated_test_client("{type_lower}_block_basic")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {{}}.test_table (value {ch_type}) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = {column_type}::new(Type::{type_lower}());
    col.append({test_val});
    col.append({min_val});
    col.append({max_val});
    block
        .append_column("value", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{{}}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT value FROM {{}}.test_table ORDER BY value", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<{column_type}>()
        .expect("Invalid column type");

    let mut expected = vec![{test_val}, {min_val}, {max_val}];
    expected.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    for (idx, exp) in expected.iter().enumerate() {{
        {comparison}
    }}

    cleanup_test_database(&db_name).await;
}}

#[tokio::test]
#[ignore]
async fn test_{type_lower}_block_insert_boundary() {{
    let (mut client, db_name) = create_isolated_test_client("{type_lower}_block_boundary")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {{}}.test_table (id UInt32, value {ch_type}) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = vec![
        ("Min value", {min_val}),
        ("Max value", {max_val}),
        ("Mid value", {mid_val}),
        ("Test value", {test_val}),
    ];

    for (idx, (_desc, value)) in test_cases.iter().enumerate() {{
        let mut block = Block::new();

        let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
            Type::uint32()
        );
        id_col.append(idx as u32);

        let mut val_col = {column_type}::new(Type::{type_lower}());
        val_col.append(*value);

        block
            .append_column("id", Arc::new(id_col))
            .expect("Failed to append id column");
        block
            .append_column("value", Arc::new(val_col))
            .expect("Failed to append value column");

        client
            .insert(&format!("{{}}.test_table", db_name), block)
            .await
            .expect("Failed to insert block");
    }}

    let result = client
        .query(format!(
            "SELECT value FROM {{}}.test_table ORDER BY id",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), test_cases.len());
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<{column_type}>()
        .expect("Invalid column type");

    for (idx, (_desc, expected)) in test_cases.iter().enumerate() {{
        {comparison}
    }}

    cleanup_test_database(&db_name).await;
}}

proptest! {{
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_{type_lower}_block_insert_random(values in prop::collection::vec(any::<{rust_type}>(), 1..100)) {{
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {{
            let (mut client, db_name) = create_isolated_test_client("{type_lower}_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {{}}.test_table (id UInt32, value {ch_type}) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
                Type::uint32()
            );
            let mut val_col = {column_type}::new(Type::{type_lower}());

            for (idx, value) in values.iter().enumerate() {{
                id_col.append(idx as u32);
                val_col.append(*value);
            }}

            block
                .append_column("id", Arc::new(id_col))
                .expect("Failed to append id column");
            block
                .append_column("value", Arc::new(val_col))
                .expect("Failed to append value column");

            client
                .insert(&format!("{{}}.test_table", db_name), block)
                .await
                .expect("Failed to insert block");

            let result = client
                .query(format!(
                    "SELECT value FROM {{}}.test_table ORDER BY id",
                    db_name
                ))
                .await
                .expect("Failed to select");

            assert_eq!(result.total_rows(), values.len());
            let result_col = result.blocks()[0]
                .column(0)
                .expect("Column not found")
                .as_any()
                .downcast_ref::<{column_type}>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {{
                {comparison}
            }}

            cleanup_test_database(&db_name).await;
        }});
    }}
}}
'''

def main():
    print("Generating integration_block test files for numeric types...")

    for rust_type, type_lower, column_type, ch_type, min_val, max_val, mid_val, test_val in NUMERIC_TYPES:
        content = generate_numeric_test(rust_type, type_lower, column_type, ch_type, min_val, max_val, mid_val, test_val)
        filename = f"tests/integration_block_{type_lower}.rs"
        with open(filename, 'w') as f:
            f.write(content)
        print(f"Created {filename}")

    print(f"\nGenerated {len(NUMERIC_TYPES)} numeric test files")

if __name__ == "__main__":
    main()
