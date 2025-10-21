#!/bin/bash
# Script to generate all integration_block_*.rs test files

set -e

TESTS_DIR="tests"

# Function to create a numeric type test
create_numeric_test() {
    local TYPE_LOWER=$1
    local TYPE_UPPER=$2
    local COLUMN_TYPE=$3
    local CH_TYPE=$4
    local MIN_VAL=$5
    local MAX_VAL=$6
    local MID_VAL=$7
    local TEST_VAL=$8

    cat > "${TESTS_DIR}/integration_block_${TYPE_LOWER}.rs" << 'ENDFILE'
/// Integration tests for TYPE_UPPER column using Block insertion
mod common;

use clickhouse_client::{
    column::numeric::COLUMN_TYPE,
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
async fn test_TYPE_LOWER_block_insert_basic() {
    let (mut client, db_name) = create_isolated_test_client("TYPE_LOWER_block_basic")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value CH_TYPE) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = COLUMN_TYPE::new(Type::TYPE_LOWER());
    col.append(TEST_VAL);
    col.append(MIN_VAL);
    col.append(MAX_VAL);
    block
        .append_column("value", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT value FROM {}.test_table ORDER BY value", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<COLUMN_TYPE>()
        .expect("Invalid column type");

    let mut expected = vec![TEST_VAL, MIN_VAL, MAX_VAL];
    expected.sort();

    for (idx, exp) in expected.iter().enumerate() {
        assert_eq!(result_col.at(idx), *exp);
    }

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_TYPE_LOWER_block_insert_boundary() {
    let (mut client, db_name) = create_isolated_test_client("TYPE_LOWER_block_boundary")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value CH_TYPE) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = vec![
        ("Min value", MIN_VAL),
        ("Max value", MAX_VAL),
        ("Mid value", MID_VAL),
        ("Test value", TEST_VAL),
    ];

    for (idx, (_desc, value)) in test_cases.iter().enumerate() {
        let mut block = Block::new();

        let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
            Type::uint32()
        );
        id_col.append(idx as u32);

        let mut val_col = COLUMN_TYPE::new(Type::TYPE_LOWER());
        val_col.append(*value);

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
    }

    let result = client
        .query(format!(
            "SELECT value FROM {}.test_table ORDER BY id",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), test_cases.len());
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<COLUMN_TYPE>()
        .expect("Invalid column type");

    for (idx, (_desc, expected)) in test_cases.iter().enumerate() {
        assert_eq!(result_col.at(idx), *expected);
    }

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_TYPE_LOWER_block_insert_random(values in prop::collection::vec(PROPTEST_STRATEGY, 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("TYPE_LOWER_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value CH_TYPE) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
                Type::uint32()
            );
            let mut val_col = COLUMN_TYPE::new(Type::TYPE_LOWER());

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
            let result_col = result.blocks()[0]
                .column(0)
                .expect("Column not found")
                .as_any()
                .downcast_ref::<COLUMN_TYPE>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {
                assert_eq!(result_col.at(idx), *expected);
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
ENDFILE

    # Replace placeholders
    sed -i '' "s/TYPE_LOWER/${TYPE_LOWER}/g" "${TESTS_DIR}/integration_block_${TYPE_LOWER}.rs"
    sed -i '' "s/TYPE_UPPER/${TYPE_UPPER}/g" "${TESTS_DIR}/integration_block_${TYPE_LOWER}.rs"
    sed -i '' "s/COLUMN_TYPE/${COLUMN_TYPE}/g" "${TESTS_DIR}/integration_block_${TYPE_LOWER}.rs"
    sed -i '' "s/CH_TYPE/${CH_TYPE}/g" "${TESTS_DIR}/integration_block_${TYPE_LOWER}.rs"
    sed -i '' "s/MIN_VAL/${MIN_VAL}/g" "${TESTS_DIR}/integration_block_${TYPE_LOWER}.rs"
    sed -i '' "s/MAX_VAL/${MAX_VAL}/g" "${TESTS_DIR}/integration_block_${TYPE_LOWER}.rs"
    sed -i '' "s/MID_VAL/${MID_VAL}/g" "${TESTS_DIR}/integration_block_${TYPE_LOWER}.rs"
    sed -i '' "s/TEST_VAL/${TEST_VAL}/g" "${TESTS_DIR}/integration_block_${TYPE_LOWER}.rs"
    sed -i '' "s/PROPTEST_STRATEGY/any::<${RUST_TYPE}>()/g" "${TESTS_DIR}/integration_block_${TYPE_LOWER}.rs"

    echo "Created ${TESTS_DIR}/integration_block_${TYPE_LOWER}.rs"
}

echo "Generating integration_block tests..."

# This is getting complex. Let me use a Rust program instead for better type handling.
