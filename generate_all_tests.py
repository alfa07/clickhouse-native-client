#!/usr/bin/env python3
"""Generate ALL integration_block_* test files"""

import os

def write_file(filename, content):
    with open(filename, 'w') as f:
        f.write(content)
    print(f"Created {filename}")

# String test
def generate_string_test():
    return '''/// Integration tests for String column using Block insertion
mod common;

use clickhouse_client::{
    column::string::ColumnString,
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
async fn test_string_block_insert_basic() {
    let (mut client, db_name) = create_isolated_test_client("string_block_basic")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value String) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnString::new(Type::string());
    col.append("hello");
    col.append("world");
    col.append("");
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
        .downcast_ref::<ColumnString>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), "");
    assert_eq!(result_col.at(1), "hello");
    assert_eq!(result_col.at(2), "world");

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_string_block_insert_boundary() {
    let (mut client, db_name) = create_isolated_test_client("string_block_boundary")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value String) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = vec![
        ("Empty string", ""),
        ("Single char", "a"),
        ("Unicode", "Hello 世界 🌍"),
        ("Long string", &"x".repeat(1000)),
        ("Special chars", "\\n\\t\\"'"),
    ];

    for (idx, (_desc, value)) in test_cases.iter().enumerate() {
        let mut block = Block::new();

        let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
            Type::uint32()
        );
        id_col.append(idx as u32);

        let mut val_col = ColumnString::new(Type::string());
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
        .downcast_ref::<ColumnString>()
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
    fn test_string_block_insert_random(values in prop::collection::vec(".*", 1..50)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("string_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value String) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
                Type::uint32()
            );
            let mut val_col = ColumnString::new(Type::string());

            for (idx, value) in values.iter().enumerate() {
                id_col.append(idx as u32);
                val_col.append(value.as_str());
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
                .downcast_ref::<ColumnString>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {
                assert_eq!(result_col.at(idx), expected.as_str());
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
'''

#  FixedString test
def generate_fixedstring_test():
    return '''/// Integration tests for FixedString column using Block insertion
mod common;

use clickhouse_client::{
    column::string::ColumnFixedString,
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
async fn test_fixedstring_block_insert_basic() {
    let (mut client, db_name) = create_isolated_test_client("fixedstring_block_basic")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value FixedString(10)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnFixedString::new(Type::fixed_string(10));
    col.append("hello".as_bytes());
    col.append("world".as_bytes());
    col.append("test".as_bytes());
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
        .downcast_ref::<ColumnFixedString>()
        .expect("Invalid column type");

    assert_eq!(result_col.size(), 3);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_fixedstring_block_insert_boundary() {
    let (mut client, db_name) = create_isolated_test_client("fixedstring_block_boundary")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value FixedString(10)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = vec![
        ("Empty bytes", vec![0u8; 10]),
        ("Partial fill", {
            let mut v = b"hello".to_vec();
            v.resize(10, 0);
            v
        }),
        ("Full string", {
            let mut v = b"0123456789".to_vec();
            v.resize(10, 0);
            v
        }),
    ];

    for (idx, (_desc, value)) in test_cases.iter().enumerate() {
        let mut block = Block::new();

        let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
            Type::uint32()
        );
        id_col.append(idx as u32);

        let mut val_col = ColumnFixedString::new(Type::fixed_string(10));
        val_col.append(value.as_slice());

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

    cleanup_test_database(&db_name).await;
}
'''

def generate_uuid_test():
    return '''/// Integration tests for UUID column using Block insertion
mod common;

use clickhouse_client::{
    column::uuid::{ColumnUuid, Uuid},
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
async fn test_uuid_block_insert_basic() {
    let (mut client, db_name) = create_isolated_test_client("uuid_block_basic")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value UUID) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnUuid::new(Type::uuid());
    col.append(Uuid::from_u128(12345678901234567890));
    col.append(Uuid::from_u128(0));
    col.append(Uuid::from_u128(u128::MAX));
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
        .downcast_ref::<ColumnUuid>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), Uuid::from_u128(0));

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_uuid_block_insert_boundary() {
    let (mut client, db_name) = create_isolated_test_client("uuid_block_boundary")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value UUID) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = vec![
        ("Min UUID", Uuid::from_u128(0)),
        ("Max UUID", Uuid::from_u128(u128::MAX)),
        ("Mid UUID", Uuid::from_u128(u128::MAX / 2)),
        ("Random UUID", Uuid::from_u128(0x123456789ABCDEF0123456789ABCDEF0)),
    ];

    for (idx, (_desc, value)) in test_cases.iter().enumerate() {
        let mut block = Block::new();

        let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
            Type::uint32()
        );
        id_col.append(idx as u32);

        let mut val_col = ColumnUuid::new(Type::uuid());
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
        .downcast_ref::<ColumnUuid>()
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
    fn test_uuid_block_insert_random(values in prop::collection::vec(any::<u128>(), 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("uuid_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value UUID) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
                Type::uint32()
            );
            let mut val_col = ColumnUuid::new(Type::uuid());

            for (idx, value) in values.iter().enumerate() {
                id_col.append(idx as u32);
                val_col.append(Uuid::from_u128(*value));
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
                .downcast_ref::<ColumnUuid>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {
                assert_eq!(result_col.at(idx), Uuid::from_u128(*expected));
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
'''

def main():
    print("Generating integration_block test files...")

    count = 0

    # String
    write_file("tests/integration_block_string.rs", generate_string_test())
    count += 1

    # FixedString
    write_file("tests/integration_block_fixedstring.rs", generate_fixedstring_test())
    count += 1

    # UUID
    write_file("tests/integration_block_uuid.rs", generate_uuid_test())
    count += 1

    print(f"\nGenerated {count} additional test files (String, FixedString, UUID)")
    print("Note: Run generate_tests.py for numeric types, and add more generators for Date, IP, Enum, etc.")

if __name__ == "__main__":
    main()
