/// Integration tests for String and FixedString column types
mod common;

use clickhouse_client::{
    column::string::*,
    types::Type,
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};
use proptest::prelude::*;
use std::sync::Arc;

// ============================================================================
// String Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_string_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("string_roundtrip")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (text String) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnString::new(Type::string());
    col.append("".to_string()); // Empty string
    col.append("hello".to_string()); // Simple string
    col.append("world".to_string()); // Another simple string
    col.append("a".to_string()); // Single char
    col.append("ClickHouse is fast!".to_string()); // With space
    col.append("UTF-8: こんにちは".to_string()); // UTF-8 characters
    col.append("Special chars: !@#$%^&*()".to_string()); // Special characters
    col.append("Line\nbreak".to_string()); // Line break
    col.append("Tab\there".to_string()); // Tab
    col.append("Quote: \"test\"".to_string()); // Quotes
    block
        .append_column("text", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT text FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 10);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnString>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), "");
    assert_eq!(result_col.at(1), "hello");
    assert_eq!(result_col.at(2), "world");
    assert_eq!(result_col.at(3), "a");
    assert_eq!(result_col.at(4), "ClickHouse is fast!");
    assert_eq!(result_col.at(5), "UTF-8: こんにちは");
    assert_eq!(result_col.at(6), "Special chars: !@#$%^&*()");
    assert_eq!(result_col.at(7), "Line\nbreak");
    assert_eq!(result_col.at(8), "Tab\there");
    assert_eq!(result_col.at(9), "Quote: \"test\"");

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_string_long_values() {
    let (mut client, db_name) =
        create_isolated_test_client("string_long_values")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (text String) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnString::new(Type::string());

    // Create very long strings
    let long_string_1k = "a".repeat(1024);
    let long_string_10k = "b".repeat(10240);
    let long_string_100k = "c".repeat(102400);

    col.append(long_string_1k.clone());
    col.append(long_string_10k.clone());
    col.append(long_string_100k.clone());

    block
        .append_column("text", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT text FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnString>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), long_string_1k);
    assert_eq!(result_col.at(1), long_string_10k);
    assert_eq!(result_col.at(2), long_string_100k);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// FixedString Tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_fixed_string_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("fixed_string_roundtrip")
            .await
            .expect("Failed to create test client");

    let fixed_size = 10;
    client
        .query(format!(
            "CREATE TABLE {}.test_table (text FixedString({})) ENGINE = Memory",
            db_name, fixed_size
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnFixedString::new(Type::fixed_string(fixed_size));

    col.append("".to_string()); // Empty (will be padded)
    col.append("abc".to_string()); // Short (will be padded)
    col.append("1234567890".to_string()); // Exactly 10 bytes
    col.append("test".to_string()); // Will be padded

    block
        .append_column("text", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT text FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnFixedString>()
        .expect("Invalid column type");

    // at() returns trimmed strings (null bytes removed)
    assert_eq!(result_col.at(0), "");
    assert_eq!(result_col.at(1), "abc");
    assert_eq!(result_col.at(2), "1234567890");
    assert_eq!(result_col.at(3), "test");

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_fixed_string_various_sizes() {
    for size in [1, 5, 16, 32, 64, 128] {
        let (mut client, db_name) =
            create_isolated_test_client(&format!("fixed_string_{}", size))
                .await
                .expect("Failed to create test client");

        client
            .query(format!(
                "CREATE TABLE {}.test_table (text FixedString({})) ENGINE = Memory",
                db_name, size
            ))
            .await
            .expect("Failed to create table");

        let mut block = Block::new();
        let mut col = ColumnFixedString::new(Type::fixed_string(size));

        // Add empty string
        col.append("".to_string());

        // Add string that's exactly the size (or as close as possible)
        let exact_size_str = "x".repeat(size);
        col.append(exact_size_str.clone());

        // Add string that's shorter
        if size > 2 {
            col.append("ab".to_string());
        }

        block
            .append_column("text", Arc::new(col))
            .expect("Failed to append column");

        client
            .insert(&format!("{}.test_table", db_name), block)
            .await
            .expect("Failed to insert block");

        let result = client
            .query(format!("SELECT text FROM {}.test_table", db_name))
            .await
            .expect("Failed to select");

        let result_block = &result.blocks()[0];
        let col_ref = result_block.column(0).expect("Column not found");

        let result_col = col_ref
            .as_any()
            .downcast_ref::<ColumnFixedString>()
            .expect("Invalid column type");

        assert_eq!(result_col.at(0), "");
        assert_eq!(result_col.at(1), exact_size_str);

        if size > 2 {
            assert_eq!(result_col.at(2), "ab");
        }

        cleanup_test_database(&db_name).await;
    }
}

// ============================================================================
// Property-based tests
// ============================================================================

proptest! {
    #[test]
    #[ignore]
    fn prop_test_string_values(values in prop::collection::vec(".*", 1..50)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("prop_string")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (text String) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();
            let mut col = ColumnString::new(Type::string());
            for val in &values {
                col.append(val.clone());
            }
            block
                .append_column("text", Arc::new(col))
                .expect("Failed to append column");

            client
                .insert(&format!("{}.test_table", db_name), block)
                .await
                .expect("Failed to insert block");

            let result = client
                .query(format!("SELECT text FROM {}.test_table", db_name))
                .await
                .expect("Failed to select");

            prop_assert_eq!(result.total_rows(), values.len());

            let result_block = &result.blocks()[0];
            let col_ref = result_block.column(0).expect("Column not found");

            let result_col = col_ref.as_any().downcast_ref::<ColumnString>().expect("Invalid column type");

            for (i, expected) in values.iter().enumerate() {
                prop_assert_eq!(&result_col.at(i), expected);
            }

            cleanup_test_database(&db_name).await;
            Ok(())
        })?;
    }
}
