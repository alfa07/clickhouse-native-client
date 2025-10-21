/// Integration tests for Nullable(String) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        string::ColumnString,
        ColumnNullable,
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
async fn test_nullable_string_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_string_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Nullable(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let nullable_type = Type::nullable(Type::string());
    let mut nullable_col = ColumnNullable::new(nullable_type);

    // Add some non-null values
    nullable_col.append_non_null();
    Arc::get_mut(nullable_col.nested_mut())
        .unwrap()
        .as_any_mut()
        .downcast_mut::<ColumnString>()
        .unwrap()
        .append("hello");

    // Add a null value
    nullable_col.append_null();

    // Add another non-null value
    nullable_col.append_non_null();
    Arc::get_mut(nullable_col.nested_mut())
        .unwrap()
        .as_any_mut()
        .downcast_mut::<ColumnString>()
        .unwrap()
        .append("world");

    block
        .append_column("value", Arc::new(nullable_col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT value FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);
    let blocks = result.blocks();

    let col_ref = blocks[0].column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnNullable>()
        .expect("Invalid column type");

    assert!(!result_col.is_null(0));
    assert!(result_col.is_null(1));
    assert!(!result_col.is_null(2));

    let nested = result_col
        .nested()
        .as_any()
        .downcast_ref::<ColumnString>()
        .expect("Nested should be ColumnString");
    assert_eq!(nested.at(0), "hello");
    assert_eq!(nested.at(2), "world");

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_nullable_string_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_string_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Nullable(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let long_string = "x".repeat(1000);
    let test_cases: Vec<(&str, Option<&str>)> = vec![
        ("Null value", None),
        ("Empty string", Some("")),
        ("Single char", Some("a")),
        ("Unicode", Some("Hello 世界")),
        ("Long string", Some(&long_string)),
        ("Special chars", Some("\n\t\"'")),
        ("Another null", None),
    ];

    let mut block = Block::new();

    let mut id_col =
        clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
    let nullable_type = Type::nullable(Type::string());
    let mut nullable_col = ColumnNullable::new(nullable_type);

    for (idx, (_desc, value_opt)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);

        match value_opt {
            Some(value) => {
                nullable_col.append_non_null();
                Arc::get_mut(nullable_col.nested_mut())
                    .unwrap()
                    .as_any_mut()
                    .downcast_mut::<ColumnString>()
                    .unwrap()
                    .append(*value);
            }
            None => {
                nullable_col.append_null();
            }
        }
    }

    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append id column");
    block
        .append_column("value", Arc::new(nullable_col))
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
        .downcast_ref::<ColumnNullable>()
        .expect("Invalid column type");

    let nested = result_col
        .nested()
        .as_any()
        .downcast_ref::<ColumnString>()
        .expect("Nested should be ColumnString");

    for (idx, (_desc, expected_opt)) in test_cases.iter().enumerate() {
        match expected_opt {
            Some(expected) => {
                assert!(!result_col.is_null(idx));
                assert_eq!(nested.at(idx), *expected);
            }
            None => {
                assert!(result_col.is_null(idx));
            }
        }
    }

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_nullable_string_block_insert_all_nulls() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_string_block_all_nulls")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Nullable(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let nullable_type = Type::nullable(Type::string());
    let mut nullable_col = ColumnNullable::new(nullable_type);

    // Add 5 null values
    for _ in 0..5 {
        nullable_col.append_null();
    }

    block
        .append_column("value", Arc::new(nullable_col))
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
    let blocks = result.blocks();

    let col_ref = blocks[0].column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnNullable>()
        .expect("Invalid column type");

    for i in 0..5 {
        assert!(result_col.is_null(i));
    }

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_nullable_string_block_insert_random(
        values in prop::collection::vec(prop::option::of(".*"), 1..50)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) =
                create_isolated_test_client("nullable_string_block_random")
                    .await
                    .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Nullable(String)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col =
                clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
            let nullable_type = Type::nullable(Type::string());
            let mut nullable_col = ColumnNullable::new(nullable_type);

            for (idx, value_opt) in values.iter().enumerate() {
                id_col.append(idx as u32);
                match value_opt {
                    Some(value) => {
                        nullable_col.append_non_null();
                        Arc::get_mut(nullable_col.nested_mut())
                            .unwrap()
                            .as_any_mut()
                            .downcast_mut::<ColumnString>()
                            .unwrap()
                            .append(value.as_str());
                    }
                    None => {
                        nullable_col.append_null();
                    }
                }
            }

            block
                .append_column("id", Arc::new(id_col))
                .expect("Failed to append id column");
            block
                .append_column("value", Arc::new(nullable_col))
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

                .downcast_ref::<ColumnNullable>()

                .expect("Invalid column type");

            let nested = result_col
                .nested()
                .as_any()
                .downcast_ref::<ColumnString>()
                .expect("Nested should be ColumnString");

            for (idx, expected_opt) in values.iter().enumerate() {
                match expected_opt {
                    Some(expected) => {
                        assert!(!result_col.is_null(idx));
                        assert_eq!(nested.at(idx), expected.as_str());
                    }
                    None => {
                        assert!(result_col.is_null(idx));
                    }
                }
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
