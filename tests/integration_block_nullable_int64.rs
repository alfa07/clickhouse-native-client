/// Integration tests for Nullable(Int64) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        numeric::ColumnInt64,
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
async fn test_nullable_int64_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_int64_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Nullable(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let nullable_type = Type::nullable(Type::int64());
    let mut nullable_col = ColumnNullable::new(nullable_type);

    // Add some non-null values
    nullable_col.append_non_null();
    nullable_col.nested_mut::<ColumnInt64>().append(42);

    // Add a null value
    nullable_col.append_null();
    nullable_col.nested_mut::<ColumnInt64>().append(0); // Placeholder for null value

    // Add another non-null value
    nullable_col.append_non_null();
    nullable_col.nested_mut::<ColumnInt64>().append(-1000);

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

    let nested: &ColumnInt64 = result_col.nested();
    assert_eq!(nested.at(0), 42);
    assert_eq!(nested.at(2), -1000);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_nullable_int64_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_int64_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Nullable(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases: Vec<(&str, Option<i64>)> = vec![
        ("Null value", None),
        ("Zero", Some(0)),
        ("Min value", Some(i64::MIN)),
        ("Max value", Some(i64::MAX)),
        ("Negative", Some(-42)),
        ("Positive", Some(42)),
        ("Another null", None),
        ("Large negative", Some(-9223372036854775807)),
        ("Large positive", Some(9223372036854775806)),
    ];

    let mut block = Block::new();

    let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new();
    let nullable_type = Type::nullable(Type::int64());
    let mut nullable_col = ColumnNullable::new(nullable_type);

    for (idx, (_desc, value_opt)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);

        match value_opt {
            Some(value) => {
                nullable_col.append_non_null();
                nullable_col.nested_mut::<ColumnInt64>().append(*value);
            }
            None => {
                nullable_col.append_null();
                nullable_col.nested_mut::<ColumnInt64>().append(0); // Placeholder for null value
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

    let nested: &ColumnInt64 = result_col.nested();

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
async fn test_nullable_int64_block_insert_all_nulls() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_int64_block_all_nulls")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Nullable(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let nullable_type = Type::nullable(Type::int64());
    let mut nullable_col = ColumnNullable::new(nullable_type);

    // Add 5 null values
    for _ in 0..5 {
        nullable_col.append_null();
        nullable_col.nested_mut::<ColumnInt64>().append(0); // Placeholder for
                                                            // null value
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

#[tokio::test]
#[ignore]
async fn test_nullable_int64_block_insert_all_non_null() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_int64_block_all_non_null")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Nullable(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let nullable_type = Type::nullable(Type::int64());
    let mut nullable_col = ColumnNullable::new(nullable_type);

    // Add 5 non-null values
    for i in 0..5 {
        nullable_col.append_non_null();
        nullable_col.nested_mut::<ColumnInt64>().append(i * 10);
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

    let nested: &ColumnInt64 = result_col.nested();

    for i in 0..5 {
        assert!(!result_col.is_null(i));
        assert_eq!(nested.at(i), (i as i64) * 10);
    }

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_nullable_int64_block_insert_random(
        values in prop::collection::vec(prop::option::of(any::<i64>()), 1..100)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) =
                create_isolated_test_client("nullable_int64_block_random")
                    .await
                    .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Nullable(Int64)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col =
                clickhouse_client::column::numeric::ColumnUInt32::new();
            let nullable_type = Type::nullable(Type::int64());
            let mut nullable_col = ColumnNullable::new(nullable_type);

            for (idx, value_opt) in values.iter().enumerate() {
                id_col.append(idx as u32);
                match value_opt {
                    Some(value) => {
                        nullable_col.append_non_null();
                        nullable_col.nested_mut::<ColumnInt64>()
                            .append(*value);
                    }
                    None => {
                        nullable_col.append_null();
                        nullable_col.nested_mut::<ColumnInt64>()
                            .append(0); // Placeholder for null value
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

            let nested: &ColumnInt64 = result_col.nested();

            for (idx, expected_opt) in values.iter().enumerate() {
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
        });
    }
}
