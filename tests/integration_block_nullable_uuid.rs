/// Integration tests for Nullable(UUID) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        uuid::Uuid,
        ColumnNullable,
        ColumnUuid,
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
async fn test_nullable_uuid_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_uuid_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Nullable(UUID)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let nullable_type = Type::nullable(Type::uuid());
    let mut nullable_col = ColumnNullable::new(nullable_type);

    // Add some non-null values
    nullable_col.append_non_null();
    Arc::get_mut(nullable_col.nested_mut())
        .unwrap()
        .as_any_mut()
        .downcast_mut::<ColumnUuid>()
        .unwrap()
        .append(Uuid::new(0x1234567890abcdef, 0xfedcba0987654321));

    // Add a null value
    nullable_col.append_null();

    // Add another non-null value
    nullable_col.append_non_null();
    Arc::get_mut(nullable_col.nested_mut())
        .unwrap()
        .as_any_mut()
        .downcast_mut::<ColumnUuid>()
        .unwrap()
        .append(Uuid::new(0xabcdef1234567890, 0x0987654321fedcba));

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
        .downcast_ref::<ColumnUuid>()
        .expect("Nested should be ColumnUuid");
    assert_eq!(
        nested.at(0),
        Uuid::new(0x1234567890abcdef, 0xfedcba0987654321)
    );
    assert_eq!(
        nested.at(2),
        Uuid::new(0xabcdef1234567890, 0x0987654321fedcba)
    );

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_nullable_uuid_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_uuid_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Nullable(UUID)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases: Vec<(&str, Option<Uuid>)> = vec![
        ("Null value", None),
        ("Zero UUID", Some(Uuid::new(0, 0))),
        ("Max UUID", Some(Uuid::new(u64::MAX, u64::MAX))),
        (
            "Random UUID 1",
            Some(Uuid::new(0x1111111111111111, 0x2222222222222222)),
        ),
        (
            "Random UUID 2",
            Some(Uuid::new(0xaaaaaaaaaaaaaaaa, 0xbbbbbbbbbbbbbbbb)),
        ),
        ("Another null", None),
        (
            "Pattern UUID",
            Some(Uuid::new(0x123456789abcdef0, 0x0fedcba987654321)),
        ),
    ];

    let mut block = Block::new();

    let mut id_col =
        clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
    let nullable_type = Type::nullable(Type::uuid());
    let mut nullable_col = ColumnNullable::new(nullable_type);

    for (idx, (_desc, value_opt)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);

        match value_opt {
            Some(value) => {
                nullable_col.append_non_null();
                Arc::get_mut(nullable_col.nested_mut())
                    .unwrap()
                    .as_any_mut()
                    .downcast_mut::<ColumnUuid>()
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
        .downcast_ref::<ColumnUuid>()
        .expect("Nested should be ColumnUuid");

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
async fn test_nullable_uuid_block_insert_all_nulls() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_uuid_block_all_nulls")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Nullable(UUID)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let nullable_type = Type::nullable(Type::uuid());
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
    fn test_nullable_uuid_block_insert_random(
        values in prop::collection::vec(
            prop::option::of((any::<u64>(), any::<u64>())),
            1..50
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) =
                create_isolated_test_client("nullable_uuid_block_random")
                    .await
                    .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Nullable(UUID)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col =
                clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
            let nullable_type = Type::nullable(Type::uuid());
            let mut nullable_col = ColumnNullable::new(nullable_type);

            for (idx, value_opt) in values.iter().enumerate() {
                id_col.append(idx as u32);
                match value_opt {
                    Some((high, low)) => {
                        nullable_col.append_non_null();
                        Arc::get_mut(nullable_col.nested_mut())
                            .unwrap()
                            .as_any_mut()
                            .downcast_mut::<ColumnUuid>()
                            .unwrap()
                            .append(Uuid::new(*high, *low));
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
                .downcast_ref::<ColumnUuid>()
                .expect("Nested should be ColumnUuid");

            for (idx, expected_opt) in values.iter().enumerate() {
                match expected_opt {
                    Some((high, low)) => {
                        assert!(!result_col.is_null(idx));
                        assert_eq!(nested.at(idx), Uuid::new(*high, *low));
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
