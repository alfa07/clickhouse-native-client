/// Integration tests for Nullable(IPv6) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        ipv6::ColumnIpv6,
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
async fn test_nullable_ipv6_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_ipv6_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Nullable(IPv6)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let nullable_type = Type::nullable(Type::ipv6());
    let mut nullable_col = ColumnNullable::new(nullable_type);

    // Add some non-null values
    nullable_col.append_non_null();
    Arc::get_mut(nullable_col.nested_mut())
        .unwrap()
        .as_any_mut()
        .downcast_mut::<ColumnIpv6>()
        .unwrap()
        .append_from_string("::1")
        .expect("Failed to parse IPv6");

    // Add a null value
    nullable_col.append_null();

    // Add another non-null value
    nullable_col.append_non_null();
    Arc::get_mut(nullable_col.nested_mut())
        .unwrap()
        .as_any_mut()
        .downcast_mut::<ColumnIpv6>()
        .unwrap()
        .append_from_string("2001:db8::1")
        .expect("Failed to parse IPv6");

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
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnNullable>()
        .expect("Invalid column type");

    assert!(!result_col.is_null(0));
    assert!(result_col.is_null(1));
    assert!(!result_col.is_null(2));

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_nullable_ipv6_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_ipv6_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Nullable(IPv6)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases: Vec<(&str, Option<&str>)> = vec![
        ("Null value", None),
        ("Localhost", Some("::1")),
        ("Full address", Some("2001:0db8:85a3:0000:0000:8a2e:0370:7334")),
        ("Compressed", Some("2001:db8::1")),
        ("Link local", Some("fe80::1")),
        ("Zero", Some("::")),
        ("Another null", None),
    ];

    let mut block = Block::new();

    let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
        Type::uint32(),
    );
    let nullable_type = Type::nullable(Type::ipv6());
    let mut nullable_col = ColumnNullable::new(nullable_type);

    for (idx, (_desc, value_opt)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);

        match value_opt {
            Some(value) => {
                nullable_col.append_non_null();
                Arc::get_mut(nullable_col.nested_mut())
                    .unwrap()
                    .as_any_mut()
                    .downcast_mut::<ColumnIpv6>()
                    .unwrap()
                    .append_from_string(*value)
                    .expect("Failed to parse IPv6");
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
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnNullable>()
        .expect("Invalid column type");

    for (idx, (_desc, expected_opt)) in test_cases.iter().enumerate() {
        match expected_opt {
            Some(_) => {
                assert!(!result_col.is_null(idx));
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
async fn test_nullable_ipv6_block_insert_all_nulls() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_ipv6_block_all_nulls")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Nullable(IPv6)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let nullable_type = Type::nullable(Type::ipv6());
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
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
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
    fn test_nullable_ipv6_block_insert_random(
        values in prop::collection::vec(
            prop::option::of((any::<u128>(),)),
            1..50
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) =
                create_isolated_test_client("nullable_ipv6_block_random")
                    .await
                    .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Nullable(IPv6)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col =
                clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
            let nullable_type = Type::nullable(Type::ipv6());
            let mut nullable_col = ColumnNullable::new(nullable_type);

            for (idx, value_opt) in values.iter().enumerate() {
                id_col.append(idx as u32);
                match value_opt {
                    Some((ipv6_as_u128,)) => {
                        nullable_col.append_non_null();
                        let bytes = ipv6_as_u128.to_be_bytes();
                        Arc::get_mut(nullable_col.nested_mut())
                            .unwrap()
                            .as_any_mut()
                            .downcast_mut::<ColumnIpv6>()
                            .unwrap()
                            .append(bytes);
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
            let result_col = result.blocks()[0]
                .column(0)
                .expect("Column not found")
                .as_any()
                .downcast_ref::<ColumnNullable>()
                .expect("Invalid column type");

            let nested = result_col
                .nested()
                .as_any()
                .downcast_ref::<ColumnIpv6>()
                .expect("Nested should be ColumnIpv6");

            for (idx, expected_opt) in values.iter().enumerate() {
                match expected_opt {
                    Some((expected,)) => {
                        assert!(!result_col.is_null(idx));
                        let actual_bytes = nested.at(idx);
                        let expected_bytes = expected.to_be_bytes();
                        assert_eq!(actual_bytes, expected_bytes);
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
