/// Integration tests for UUID column using Block insertion
mod common;

use clickhouse_client::{
    column::uuid::{
        ColumnUuid,
        Uuid,
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
async fn test_uuid_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("uuid_block_basic")
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
    col.append(Uuid::new(0, 12345678901234567890));
    col.append(Uuid::new(0, 0));
    col.append(Uuid::new(u64::MAX, u64::MAX));
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
        .downcast_ref::<ColumnUuid>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), Uuid::new(0, 0));

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_uuid_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("uuid_block_boundary")
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
        ("Min UUID", Uuid::new(0, 0)),
        ("Max UUID", Uuid::new(u64::MAX, u64::MAX)),
        ("Mid UUID", Uuid::new(u64::MAX / 2, u64::MAX)),
        ("Random UUID", Uuid::new(0x123456789ABCDEF0, 0x123456789ABCDEF0)),
    ];

    let mut block = Block::new();
    let mut id_col =
        clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
    let mut val_col = ColumnUuid::new(Type::uuid());

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
                let high = (*value >> 64) as u64;
                let low = *value as u64;
                val_col.append(Uuid::new(high, low));
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
                .downcast_ref::<ColumnUuid>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {
                let high = (*expected >> 64) as u64;
                let low = *expected as u64;
                assert_eq!(result_col.at(idx), Uuid::new(high, low));
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
