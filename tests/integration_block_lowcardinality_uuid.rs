/// Integration tests for LowCardinality(UUID) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        column_value::ColumnValue,
        uuid::Uuid,
        ColumnLowCardinality,
    },
    types::{
        Type,
        TypeCode,
    },
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};
use proptest::prelude::*;
use std::sync::Arc;

// Helper function to create ColumnValue from UUID
fn uuid_to_column_value(uuid: Uuid) -> ColumnValue {
    let mut data = Vec::with_capacity(16);
    data.extend_from_slice(&uuid.high.to_le_bytes());
    data.extend_from_slice(&uuid.low.to_le_bytes());
    ColumnValue { type_code: TypeCode::UUID, data }
}

#[tokio::test]
#[ignore]
async fn test_lowcardinality_uuid_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("lowcardinality_uuid_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value LowCardinality(UUID)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let lc_type = Type::lowcardinality(Type::uuid());
    let mut lc_col = ColumnLowCardinality::new(lc_type);

    let uuid1 = Uuid::new(0x1111111111111111, 0x2222222222222222);
    let uuid2 = Uuid::new(0x3333333333333333, 0x4444444444444444);
    let uuid3 = Uuid::new(0x5555555555555555, 0x6666666666666666);

    // Add some values with repetition
    lc_col
        .append_unsafe(&uuid_to_column_value(uuid1))
        .expect("Failed to append");
    lc_col
        .append_unsafe(&uuid_to_column_value(uuid2))
        .expect("Failed to append");
    lc_col
        .append_unsafe(&uuid_to_column_value(uuid1))
        .expect("Failed to append"); // Repeated
    lc_col
        .append_unsafe(&uuid_to_column_value(uuid3))
        .expect("Failed to append");
    lc_col
        .append_unsafe(&uuid_to_column_value(uuid2))
        .expect("Failed to append"); // Repeated

    block
        .append_column("value", Arc::new(lc_col))
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
        .downcast_ref::<ColumnLowCardinality>()
        .expect("Invalid column type");

    // Dictionary should have only 3 unique values
    assert_eq!(result_col.dictionary_size(), 3);
    assert_eq!(result_col.len(), 5);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_lowcardinality_uuid_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("lowcardinality_uuid_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value LowCardinality(UUID)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases: Vec<(&str, Vec<Uuid>)> = vec![
        (
            "Single unique",
            vec![
                Uuid::new(0x1111, 0x2222),
                Uuid::new(0x1111, 0x2222),
                Uuid::new(0x1111, 0x2222),
            ],
        ),
        (
            "All different",
            vec![
                Uuid::new(0x1111, 0x2222),
                Uuid::new(0x3333, 0x4444),
                Uuid::new(0x5555, 0x6666),
                Uuid::new(0x7777, 0x8888),
            ],
        ),
        (
            "Zero UUID",
            vec![Uuid::new(0, 0), Uuid::new(0x1111, 0x2222), Uuid::new(0, 0)],
        ),
        (
            "Max UUIDs",
            vec![Uuid::new(u64::MAX, u64::MAX), Uuid::new(u64::MAX, u64::MAX)],
        ),
    ];

    let mut block = Block::new();

    let mut id_col =
        clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
    let lc_type = Type::lowcardinality(Type::uuid());
    let mut lc_col = ColumnLowCardinality::new(lc_type);

    for (idx, (_desc, values)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);

        for uuid in values {
            lc_col
                .append_unsafe(&uuid_to_column_value(*uuid))
                .expect("Failed to append");
        }
    }

    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append id column");
    block
        .append_column("value", Arc::new(lc_col))
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

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_lowcardinality_uuid_block_insert_high_cardinality() {
    let (mut client, db_name) =
        create_isolated_test_client("lowcardinality_uuid_block_high_card")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value LowCardinality(UUID)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let lc_type = Type::lowcardinality(Type::uuid());
    let mut lc_col = ColumnLowCardinality::new(lc_type);

    // Create many entries with few unique UUIDs
    let uuids = vec![
        Uuid::new(0x1111, 0x2222),
        Uuid::new(0x3333, 0x4444),
        Uuid::new(0x5555, 0x6666),
        Uuid::new(0x7777, 0x8888),
        Uuid::new(0x9999, 0xaaaa),
    ];

    for i in 0..100 {
        let uuid = uuids[i % uuids.len()];
        lc_col
            .append_unsafe(&uuid_to_column_value(uuid))
            .expect("Failed to append");
    }

    block
        .append_column("value", Arc::new(lc_col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT value FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 100);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnLowCardinality>()
        .expect("Invalid column type");

    // Dictionary should have only 5 unique values despite 100 rows
    assert_eq!(result_col.dictionary_size(), 5);

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5))]

    #[test]
    #[ignore]
    fn test_lowcardinality_uuid_block_insert_random(
        indices in prop::collection::vec(0usize..5, 10..50)
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) =
                create_isolated_test_client("lowcardinality_uuid_block_random")
                    .await
                    .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value LowCardinality(UUID)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let uuids = vec![
                Uuid::new(0x1111, 0x2222),
                Uuid::new(0x3333, 0x4444),
                Uuid::new(0x5555, 0x6666),
                Uuid::new(0x7777, 0x8888),
                Uuid::new(0x9999, 0xaaaa),
            ];

            let mut block = Block::new();

            let mut id_col =
                clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
            let lc_type = Type::lowcardinality(Type::uuid());
            let mut lc_col = ColumnLowCardinality::new(lc_type);

            for (idx, uuid_idx) in indices.iter().enumerate() {
                id_col.append(idx as u32);
                let uuid = uuids[*uuid_idx];
                lc_col
                    .append_unsafe(&uuid_to_column_value(uuid))
                    .expect("Failed to append");
            }

            block
                .append_column("id", Arc::new(id_col))
                .expect("Failed to append id column");
            block
                .append_column("value", Arc::new(lc_col))
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

            assert_eq!(result.total_rows(), indices.len());
            let result_col = result.blocks()[0]
                .column(0)
                .expect("Column not found")
                .as_any()
                .downcast_ref::<ColumnLowCardinality>()
                .expect("Invalid column type");

            // Dictionary should have at most 5 unique values
            assert!(result_col.dictionary_size() <= 5);

            cleanup_test_database(&db_name).await;
        });
    }
}
