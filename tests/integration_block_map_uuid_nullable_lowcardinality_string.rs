/// Integration tests for Map(UUID, Nullable(LowCardinality(String))) column
/// using Block insertion
mod common;

use clickhouse_client::{
    column::{
        column_value::ColumnValue,
        uuid::Uuid,
        ColumnArray,
        ColumnLowCardinality,
        ColumnMap,
        ColumnNullable,
        ColumnTuple,
        ColumnUuid,
    },
    types::Type,
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};
use std::sync::Arc;

#[tokio::test]
#[ignore]
async fn test_map_uuid_nullable_lowcardinality_string_block_insert_basic() {
    let (mut client, db_name) = create_isolated_test_client(
        "map_uuid_nullable_lowcardinality_string_block_basic",
    )
    .await
    .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Map(UUID, Nullable(LowCardinality(String)))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create Map column
    let map_type = Type::Map {
        key_type: Box::new(Type::uuid()),
        value_type: Box::new(Type::nullable(Type::lowcardinality(
            Type::string(),
        ))),
    };

    let mut map_col = ColumnMap::new(map_type);

    // Get underlying array and populate it
    let array = Arc::get_mut(&mut map_col.data)
        .expect("Failed to get mutable reference to map data");
    let array_mut = array
        .as_any_mut()
        .downcast_mut::<ColumnArray>()
        .expect("Map data should be ColumnArray");

    // Get the tuple nested in the array
    let tuple = Arc::get_mut(array_mut.nested_mut())
        .expect("Failed to get mutable reference to nested");
    let tuple_mut = tuple
        .as_any_mut()
        .downcast_mut::<ColumnTuple>()
        .expect("Nested should be ColumnTuple");

    // Add first map entry: UUID1 -> "status_active"
    tuple_mut
        .column_at_mut(0)
        .as_any_mut()
        .downcast_mut::<ColumnUuid>()
        .expect("First column should be ColumnUuid")
        .append(Uuid::new(0x1234567890abcdef, 0xfedcba0987654321));

    let nullable_col = tuple_mut
        .column_at_mut(1)
        .as_any_mut()
        .downcast_mut::<ColumnNullable>()
        .expect("Second column should be ColumnNullable");
    nullable_col.append_non_null();

    let lc_nested = Arc::get_mut(nullable_col.nested_mut())
        .expect("Failed to get mutable reference to nested");
    let lc_col = lc_nested
        .as_any_mut()
        .downcast_mut::<ColumnLowCardinality>()
        .expect("Nested should be ColumnLowCardinality");
    lc_col
        .append_unsafe(&ColumnValue::from_string("status_active"))
        .expect("Failed to append");

    // Add second map entry: UUID2 -> NULL
    tuple_mut
        .column_at_mut(0)
        .as_any_mut()
        .downcast_mut::<ColumnUuid>()
        .expect("First column should be ColumnUuid")
        .append(Uuid::new(0xabcdef1234567890, 0x0987654321fedcba));

    let nullable_col = tuple_mut
        .column_at_mut(1)
        .as_any_mut()
        .downcast_mut::<ColumnNullable>()
        .expect("Second column should be ColumnNullable");
    nullable_col.append_null();

    // Add third map entry: UUID3 -> "status_inactive"
    tuple_mut
        .column_at_mut(0)
        .as_any_mut()
        .downcast_mut::<ColumnUuid>()
        .expect("First column should be ColumnUuid")
        .append(Uuid::new(0x1111222233334444, 0x5555666677778888));

    let nullable_col = tuple_mut
        .column_at_mut(1)
        .as_any_mut()
        .downcast_mut::<ColumnNullable>()
        .expect("Second column should be ColumnNullable");
    nullable_col.append_non_null();

    let lc_nested = Arc::get_mut(nullable_col.nested_mut())
        .expect("Failed to get mutable reference to nested");
    let lc_col = lc_nested
        .as_any_mut()
        .downcast_mut::<ColumnLowCardinality>()
        .expect("Nested should be ColumnLowCardinality");
    lc_col
        .append_unsafe(&ColumnValue::from_string("status_inactive"))
        .expect("Failed to append");

    array_mut.append_len(3); // Three map entries

    block
        .append_column("value", Arc::new(map_col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT value FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 1);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnMap>()
        .expect("Invalid column type");

    let result_array =
        result_col.as_array().expect("Map should have underlying array");
    assert_eq!(result_array.get_array_len(0), Some(3)); // Three key-value pairs

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_map_uuid_nullable_lowcardinality_string_block_insert_boundary() {
    let (mut client, db_name) = create_isolated_test_client(
        "map_uuid_nullable_lowcardinality_string_block_boundary",
    )
    .await
    .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Map(UUID, Nullable(LowCardinality(String)))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases: Vec<(&str, Vec<(Uuid, Option<&str>)>)> = vec![
        ("Empty map", vec![]),
        ("Single non-null", vec![(Uuid::new(0x1111, 0x2222), Some("active"))]),
        ("Single null", vec![(Uuid::new(0x3333, 0x4444), None)]),
        (
            "Repeated values (low cardinality)",
            vec![
                (Uuid::new(0x5555, 0x6666), Some("status1")),
                (Uuid::new(0x7777, 0x8888), Some("status1")), // Repeated
                (Uuid::new(0x9999, 0xaaaa), Some("status2")),
                (Uuid::new(0xbbbb, 0xcccc), Some("status1")), // Repeated
            ],
        ),
        (
            "Mixed null/non-null",
            vec![
                (Uuid::new(0xdddd, 0xeeee), None),
                (Uuid::new(0xffff, 0x0000), Some("value")),
                (Uuid::new(0x1234, 0x5678), None),
            ],
        ),
    ];

    let mut block = Block::new();

    let mut id_col =
        clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());

    let map_type = Type::Map {
        key_type: Box::new(Type::uuid()),
        value_type: Box::new(Type::nullable(Type::lowcardinality(
            Type::string(),
        ))),
    };

    let mut map_col = ColumnMap::new(map_type);

    let array = Arc::get_mut(&mut map_col.data)
        .expect("Failed to get mutable reference to map data");
    let array_mut = array
        .as_any_mut()
        .downcast_mut::<ColumnArray>()
        .expect("Map data should be ColumnArray");

    let tuple = Arc::get_mut(array_mut.nested_mut())
        .expect("Failed to get mutable reference to nested");
    let tuple_mut = tuple
        .as_any_mut()
        .downcast_mut::<ColumnTuple>()
        .expect("Nested should be ColumnTuple");

    for (idx, (_desc, entries)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);

        for (uuid, value_opt) in entries {
            tuple_mut
                .column_at_mut(0)
                .as_any_mut()
                .downcast_mut::<ColumnUuid>()
                .expect("First column should be ColumnUuid")
                .append(*uuid);

            let nullable_col = tuple_mut
                .column_at_mut(1)
                .as_any_mut()
                .downcast_mut::<ColumnNullable>()
                .expect("Second column should be ColumnNullable");

            match value_opt {
                Some(value) => {
                    nullable_col.append_non_null();
                    let lc_nested = Arc::get_mut(nullable_col.nested_mut())
                        .expect("Failed to get mutable reference to nested");
                    let lc_col = lc_nested
                        .as_any_mut()
                        .downcast_mut::<ColumnLowCardinality>()
                        .expect("Nested should be ColumnLowCardinality");
                    lc_col
                        .append_unsafe(&ColumnValue::from_string(value))
                        .expect("Failed to append");
                }
                None => {
                    nullable_col.append_null();
                }
            }
        }
        array_mut.append_len(entries.len() as u64);
    }

    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append id column");
    block
        .append_column("value", Arc::new(map_col))
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
        .downcast_ref::<ColumnMap>()
        .expect("Invalid column type");

    let result_array =
        result_col.as_array().expect("Map should have underlying array");

    for (idx, (_desc, entries)) in test_cases.iter().enumerate() {
        assert_eq!(result_array.get_array_len(idx), Some(entries.len()));
    }

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_map_uuid_nullable_lowcardinality_string_block_insert_high_cardinality(
) {
    let (mut client, db_name) = create_isolated_test_client(
        "map_uuid_nullable_lowcardinality_string_block_high_card",
    )
    .await
    .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Map(UUID, Nullable(LowCardinality(String)))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let map_type = Type::Map {
        key_type: Box::new(Type::uuid()),
        value_type: Box::new(Type::nullable(Type::lowcardinality(
            Type::string(),
        ))),
    };

    let mut map_col = ColumnMap::new(map_type);

    let array = Arc::get_mut(&mut map_col.data)
        .expect("Failed to get mutable reference to map data");
    let array_mut = array
        .as_any_mut()
        .downcast_mut::<ColumnArray>()
        .expect("Map data should be ColumnArray");

    let tuple = Arc::get_mut(array_mut.nested_mut())
        .expect("Failed to get mutable reference to nested");
    let tuple_mut = tuple
        .as_any_mut()
        .downcast_mut::<ColumnTuple>()
        .expect("Nested should be ColumnTuple");

    // Create many entries with few unique values (ideal for LowCardinality)
    let statuses = vec!["active", "inactive", "pending", "archived"];
    for i in 0..20 {
        let uuid = Uuid::new(i as u64, (i * 2) as u64);
        let status = statuses[i % statuses.len()];

        tuple_mut
            .column_at_mut(0)
            .as_any_mut()
            .downcast_mut::<ColumnUuid>()
            .expect("First column should be ColumnUuid")
            .append(uuid);

        let nullable_col = tuple_mut
            .column_at_mut(1)
            .as_any_mut()
            .downcast_mut::<ColumnNullable>()
            .expect("Second column should be ColumnNullable");

        if i % 5 == 0 {
            // Every 5th entry is NULL
            nullable_col.append_null();
        } else {
            nullable_col.append_non_null();
            let lc_nested = Arc::get_mut(nullable_col.nested_mut())
                .expect("Failed to get mutable reference to nested");
            let lc_col = lc_nested
                .as_any_mut()
                .downcast_mut::<ColumnLowCardinality>()
                .expect("Nested should be ColumnLowCardinality");
            lc_col
                .append_unsafe(&ColumnValue::from_string(status))
                .expect("Failed to append");
        }
    }
    array_mut.append_len(20);

    block
        .append_column("value", Arc::new(map_col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT value FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 1);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnMap>()
        .expect("Invalid column type");

    let result_array =
        result_col.as_array().expect("Map should have underlying array");
    assert_eq!(result_array.get_array_len(0), Some(20));

    cleanup_test_database(&db_name).await;
}
