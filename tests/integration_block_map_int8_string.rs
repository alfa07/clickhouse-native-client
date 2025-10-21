/// Integration tests for Map(Int8, String) column using Block insertion
mod common;

use clickhouse_client::{
    column::{
        numeric::ColumnInt8,
        string::ColumnString,
        ColumnArray,
        ColumnMap,
        ColumnTuple,
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
async fn test_map_int8_string_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("map_int8_string_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Map(Int8, String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create Map column (stored as Array(Tuple(Int8, String)))
    let map_type = Type::Map {
        key_type: Box::new(Type::int8()),
        value_type: Box::new(Type::string()),
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

    // Add first map: {1: "one", 2: "two"}
    tuple_mut
        .column_at_mut(0)
        .as_any_mut()
        .downcast_mut::<ColumnInt8>()
        .expect("First column should be ColumnInt8")
        .append(1);
    tuple_mut
        .column_at_mut(1)
        .as_any_mut()
        .downcast_mut::<ColumnString>()
        .expect("Second column should be ColumnString")
        .append("one");
    tuple_mut
        .column_at_mut(0)
        .as_any_mut()
        .downcast_mut::<ColumnInt8>()
        .expect("First column should be ColumnInt8")
        .append(2);
    tuple_mut
        .column_at_mut(1)
        .as_any_mut()
        .downcast_mut::<ColumnString>()
        .expect("Second column should be ColumnString")
        .append("two");
    array_mut.append_len(2);

    // Add second map: {3: "three"}
    tuple_mut
        .column_at_mut(0)
        .as_any_mut()
        .downcast_mut::<ColumnInt8>()
        .expect("First column should be ColumnInt8")
        .append(3);
    tuple_mut
        .column_at_mut(1)
        .as_any_mut()
        .downcast_mut::<ColumnString>()
        .expect("Second column should be ColumnString")
        .append("three");
    array_mut.append_len(1);

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

    assert_eq!(result.total_rows(), 2);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnMap>()
        .expect("Invalid column type");

    let result_array =
        result_col.as_array().expect("Map should have underlying array");
    assert_eq!(result_array.get_array_len(0), Some(2));
    assert_eq!(result_array.get_array_len(1), Some(1));

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_map_int8_string_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("map_int8_string_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Map(Int8, String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases: Vec<(&str, Vec<(i8, &str)>)> = vec![
        ("Empty map", vec![]),
        ("Single entry", vec![(1, "test")]),
        ("Multiple entries", vec![(1, "a"), (2, "b"), (3, "c")]),
        ("Min/Max keys", vec![(i8::MIN, "min"), (i8::MAX, "max")]),
        ("Unicode values", vec![(1, "Hello 世界")]),
    ];

    for (idx, (_desc, entries)) in test_cases.iter().enumerate() {
        let mut block = Block::new();

        let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
            Type::uint32(),
        );
        id_col.append(idx as u32);

        let map_type = Type::Map {
            key_type: Box::new(Type::int8()),
            value_type: Box::new(Type::string()),
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

        for (key, value) in entries {
            tuple_mut
                .column_at_mut(0)
                .as_any_mut()
                .downcast_mut::<ColumnInt8>()
                .expect("First column should be ColumnInt8")
                .append(*key);
            tuple_mut
                .column_at_mut(1)
                .as_any_mut()
                .downcast_mut::<ColumnString>()
                .expect("Second column should be ColumnString")
                .append(*value);
        }
        array_mut.append_len(entries.len() as u64);

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
    }

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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5))]

    #[test]
    #[ignore]
    fn test_map_int8_string_block_insert_random(
        values in prop::collection::vec(
            prop::collection::vec((any::<i8>(), ".*"), 0..5),
            1..10
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) =
                create_isolated_test_client("map_int8_string_block_random")
                    .await
                    .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Map(Int8, String)) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            for (idx, entries) in values.iter().enumerate() {
                let mut block = Block::new();

                let mut id_col =
                    clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
                id_col.append(idx as u32);

                let map_type = Type::Map {
                    key_type: Box::new(Type::int8()),
                    value_type: Box::new(Type::string()),
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

                for (key, value) in entries {
                    tuple_mut.column_at_mut(0)
                        .as_any_mut()
                        .downcast_mut::<ColumnInt8>()
                        .expect("First column should be ColumnInt8")
                        .append(*key);
                    tuple_mut.column_at_mut(1)
                        .as_any_mut()
                        .downcast_mut::<ColumnString>()
                        .expect("Second column should be ColumnString")
                        .append(value.as_str());
                }
                array_mut.append_len(entries.len() as u64);

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
            }

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
                .downcast_ref::<ColumnMap>()
                .expect("Invalid column type");

            let result_array = result_col
                .as_array()
                .expect("Map should have underlying array");

            for (idx, entries) in values.iter().enumerate() {
                assert_eq!(result_array.get_array_len(idx), Some(entries.len()));
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
