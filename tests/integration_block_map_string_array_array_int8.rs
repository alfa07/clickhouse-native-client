/// Integration tests for Map(String, Array(Array(Int8))) column using Block
/// insertion
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
use std::sync::Arc;

#[tokio::test]
#[ignore]
async fn test_map_string_array_array_int8_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("map_string_array_array_int8_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Map(String, Array(Array(Int8)))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create Map column (stored as Array(Tuple(String, Array(Array(Int8)))))
    let map_type = Type::Map {
        key_type: Box::new(Type::string()),
        value_type: Box::new(Type::array(Type::array(Type::int8()))),
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

    // Add first map entry: "key1" -> [[1, 2], [3]]
    tuple_mut
        .column_at_mut(0)
        .as_any_mut()
        .downcast_mut::<ColumnString>()
        .expect("First column should be ColumnString")
        .append("key1");

    // Create Array(Array(Int8)) for value
    let outer_array_col = tuple_mut
        .column_at_mut(1)
        .as_any_mut()
        .downcast_mut::<ColumnArray>()
        .expect("Second column should be ColumnArray");

    let inner_array = Arc::get_mut(outer_array_col.nested_mut())
        .expect("Failed to get mutable reference to inner array");
    let inner_array_mut = inner_array
        .as_any_mut()
        .downcast_mut::<ColumnArray>()
        .expect("Inner should be ColumnArray");

    let int8_col = Arc::get_mut(inner_array_mut.nested_mut())
        .expect("Failed to get mutable reference to int8 column");
    let int8_col_mut = int8_col
        .as_any_mut()
        .downcast_mut::<ColumnInt8>()
        .expect("Innermost should be ColumnInt8");

    // [[1, 2], [3]]
    int8_col_mut.append(1);
    int8_col_mut.append(2);
    inner_array_mut.append_len(2); // [1, 2]
    int8_col_mut.append(3);
    inner_array_mut.append_len(1); // [3]
    outer_array_col.append_len(2); // [[1, 2], [3]]

    array_mut.append_len(1); // One map entry

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
    assert_eq!(result_array.get_array_len(0), Some(1)); // One key-value pair

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_map_string_array_array_int8_block_insert_boundary() {
    let (mut client, db_name) = create_isolated_test_client(
        "map_string_array_array_int8_block_boundary",
    )
    .await
    .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Map(String, Array(Array(Int8)))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Test case: empty map
    let mut block = Block::new();
    let mut id_col =
        clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
    id_col.append(0);

    let map_type = Type::Map {
        key_type: Box::new(Type::string()),
        value_type: Box::new(Type::array(Type::array(Type::int8()))),
    };

    let mut map_col = ColumnMap::new(map_type.clone());
    let array = Arc::get_mut(&mut map_col.data)
        .expect("Failed to get mutable reference to map data");
    let array_mut = array
        .as_any_mut()
        .downcast_mut::<ColumnArray>()
        .expect("Map data should be ColumnArray");
    array_mut.append_len(0); // Empty map

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

    // Test case: map with empty arrays
    let mut block = Block::new();
    let mut id_col =
        clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
    id_col.append(1);

    let mut map_col = ColumnMap::new(map_type.clone());
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

    tuple_mut
        .column_at_mut(0)
        .as_any_mut()
        .downcast_mut::<ColumnString>()
        .expect("First column should be ColumnString")
        .append("empty");

    let outer_array_col = tuple_mut
        .column_at_mut(1)
        .as_any_mut()
        .downcast_mut::<ColumnArray>()
        .expect("Second column should be ColumnArray");
    outer_array_col.append_len(0); // Empty array value

    array_mut.append_len(1); // One map entry

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

    assert_eq!(result.total_rows(), 2);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnMap>()
        .expect("Invalid column type");

    let result_array =
        result_col.as_array().expect("Map should have underlying array");
    assert_eq!(result_array.get_array_len(0), Some(0)); // Empty map
    assert_eq!(result_array.get_array_len(1), Some(1)); // One entry

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_map_string_array_array_int8_block_insert_multiple_entries() {
    let (mut client, db_name) = create_isolated_test_client(
        "map_string_array_array_int8_block_multiple",
    )
    .await
    .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Map(String, Array(Array(Int8)))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let map_type = Type::Map {
        key_type: Box::new(Type::string()),
        value_type: Box::new(Type::array(Type::array(Type::int8()))),
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

    // Add two map entries
    for (key_name, arrays) in [
        ("first", vec![vec![1i8, 2i8], vec![3i8]]),
        ("second", vec![vec![4i8]]),
    ] {
        tuple_mut
            .column_at_mut(0)
            .as_any_mut()
            .downcast_mut::<ColumnString>()
            .expect("First column should be ColumnString")
            .append(key_name);

        let outer_array_col = tuple_mut
            .column_at_mut(1)
            .as_any_mut()
            .downcast_mut::<ColumnArray>()
            .expect("Second column should be ColumnArray");

        let inner_array = Arc::get_mut(outer_array_col.nested_mut())
            .expect("Failed to get mutable reference to inner array");
        let inner_array_mut = inner_array
            .as_any_mut()
            .downcast_mut::<ColumnArray>()
            .expect("Inner should be ColumnArray");

        let int8_col = Arc::get_mut(inner_array_mut.nested_mut())
            .expect("Failed to get mutable reference to int8 column");
        let int8_col_mut = int8_col
            .as_any_mut()
            .downcast_mut::<ColumnInt8>()
            .expect("Innermost should be ColumnInt8");

        for inner_arr in arrays {
            for val in inner_arr {
                int8_col_mut.append(val);
            }
            inner_array_mut.append_len(inner_arr.len() as u64);
        }
        outer_array_col.append_len(arrays.len() as u64);
    }

    array_mut.append_len(2); // Two map entries

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
    assert_eq!(result_array.get_array_len(0), Some(2)); // Two key-value pairs

    cleanup_test_database(&db_name).await;
}
