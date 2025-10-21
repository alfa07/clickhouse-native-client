/// Integration tests for Array(LowCardinality(String)) column using Block
/// insertion
mod common;

use clickhouse_client::{
    column::{
        array::ColumnArray,
        lowcardinality::ColumnLowCardinality,
        string::ColumnString,
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
async fn test_array_lowcardinality_string_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("array_lowcard_string_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (tags Array(LowCardinality(String))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create nested LowCardinality(String) column
    let inner = Arc::new(ColumnString::new(Type::string()));
    let mut nested = ColumnLowCardinality::with_inner(
        Type::low_cardinality(Type::string()),
        inner,
    );
    nested.append_string("tag1".to_string());
    nested.append_string("tag2".to_string());
    nested.append_string("tag1".to_string()); // Repeated tag
    nested.append_string("tag3".to_string());

    // Create Array column with offsets: [2, 4] for arrays [["tag1", "tag2"],
    // ["tag1", "tag3"]]
    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_offset(2); // First array has 2 elements
    col.append_offset(4); // Second array has 2 elements (total 4)

    block
        .append_column("tags", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT tags FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 2);

    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnArray>()
        .expect("Invalid column type");

    assert_eq!(result_col.size(), 2);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_array_lowcardinality_string_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("array_lowcard_string_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, tags Array(LowCardinality(String))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases: Vec<(&str, Vec<&str>)> = vec![
        ("Empty array", vec![]),
        ("Single element", vec!["tag1"]),
        ("Multiple unique tags", vec!["tag1", "tag2", "tag3"]),
        ("Repeated tags", vec!["tag1", "tag1", "tag1"]),
        (
            "Mixed repeated and unique",
            vec!["tag1", "tag2", "tag1", "tag3", "tag2"],
        ),
        ("Empty strings", vec!["", "", ""]),
        ("Unicode tags", vec!["タグ1", "标签2", "тег3"]),
    ];

    let mut block = Block::new();

    let mut id_col =
        clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
    let inner = Arc::new(ColumnString::new(Type::string()));
    let mut nested = ColumnLowCardinality::with_inner(
        Type::low_cardinality(Type::string()),
        inner,
    );

    for (idx, (_desc, values)) in test_cases.iter().enumerate() {
        id_col.append(idx as u32);

        for &val in values {
            nested.append_string(val.to_string());
        }
    }

    let mut array_col = ColumnArray::with_nested(Arc::new(nested));
    let mut cumulative = 0u64;
    for (_desc, values) in &test_cases {
        cumulative += values.len() as u64;
        array_col.append_offset(cumulative);
    }

    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append id column");
    block
        .append_column("tags", Arc::new(array_col))
        .expect("Failed to append tags column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT tags FROM {}.test_table ORDER BY id", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), test_cases.len());

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_array_lowcardinality_string_block_insert_many_elements() {
    let (mut client, db_name) =
        create_isolated_test_client("array_lowcard_string_block_many")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (tags Array(LowCardinality(String))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    // Create a large array with many repeated tags (good for LowCardinality
    // compression)
    let inner = Arc::new(ColumnString::new(Type::string()));
    let mut nested = ColumnLowCardinality::with_inner(
        Type::low_cardinality(Type::string()),
        inner,
    );

    let tags = vec!["tag1", "tag2", "tag3", "tag4", "tag5"];
    for i in 0..1000 {
        nested.append_string(tags[i % tags.len()].to_string());
    }

    let mut col = ColumnArray::with_nested(Arc::new(nested));
    col.append_offset(1000);

    block
        .append_column("tags", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT tags FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 1);

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_array_lowcardinality_string_block_insert_random(
        arrays in prop::collection::vec(
            prop::collection::vec("tag[0-9]{1,2}", 0..20),  // Generate tags like tag0, tag1, ..., tag99
            1..10
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("array_lowcard_string_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, tags Array(LowCardinality(String))) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
                Type::uint32()
            );

            let inner = Arc::new(ColumnString::new(Type::string()));
            let mut nested = ColumnLowCardinality::with_inner(
                Type::low_cardinality(Type::string()),
                inner
            );

            for (idx, array) in arrays.iter().enumerate() {
                id_col.append(idx as u32);
                for val in array {
                    nested.append_string(val.clone());
                }
            }

            let mut array_col = ColumnArray::with_nested(Arc::new(nested));
            let mut cumulative = 0u64;
            for array in &arrays {
                cumulative += array.len() as u64;
                array_col.append_offset(cumulative);
            }

            block
                .append_column("id", Arc::new(id_col))
                .expect("Failed to append id column");
            block
                .append_column("tags", Arc::new(array_col))
                .expect("Failed to append tags column");

            client
                .insert(&format!("{}.test_table", db_name), block)
                .await
                .expect("Failed to insert block");

            let result = client
                .query(format!(
                    "SELECT tags FROM {}.test_table ORDER BY id",
                    db_name
                ))
                .await
                .expect("Failed to select");

            assert_eq!(result.total_rows(), arrays.len());

            cleanup_test_database(&db_name).await;
        });
    }
}
