/// Integration tests for Tuple(String, Int64, Array(String)) column using
/// Block insertion
mod common;

use clickhouse_client::{
    column::{
        numeric::ColumnInt64,
        string::ColumnString,
        ColumnArray,
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
async fn test_tuple_string_int64_array_string_block_insert_basic() {
    let (mut client, db_name) = create_isolated_test_client(
        "tuple_string_int64_array_string_block_basic",
    )
    .await
    .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Tuple(String, Int64, Array(String))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let mut col1 = ColumnString::new(Type::string());
    col1.append("first");
    col1.append("second");

    let mut col2 = ColumnInt64::new(Type::int64());
    col2.append(100);
    col2.append(200);

    let mut array_col = ColumnArray::new(Type::array(Type::string()));
    let mut nested = ColumnString::new(Type::string());
    nested.append("a");
    nested.append("b");
    array_col.append_len(2);
    nested.append("c");
    array_col.append_len(1);
    *array_col.nested_mut() = Arc::new(nested);

    let tuple_type = Type::Tuple {
        item_types: vec![
            Type::string(),
            Type::int64(),
            Type::array(Type::string()),
        ],
    };
    let tuple_col = ColumnTuple::new(
        tuple_type,
        vec![Arc::new(col1), Arc::new(col2), Arc::new(array_col)],
    );

    block
        .append_column("value", Arc::new(tuple_col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!(
            "SELECT value FROM {}.test_table ORDER BY value.2",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 2);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<ColumnTuple>()
        .expect("Invalid column type");

    let result_col1 = result_col
        .column_at(0)
        .as_any()
        .downcast_ref::<ColumnString>()
        .expect("Invalid column type");
    let result_col2 = result_col
        .column_at(1)
        .as_any()
        .downcast_ref::<ColumnInt64>()
        .expect("Invalid column type");
    let result_col3 = result_col
        .column_at(2)
        .as_any()
        .downcast_ref::<ColumnArray>()
        .expect("Invalid column type");

    assert_eq!(result_col1.at(0), "first");
    assert_eq!(result_col2.at(0), 100);
    assert_eq!(result_col3.get_array_len(0), Some(2));

    assert_eq!(result_col1.at(1), "second");
    assert_eq!(result_col2.at(1), 200);
    assert_eq!(result_col3.get_array_len(1), Some(1));

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_tuple_string_int64_array_string_block_insert_boundary() {
    let (mut client, db_name) = create_isolated_test_client(
        "tuple_string_int64_array_string_block_boundary",
    )
    .await
    .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value Tuple(String, Int64, Array(String))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases: Vec<(&str, &str, i64, Vec<&str>)> = vec![
        ("Empty array", "", 0, vec![]),
        ("Single element", "test", 100, vec!["item"]),
        ("Multiple elements", "multi", 200, vec!["a", "b", "c"]),
        ("Empty strings", "empty", 300, vec!["", "", ""]),
        ("Unicode", "世界", 400, vec!["Hello", "世界"]),
    ];

    let mut block = Block::new();

    let mut id_col =
        clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
    let mut col1 = ColumnString::new(Type::string());
    let mut col2 = ColumnInt64::new(Type::int64());
    let mut nested = ColumnString::new(Type::string());
    let mut array_col = ColumnArray::new(Type::array(Type::string()));

    for (idx, (_desc, val1, val2, array_vals)) in test_cases.iter().enumerate()
    {
        id_col.append(idx as u32);
        col1.append(*val1);
        col2.append(*val2);

        for item in array_vals {
            nested.append(item);
        }
        array_col.append_len(array_vals.len() as u64);
    }

    *array_col.nested_mut() = Arc::new(nested);

    let tuple_type = Type::Tuple {
        item_types: vec![
            Type::string(),
            Type::int64(),
            Type::array(Type::string()),
        ],
    };
    let tuple_col = ColumnTuple::new(
        tuple_type,
        vec![Arc::new(col1), Arc::new(col2), Arc::new(array_col)],
    );

    block
        .append_column("id", Arc::new(id_col))
        .expect("Failed to append id column");
    block
        .append_column("value", Arc::new(tuple_col))
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
        .downcast_ref::<ColumnTuple>()
        .expect("Invalid column type");

    let result_col1 = result_col
        .column_at(0)
        .as_any()
        .downcast_ref::<ColumnString>()
        .expect("Invalid column type");
    let result_col2 = result_col
        .column_at(1)
        .as_any()
        .downcast_ref::<ColumnInt64>()
        .expect("Invalid column type");
    let result_col3 = result_col
        .column_at(2)
        .as_any()
        .downcast_ref::<ColumnArray>()
        .expect("Invalid column type");

    for (idx, (_desc, expected1, expected2, array_vals)) in
        test_cases.iter().enumerate()
    {
        assert_eq!(result_col1.at(idx), *expected1);
        assert_eq!(result_col2.at(idx), *expected2);
        assert_eq!(result_col3.get_array_len(idx), Some(array_vals.len()));
    }

    cleanup_test_database(&db_name).await;
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(5))]

    #[test]
    #[ignore]
    fn test_tuple_string_int64_array_string_block_insert_random(
        values in prop::collection::vec(
            (".*", any::<i64>(), prop::collection::vec(".*", 0..5)),
            1..20
        )
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client(
                "tuple_string_int64_array_string_block_random",
            )
            .await
            .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value Tuple(String, Int64, Array(String))) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col =
                clickhouse_client::column::numeric::ColumnUInt32::new(Type::uint32());
            let mut col1 = ColumnString::new(Type::string());
            let mut col2 = ColumnInt64::new(Type::int64());
            let mut array_col = ColumnArray::new(Type::array(Type::string()));
            let mut nested = ColumnString::new(Type::string());

            for (idx, (val1, val2, array_vals)) in values.iter().enumerate() {
                id_col.append(idx as u32);
                col1.append(val1.as_str());
                col2.append(*val2);

                for item in array_vals {
                    nested.append(item.as_str());
                }
                array_col.append_len(array_vals.len() as u64);
            }
            *array_col.nested_mut() = Arc::new(nested);

            let tuple_type = Type::Tuple {
                item_types: vec![Type::string(), Type::int64(), Type::array(Type::string())],
            };
            let tuple_col = ColumnTuple::new(
                tuple_type,
                vec![Arc::new(col1), Arc::new(col2), Arc::new(array_col)],
            );

            block
                .append_column("id", Arc::new(id_col))
                .expect("Failed to append id column");
            block
                .append_column("value", Arc::new(tuple_col))
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
                .downcast_ref::<ColumnTuple>()
                .expect("Invalid column type");

            let col1_arc = result_col.column_at(0);
            let result_col1 = col1_arc
                .as_any()
                .downcast_ref::<ColumnString>()
                .expect("Invalid column type");
            let col2_arc = result_col.column_at(1);
            let result_col2 = col2_arc
                .as_any()
                .downcast_ref::<ColumnInt64>()
                .expect("Invalid column type");
            let col3_arc = result_col.column_at(2);
            let result_col3 = col3_arc
                .as_any()
                .downcast_ref::<ColumnArray>()
                .expect("Invalid column type");

            for (idx, (expected1, expected2, array_vals)) in values.iter().enumerate() {
                assert_eq!(result_col1.at(idx), expected1.as_str());
                assert_eq!(result_col2.at(idx), *expected2);
                assert_eq!(result_col3.get_array_len(idx), Some(array_vals.len()));
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
