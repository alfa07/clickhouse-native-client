/// Integration tests for Nullable compound types
/// Tests: Nullable(String), Nullable(Array(IPv6)), Nullable(Tuple(IPv6, IPv4))
mod common;

use clickhouse_client::{
    column::{
        array::ColumnArray,
        ipv4::ColumnIpv4,
        ipv6::ColumnIpv6,
        nullable::ColumnNullable,
        string::ColumnString,
        tuple::ColumnTuple,
    },
    types::Type,
    Block,
};
use common::{
    cleanup_test_database,
    create_isolated_test_client,
};
use std::{
    net::{
        Ipv4Addr,
        Ipv6Addr,
    },
    sync::Arc,
};

// ============================================================================
// Nullable(String)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_nullable_string_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("nullable_string")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (text Nullable(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();

    let nested = Arc::new(ColumnString::new(Type::string()));
    let mut col = ColumnNullable::with_nested(nested);

    // NULL value
    col.append_null();
    {
        let string_col = col.nested_mut::<ColumnString>();
        string_col.append("".to_string()); // Placeholder
    }

    // Non-NULL value
    col.append_non_null();
    {
        let string_col = col.nested_mut::<ColumnString>();
        string_col.append("hello".to_string());
    }

    // NULL value
    col.append_null();
    {
        let string_col = col.nested_mut::<ColumnString>();
        string_col.append("".to_string()); // Placeholder
    }

    // Non-NULL value
    col.append_non_null();
    {
        let string_col = col.nested_mut::<ColumnString>();
        string_col.append("world".to_string());
    }

    block
        .append_column("text", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT text FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);

    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnNullable>()
        .expect("Invalid column type");

    assert!(result_col.is_null(0));
    assert!(!result_col.is_null(1));
    assert!(result_col.is_null(2));
    assert!(!result_col.is_null(3));

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_nullable_all_nulls() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_all_nulls")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Nullable(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Insert all NULLs via SQL
    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES (NULL), (NULL), (NULL)",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT value FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnNullable>()
        .expect("Invalid column type");

    assert!(result_col.is_null(0));
    assert!(result_col.is_null(1));
    assert!(result_col.is_null(2));

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Nullable with empty values tests
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_nullable_empty_string() {
    let (mut client, db_name) =
        create_isolated_test_client("nullable_empty_string")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (text Nullable(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Insert empty string (NOT NULL) and NULL
    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES (''), (NULL), ('test')",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT text FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnNullable>()
        .expect("Invalid column type");

    // First is empty string (not null)
    assert!(!result_col.is_null(0));
    // Second is NULL
    assert!(result_col.is_null(1));
    // Third is 'test' (not null)
    assert!(!result_col.is_null(2));

    cleanup_test_database(&db_name).await;
}
