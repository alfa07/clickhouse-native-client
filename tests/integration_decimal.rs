/// Integration tests for Decimal types
mod common;

use clickhouse_native_client::{
    column::decimal::ColumnDecimal,
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
async fn test_decimal_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("decimal_roundtrip")
            .await
            .expect("Failed to create test client");

    let precision = 10;
    let scale = 2;

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value Decimal({}, {})) ENGINE = Memory",
            db_name, precision, scale
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnDecimal::new(Type::decimal(precision, scale));

    // Test values: store as i128 (scaled by 10^scale)
    col.append(0); // 0.00
    col.append(12345); // 123.45
    col.append(-67890); // -678.90
    col.append(1); // 0.01
    col.append(-1); // -0.01
    col.append(9999999999); // 99999999.99 (near max for Decimal(10,2))

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

    assert_eq!(result.total_rows(), 6);
    let result_block = &result.blocks()[0];
    let col_ref = result_block.column(0).expect("Column not found");

    let result_col = col_ref
        .as_any()
        .downcast_ref::<ColumnDecimal>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), -67890);
    assert_eq!(result_col.at(1), -1);
    assert_eq!(result_col.at(2), 0);
    assert_eq!(result_col.at(3), 1);
    assert_eq!(result_col.at(4), 12345);
    assert_eq!(result_col.at(5), 9999999999);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_decimal_various_scales() {
    for (precision, scale) in [(9, 0), (18, 4), (38, 9)] {
        let (mut client, db_name) = create_isolated_test_client(&format!(
            "decimal_{}_{}",
            precision, scale
        ))
        .await
        .expect("Failed to create test client");

        client
            .query(format!(
                "CREATE TABLE {}.test_table (value Decimal({}, {})) ENGINE = Memory",
                db_name, precision, scale
            ))
            .await
            .expect("Failed to create table");

        let mut block = Block::new();
        let mut col = ColumnDecimal::new(Type::decimal(precision, scale));

        let scale_factor = 10i128.pow(scale as u32);
        col.append(0);
        col.append(123 * scale_factor);
        col.append(-456 * scale_factor);

        block
            .append_column("value", Arc::new(col))
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

        cleanup_test_database(&db_name).await;
    }
}
