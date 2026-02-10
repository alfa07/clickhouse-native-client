/// Integration tests for IPv6 column using Block insertion
mod common;

use clickhouse_native_client::{
    column::ipv6::ColumnIpv6,
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
async fn test_ipv6_block_insert_basic() {
    let (mut client, db_name) =
        create_isolated_test_client("ipv6_block_basic")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (value IPv6) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = ColumnIpv6::new(Type::ipv6());

    // ::1 (localhost)
    let mut addr1 = [0u8; 16];
    addr1[15] = 1;
    col.append(addr1);

    // ::ffff:192.168.0.1 (IPv4-mapped)
    let mut addr2 = [0u8; 16];
    addr2[10] = 0xff;
    addr2[11] = 0xff;
    addr2[12] = 192;
    addr2[13] = 168;
    addr2[14] = 0;
    addr2[15] = 1;
    col.append(addr2);

    // 2001:db8::1
    let mut addr3 = [0u8; 16];
    addr3[0] = 0x20;
    addr3[1] = 0x01;
    addr3[2] = 0x0d;
    addr3[3] = 0xb8;
    addr3[15] = 1;
    col.append(addr3);

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
    let blocks = result.blocks();
    let block = &blocks[0];
    let column = block.column(0).expect("Column not found");
    let result_col = column
        .as_any()
        .downcast_ref::<ColumnIpv6>()
        .expect("Invalid column type");

    assert_eq!(result_col.at(0), addr1);
    assert_eq!(result_col.at(1), addr2);
    assert_eq!(result_col.at(2), addr3);

    cleanup_test_database(&db_name).await;
}

#[tokio::test]
#[ignore]
async fn test_ipv6_block_insert_boundary() {
    let (mut client, db_name) =
        create_isolated_test_client("ipv6_block_boundary")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (id UInt32, value IPv6) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = [
        ("Min value all zeros", [0u8; 16]),
        ("Max value all ones", [0xFFu8; 16]),
        ("Localhost ::1", {
            let mut addr = [0u8; 16];
            addr[15] = 1;
            addr
        }),
        ("Test value", {
            let mut addr = [0u8; 16];
            for i in 0..16 {
                addr[i] = i as u8;
            }
            addr
        }),
    ];

    let mut block = Block::new();
    let mut id_col = clickhouse_native_client::column::numeric::ColumnUInt32::new();
    let mut val_col = ColumnIpv6::new(Type::ipv6());

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
    let block = &blocks[0];
    let column = block.column(0).expect("Column not found");
    let result_col = column
        .as_any()
        .downcast_ref::<ColumnIpv6>()
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
    fn test_ipv6_block_insert_random(values in prop::collection::vec(prop::array::uniform16(any::<u8>()), 1..100)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (mut client, db_name) = create_isolated_test_client("ipv6_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {}.test_table (id UInt32, value IPv6) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_native_client::column::numeric::ColumnUInt32::new();
            let mut val_col = ColumnIpv6::new(Type::ipv6());

            for (idx, value) in values.iter().enumerate() {
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
                .query(format!(
                    "SELECT value FROM {}.test_table ORDER BY id",
                    db_name
                ))
                .await
                .expect("Failed to select");

            assert_eq!(result.total_rows(), values.len());
            let blocks = result.blocks();
            let block = &blocks[0];
            let column = block.column(0).expect("Column not found");
            let result_col = column
                .as_any()
                .downcast_ref::<ColumnIpv6>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {
                assert_eq!(result_col.at(idx), *expected);
            }

            cleanup_test_database(&db_name).await;
        });
    }
}
