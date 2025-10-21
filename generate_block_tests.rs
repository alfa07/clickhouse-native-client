/// Code generator for integration_block_* test files
use std::fs;
use std::io::Write;

struct NumericType {
    rust_type: &'static str,
    type_lower: &'static str,
    column_type: &'static str,
    ch_type: &'static str,
    min_val: &'static str,
    max_val: &'static str,
    mid_val: &'static str,
    test_val: &'static str,
}

const NUMERIC_TYPES: &[NumericType] = &[
    NumericType {
        rust_type: "u8",
        type_lower: "uint8",
        column_type: "ColumnUInt8",
        ch_type: "UInt8",
        min_val: "0",
        max_val: "255",
        mid_val: "127",
        test_val: "42",
    },
    NumericType {
        rust_type: "u16",
        type_lower: "uint16",
        column_type: "ColumnUInt16",
        ch_type: "UInt16",
        min_val: "0",
        max_val: "65535",
        mid_val: "32767",
        test_val: "1000",
    },
    NumericType {
        rust_type: "u32",
        type_lower: "uint32",
        column_type: "ColumnUInt32",
        ch_type: "UInt32",
        min_val: "0",
        max_val: "4294967295",
        mid_val: "2147483647",
        test_val: "100000",
    },
    NumericType {
        rust_type: "u64",
        type_lower: "uint64",
        column_type: "ColumnUInt64",
        ch_type: "UInt64",
        min_val: "0",
        max_val: "18446744073709551615",
        mid_val: "9223372036854775807",
        test_val: "1000000000",
    },
    NumericType {
        rust_type: "i8",
        type_lower: "int8",
        column_type: "ColumnInt8",
        ch_type: "Int8",
        min_val: "-128",
        max_val: "127",
        mid_val: "0",
        test_val: "42",
    },
    NumericType {
        rust_type: "i16",
        type_lower: "int16",
        column_type: "ColumnInt16",
        ch_type: "Int16",
        min_val: "-32768",
        max_val: "32767",
        mid_val: "0",
        test_val: "1000",
    },
    NumericType {
        rust_type: "i32",
        type_lower: "int32",
        column_type: "ColumnInt32",
        ch_type: "Int32",
        min_val: "-2147483648",
        max_val: "2147483647",
        mid_val: "0",
        test_val: "100000",
    },
    NumericType {
        rust_type: "i64",
        type_lower: "int64",
        column_type: "ColumnInt64",
        ch_type: "Int64",
        min_val: "-9223372036854775808",
        max_val: "9223372036854775807",
        mid_val: "0",
        test_val: "1000000000",
    },
    NumericType {
        rust_type: "f32",
        type_lower: "float32",
        column_type: "ColumnFloat32",
        ch_type: "Float32",
        min_val: "f32::MIN",
        max_val: "f32::MAX",
        mid_val: "0.0",
        test_val: "3.14159",
    },
    NumericType {
        rust_type: "f64",
        type_lower: "float64",
        column_type: "ColumnFloat64",
        ch_type: "Float64",
        min_val: "f64::MIN",
        max_val: "f64::MAX",
        mid_val: "0.0",
        test_val: "3.141592653589793",
    },
];

fn generate_numeric_test(typ: &NumericType) -> String {
    let is_float = typ.rust_type == "f32" || typ.rust_type == "f64";
    let comparison = if is_float {
        "assert!((result_col.at(idx) - *expected).abs() < 1e-6);"
    } else {
        "assert_eq!(result_col.at(idx), *expected);"
    };

    format!(
        r#"/// Integration tests for {} column using Block insertion
mod common;

use clickhouse_client::{{
    column::numeric::{},
    types::Type,
    Block,
}};
use common::{{
    cleanup_test_database,
    create_isolated_test_client,
}};
use proptest::prelude::*;
use std::sync::Arc;

#[tokio::test]
#[ignore]
async fn test_{}_block_insert_basic() {{
    let (mut client, db_name) = create_isolated_test_client("{}_block_basic")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {{}}.test_table (value {}) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let mut block = Block::new();
    let mut col = {}::new(Type::{}());
    col.append({});
    col.append({});
    col.append({});
    block
        .append_column("value", Arc::new(col))
        .expect("Failed to append column");

    client
        .insert(&format!("{{}}.test_table", db_name), block)
        .await
        .expect("Failed to insert block");

    let result = client
        .query(format!("SELECT value FROM {{}}.test_table ORDER BY value", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<{}>()
        .expect("Invalid column type");

    let mut expected = vec![{}, {}, {}];
    expected.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    for (idx, exp) in expected.iter().enumerate() {{
        {}
    }}

    cleanup_test_database(&db_name).await;
}}

#[tokio::test]
#[ignore]
async fn test_{}_block_insert_boundary() {{
    let (mut client, db_name) = create_isolated_test_client("{}_block_boundary")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {{}}.test_table (id UInt32, value {}) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    let test_cases = vec![
        ("Min value", {}),
        ("Max value", {}),
        ("Mid value", {}),
        ("Test value", {}),
    ];

    for (idx, (_desc, value)) in test_cases.iter().enumerate() {{
        let mut block = Block::new();

        let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
            Type::uint32()
        );
        id_col.append(idx as u32);

        let mut val_col = {}::new(Type::{}());
        val_col.append(*value);

        block
            .append_column("id", Arc::new(id_col))
            .expect("Failed to append id column");
        block
            .append_column("value", Arc::new(val_col))
            .expect("Failed to append value column");

        client
            .insert(&format!("{{}}.test_table", db_name), block)
            .await
            .expect("Failed to insert block");
    }}

    let result = client
        .query(format!(
            "SELECT value FROM {{}}.test_table ORDER BY id",
            db_name
        ))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), test_cases.len());
    let result_col = result.blocks()[0]
        .column(0)
        .expect("Column not found")
        .as_any()
        .downcast_ref::<{}>()
        .expect("Invalid column type");

    for (idx, (_desc, expected)) in test_cases.iter().enumerate() {{
        {}
    }}

    cleanup_test_database(&db_name).await;
}}

proptest! {{
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    #[ignore]
    fn test_{}_block_insert_random(values in prop::collection::vec(any::<{}>(), 1..100)) {{
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {{
            let (mut client, db_name) = create_isolated_test_client("{}_block_random")
                .await
                .expect("Failed to create test client");

            client
                .query(format!(
                    "CREATE TABLE {{}}.test_table (id UInt32, value {}) ENGINE = Memory",
                    db_name
                ))
                .await
                .expect("Failed to create table");

            let mut block = Block::new();

            let mut id_col = clickhouse_client::column::numeric::ColumnUInt32::new(
                Type::uint32()
            );
            let mut val_col = {}::new(Type::{}());

            for (idx, value) in values.iter().enumerate() {{
                id_col.append(idx as u32);
                val_col.append(*value);
            }}

            block
                .append_column("id", Arc::new(id_col))
                .expect("Failed to append id column");
            block
                .append_column("value", Arc::new(val_col))
                .expect("Failed to append value column");

            client
                .insert(&format!("{{}}.test_table", db_name), block)
                .await
                .expect("Failed to insert block");

            let result = client
                .query(format!(
                    "SELECT value FROM {{}}.test_table ORDER BY id",
                    db_name
                ))
                .await
                .expect("Failed to select");

            assert_eq!(result.total_rows(), values.len());
            let result_col = result.blocks()[0]
                .column(0)
                .expect("Column not found")
                .as_any()
                .downcast_ref::<{}>()
                .expect("Invalid column type");

            for (idx, expected) in values.iter().enumerate() {{
                {}
            }}

            cleanup_test_database(&db_name).await;
        }});
    }}
}}
"#,
        typ.ch_type,
        typ.column_type,
        typ.type_lower,
        typ.type_lower,
        typ.ch_type,
        typ.column_type,
        typ.type_lower,
        typ.test_val,
        typ.min_val,
        typ.max_val,
        typ.column_type,
        typ.test_val,
        typ.min_val,
        typ.max_val,
        comparison,
        typ.type_lower,
        typ.type_lower,
        typ.ch_type,
        typ.min_val,
        typ.max_val,
        typ.mid_val,
        typ.test_val,
        typ.column_type,
        typ.type_lower,
        typ.column_type,
        comparison,
        typ.type_lower,
        typ.rust_type,
        typ.type_lower,
        typ.ch_type,
        typ.column_type,
        typ.type_lower,
        typ.column_type,
        comparison,
    )
}

fn main() {
    println!("Generating integration_block test files...");

    for typ in NUMERIC_TYPES {
        let content = generate_numeric_test(typ);
        let filename = format!("tests/integration_block_{}.rs", typ.type_lower);
        let mut file = fs::File::create(&filename).expect("Failed to create file");
        file.write_all(content.as_bytes())
            .expect("Failed to write file");
        println!("Created {}", filename);
    }

    println!("\nGenerated {} test files", NUMERIC_TYPES.len());
}
"#,
    )
}
