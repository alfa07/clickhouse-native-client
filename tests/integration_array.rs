/// Integration tests for Array compound types
/// Tests: Array(Float32), Array(Float64), Array(String), Array(Int32),
/// Array(Int64),        Array(Date), Array(Date32), Array(DateTime),
/// Array(DateTime64),        Array(LowCardinality(String)), Array(Decimal(10,
/// 2))
mod common;

use common::{
    cleanup_test_database,
    create_isolated_test_client,
};

// ============================================================================
// Array(Float32)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_array_float32_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("array_float32")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(Float32)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    // Use SQL INSERT for arrays
    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ([]), ([1.5]), ([1.0, 2.0, 3.0, 4.0, 5.0]), ([-1.5, 0.0, 1.5])",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT values FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Array(Float64)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_array_float64_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("array_float64")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(Float64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ([]), ([3.14159, 2.71828]), ([-1.0, 0.0, 1.0, 2.0])",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT values FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Array(String)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_array_string_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("array_string")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(String)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ([]), (['']), (['hello', 'world', 'test']), (['こんにちは', '世界'])",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT values FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Array(Int32)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_array_int32_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("array_int32")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(Int32)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ([]), ([-2147483648, -1, 0, 1, 2147483647]), ([42, 100, 200])",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT values FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Array(Int64)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_array_int64_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("array_int64")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (values Array(Int64)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ([]), ([-9223372036854775808, 0, 9223372036854775807]), ([1000000, 2000000, 3000000])",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT values FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Array(Date)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_array_date_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("array_date")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (dates Array(Date)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ([]), (['1970-01-01', '2022-01-01', '2023-01-01']), (['2020-01-01', '2021-01-01'])",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT dates FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Array(Date32)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_array_date32_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("array_date32")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (dates Array(Date32)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ([]), (['1900-01-02', '1970-01-01', '2023-01-01']), (['2020-01-01'])",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT dates FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Array(DateTime)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_array_datetime_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("array_datetime")
        .await
        .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (timestamps Array(DateTime)) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ([]), (['1970-01-01 00:00:00', '2023-01-01 00:00:00']), (['2024-01-01 12:30:45'])",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT timestamps FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Array(DateTime64)
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_array_datetime64_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("array_datetime64")
            .await
            .expect("Failed to create test client");

    let precision = 3;
    client
        .query(format!(
            "CREATE TABLE {}.test_table (timestamps Array(DateTime64({}))) ENGINE = Memory",
            db_name, precision
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ([]), (['1970-01-01 00:00:00.000', '2023-01-01 00:00:00.123']), (['2024-01-01 12:30:45.999'])",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT timestamps FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Array(LowCardinality(String))
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_array_lowcardinality_string_roundtrip() {
    let (mut client, db_name) =
        create_isolated_test_client("array_lowcard_string")
            .await
            .expect("Failed to create test client");

    client
        .query(format!(
            "CREATE TABLE {}.test_table (tags Array(LowCardinality(String))) ENGINE = Memory",
            db_name
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ([]), (['tag1', 'tag2']), (['tag3', 'tag1', 'tag2']), (['tag1'])",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT tags FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 4);

    cleanup_test_database(&db_name).await;
}

// ============================================================================
// Array(Decimal(10, 2))
// ============================================================================

#[tokio::test]
#[ignore]
async fn test_array_decimal_roundtrip() {
    let (mut client, db_name) = create_isolated_test_client("array_decimal")
        .await
        .expect("Failed to create test client");

    let precision = 10;
    let scale = 2;

    client
        .query(format!(
            "CREATE TABLE {}.test_table (prices Array(Decimal({}, {}))) ENGINE = Memory",
            db_name, precision, scale
        ))
        .await
        .expect("Failed to create table");

    client
        .query(format!(
            "INSERT INTO {}.test_table VALUES ([]), ([123.45, 678.90, 111.11]), ([0.00, -123.45])",
            db_name
        ))
        .await
        .expect("Failed to insert");

    let result = client
        .query(format!("SELECT prices FROM {}.test_table", db_name))
        .await
        .expect("Failed to select");

    assert_eq!(result.total_rows(), 3);

    cleanup_test_database(&db_name).await;
}
