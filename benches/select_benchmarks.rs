//! SELECT Query Benchmarks
//!
//! These benchmarks match the C++ benchmark suite in
//! cpp/clickhouse-cpp/bench/bench.cpp to allow direct performance comparison
//! between Rust and C++ implementations.
//!
//! ## Benchmarks:
//! - SelectNumber: SELECT 1000 rows with 3 columns from system.numbers
//! - SelectNumberMoreColumns: SELECT 100 rows with 10 columns (type parsing
//!   stress test)
//!
//! ## Prerequisites:
//! 1. ClickHouse server running on localhost:9000
//! 2. Run with: `cargo bench --bench select_benchmarks`

use clickhouse_client::{
    Client,
    ClientOptions,
};
use criterion::{
    black_box,
    criterion_group,
    criterion_main,
    Criterion,
};
use tokio::runtime::Runtime;

/// Create a test client connected to localhost (async version)
async fn create_client() -> Client {
    Client::connect(
        ClientOptions::new("localhost", 9000)
            .database("default")
            .user("default")
            .password(""),
    )
    .await
    .expect("Failed to connect to ClickHouse")
}

/// Benchmark: SELECT number, number, number FROM system.numbers LIMIT 1000
///
/// Matches C++ benchmark: SelectNumber
/// Tests basic query throughput with moderate row count
/// NOW WITH CONNECTION REUSE (fair comparison to C++)
fn select_number(c: &mut Criterion) {
    use std::{
        cell::RefCell,
        rc::Rc,
    };

    let rt = Runtime::new().unwrap();
    // Create client ONCE and wrap in Rc<RefCell<>> for interior mutability
    let client = Rc::new(RefCell::new(rt.block_on(create_client())));

    c.bench_function("select_number_1000_rows_3_cols", |b| {
        let client = client.clone();
        b.to_async(&rt).iter(move || {
            let client = client.clone();
            async move {
                let mut client = client.borrow_mut();
                let result = client
                    .query("SELECT number, number, number FROM system.numbers LIMIT 1000")
                    .await
                    .expect("Query failed");

                // Force evaluation (match C++ block.GetRowCount())
                black_box(result.total_rows())
            }
        });
    });
}

/// Benchmark: SELECT with 10 columns, 100 rows
///
/// Matches C++ benchmark: SelectNumberMoreColumns
/// Tests type name parsing performance with many columns
/// NOW WITH CONNECTION REUSE (fair comparison to C++)
fn select_number_more_columns(c: &mut Criterion) {
    use std::{
        cell::RefCell,
        rc::Rc,
    };

    let rt = Runtime::new().unwrap();
    // Create client ONCE and wrap in Rc<RefCell<>> for interior mutability
    let client = Rc::new(RefCell::new(rt.block_on(create_client())));

    c.bench_function("select_number_100_rows_10_cols", |b| {
        let client = client.clone();
        b.to_async(&rt).iter(move || {
            let client = client.clone();
            async move {
                let mut client = client.borrow_mut();
                let result = client
                    .query(
                        "SELECT \
                        number, number, number, number, number, \
                        number, number, number, number, number \
                        FROM system.numbers LIMIT 100",
                    )
                    .await
                    .expect("Query failed");

                // Force evaluation
                black_box(result.total_rows())
            }
        });
    });
}

/// Benchmark: Large result set with single column
///
/// Additional benchmark: Tests performance with larger data transfer
/// NOW WITH CONNECTION REUSE (fair comparison to C++)
fn select_large_result(c: &mut Criterion) {
    use std::{
        cell::RefCell,
        rc::Rc,
    };

    let rt = Runtime::new().unwrap();
    // Create client ONCE and wrap in Rc<RefCell<>> for interior mutability
    let client = Rc::new(RefCell::new(rt.block_on(create_client())));

    c.bench_function("select_number_10000_rows_1_col", |b| {
        let client = client.clone();
        b.to_async(&rt).iter(move || {
            let client = client.clone();
            async move {
                let mut client = client.borrow_mut();
                let result = client
                    .query("SELECT number FROM system.numbers LIMIT 10000")
                    .await
                    .expect("Query failed");

                black_box(result.total_rows())
            }
        });
    });
}

criterion_group!(
    benches,
    select_number,
    select_number_more_columns,
    select_large_result
);
criterion_main!(benches);
