//! Column Serialization/Deserialization Benchmarks
//!
//! These benchmarks match the C++ performance tests in
//! cpp/clickhouse-cpp/ut/performance_tests.cpp to allow direct performance
//! comparison for column operations.
//!
//! ## Benchmarks:
//! - Column append operations (1M items)
//! - Column serialization (Save)
//! - Column deserialization (Load)
//!
//! ## Run with:
//! `cargo bench --bench column_benchmarks`

use bytes::BytesMut;
use clickhouse_client::{
    column::{
        numeric::ColumnUInt64,
        string::ColumnString,
        Column,
    },
    types::Type,
};
use criterion::{
    black_box,
    criterion_group,
    criterion_main,
    BenchmarkId,
    Criterion,
    Throughput,
};

const ITEMS_1M: usize = 1_000_000;
const ITEMS_100K: usize = 100_000;

/// Generate UInt64 value for index
/// Matches C++ generate() function for ColumnUInt64
#[inline]
fn generate_uint64(index: usize) -> u64 {
    let base = (index % 255) as u64;
    base << 56
        | base << 48
        | base << 40
        | base << 32
        | base << 24
        | base << 16
        | base << 8
        | base
}

/// Generate string value for index
/// Matches C++ generate() function for ColumnString (7 chars)
#[inline]
fn generate_string(index: usize) -> String {
    const TEMPLATE: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789\
                              9876543210ZYXWVUTSRQPONMLKJIHGFEDCBAzyxwvutsrqponmlkjihgfedcba";
    const RESULT_SIZE: usize = 7;

    let start_pos = index % (TEMPLATE.len() - RESULT_SIZE);
    String::from_utf8_lossy(&TEMPLATE[start_pos..start_pos + RESULT_SIZE])
        .to_string()
}

/// Benchmark: Append 1M items to UInt64 column
fn column_uint64_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_append");
    group.throughput(Throughput::Elements(ITEMS_1M as u64));

    group.bench_function(BenchmarkId::new("UInt64", "1M_items"), |b| {
        b.iter(|| {
            let mut col = ColumnUInt64::new();
            for i in 0..ITEMS_1M {
                col.append(black_box(generate_uint64(i)));
            }
            black_box(col.size())
        });
    });

    group.finish();
}

/// Benchmark: Append 1M items to String column
fn column_string_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_append");
    group.throughput(Throughput::Elements(ITEMS_1M as u64));

    group.bench_function(BenchmarkId::new("String", "1M_items"), |b| {
        b.iter(|| {
            let mut col = ColumnString::new(Type::string());
            for i in 0..ITEMS_1M {
                col.append(black_box(generate_string(i)));
            }
            black_box(col.size())
        });
    });

    group.finish();
}

/// Benchmark: Serialize UInt64 column (1M items)
fn column_uint64_save(c: &mut Criterion) {
    // Pre-create column with 1M items
    let mut col = ColumnUInt64::new();
    for i in 0..ITEMS_1M {
        col.append(generate_uint64(i));
    }

    let mut group = c.benchmark_group("column_save");
    group.throughput(Throughput::Bytes((ITEMS_1M * 8) as u64)); // 8 bytes per UInt64

    group.bench_function(BenchmarkId::new("UInt64", "1M_items"), |b| {
        b.iter(|| {
            let mut buffer = BytesMut::new();
            col.save_to_buffer(&mut buffer).expect("Failed to serialize");
            black_box(buffer.len())
        });
    });

    group.finish();
}

/// Benchmark: Serialize String column (1M items)
fn column_string_save(c: &mut Criterion) {
    // Pre-create column with 1M items
    let mut col = ColumnString::new(Type::string());
    for i in 0..ITEMS_1M {
        col.append(generate_string(i));
    }

    let mut group = c.benchmark_group("column_save");
    group.throughput(Throughput::Bytes((ITEMS_1M * 7) as u64)); // ~7 bytes per string

    group.bench_function(BenchmarkId::new("String", "1M_items"), |b| {
        b.iter(|| {
            let mut buffer = BytesMut::new();
            col.save_to_buffer(&mut buffer).expect("Failed to serialize");
            black_box(buffer.len())
        });
    });

    group.finish();
}

/// Benchmark: Deserialize UInt64 column (1M items)
fn column_uint64_load(c: &mut Criterion) {
    // Pre-serialize column
    let mut col = ColumnUInt64::new();
    for i in 0..ITEMS_1M {
        col.append(generate_uint64(i));
    }

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();
    let serialized = buffer.freeze();

    let mut group = c.benchmark_group("column_load");
    group.throughput(Throughput::Bytes(serialized.len() as u64));

    group.bench_function(BenchmarkId::new("UInt64", "1M_items"), |b| {
        b.iter(|| {
            let mut data = &serialized[..];
            let mut col = ColumnUInt64::new();
            col.load_from_buffer(&mut data, black_box(ITEMS_1M))
                .expect("Failed to deserialize");
            black_box(col.size())
        });
    });

    group.finish();
}

/// Benchmark: Deserialize String column (1M items)
fn column_string_load(c: &mut Criterion) {
    // Pre-serialize column
    let mut col = ColumnString::new(Type::string());
    for i in 0..ITEMS_1M {
        col.append(generate_string(i));
    }

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();
    let serialized = buffer.freeze();

    let mut group = c.benchmark_group("column_load");
    group.throughput(Throughput::Bytes(serialized.len() as u64));

    group.bench_function(BenchmarkId::new("String", "1M_items"), |b| {
        b.iter(|| {
            let mut data = &serialized[..];
            let mut col = ColumnString::new(Type::string());
            col.load_from_buffer(&mut data, black_box(ITEMS_1M))
                .expect("Failed to deserialize");
            black_box(col.size())
        });
    });

    group.finish();
}

/// Benchmark: Round-trip (Save + Load) for UInt64
fn column_uint64_roundtrip(c: &mut Criterion) {
    let mut col = ColumnUInt64::new();
    for i in 0..ITEMS_100K {
        col.append(generate_uint64(i));
    }

    let mut group = c.benchmark_group("column_roundtrip");
    group.throughput(Throughput::Elements(ITEMS_100K as u64));

    group.bench_function(BenchmarkId::new("UInt64", "100K_items"), |b| {
        b.iter(|| {
            // Serialize
            let mut buffer = BytesMut::new();
            col.save_to_buffer(&mut buffer).unwrap();
            let serialized = buffer.freeze();

            // Deserialize
            let mut data = &serialized[..];
            let mut loaded_col = ColumnUInt64::new();
            loaded_col.load_from_buffer(&mut data, ITEMS_100K).unwrap();

            black_box(loaded_col.size())
        });
    });

    group.finish();
}

/// Benchmark: Save with buffer reuse (FAIR comparison to C++)
/// Matches C++ methodology: reuses buffer capacity across iterations
fn column_uint64_save_fair(c: &mut Criterion) {
    // Pre-create column with 1M items
    let mut col = ColumnUInt64::new();
    for i in 0..ITEMS_1M {
        col.append(generate_uint64(i));
    }

    let mut group = c.benchmark_group("column_save_fair");
    group.throughput(Throughput::Bytes((ITEMS_1M * 8) as u64));

    // Pre-allocate buffer with capacity (like C++ does)
    let mut buffer = BytesMut::with_capacity(ITEMS_1M * 8);

    group.bench_function(BenchmarkId::new("UInt64", "1M_items_reuse"), |b| {
        b.iter(|| {
            buffer.clear(); // Keeps capacity like C++ buffer.clear()!
            col.save_to_buffer(&mut buffer).expect("Failed to serialize");
            black_box(buffer.len())
        });
    });

    group.finish();
}

/// Benchmark: Load with capacity reuse (FAIR comparison to C++)
/// Matches C++ methodology: reuses column capacity across iterations
fn column_uint64_load_fair(c: &mut Criterion) {
    // Pre-serialize column
    let mut col = ColumnUInt64::new();
    for i in 0..ITEMS_1M {
        col.append(generate_uint64(i));
    }

    let mut buffer = BytesMut::new();
    col.save_to_buffer(&mut buffer).unwrap();
    let serialized = buffer.freeze();

    let mut group = c.benchmark_group("column_load_fair");
    group.throughput(Throughput::Bytes(serialized.len() as u64));

    // Pre-allocate column with capacity (like C++ does with column.Clear())
    let mut reusable_col = ColumnUInt64::with_capacity(ITEMS_1M);

    group.bench_function(BenchmarkId::new("UInt64", "1M_items_reuse"), |b| {
        b.iter(|| {
            let mut data = &serialized[..];
            reusable_col.clear(); // Keeps capacity like C++ column.Clear()!
            reusable_col
                .load_from_buffer(&mut data, black_box(ITEMS_1M))
                .expect("Failed to deserialize");
            black_box(reusable_col.size())
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    column_uint64_append,
    column_string_append,
    column_uint64_save,
    column_string_save,
    column_uint64_load,
    column_string_load,
    column_uint64_roundtrip,
    column_uint64_save_fair,
    column_uint64_load_fair
);
criterion_main!(benches);
