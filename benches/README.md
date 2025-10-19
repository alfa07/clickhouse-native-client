# Performance Benchmarks

This directory contains performance benchmarks for the ClickHouse Rust client.

## Benchmark Suite

### Planned Benchmarks

1. **Column Operations** - `column_benchmarks.rs` (TODO)
   - Column append performance
   - Slice operations
   - Clone operations
   - Iteration performance

2. **Compression** - `compression_benchmarks.rs` (TODO)
   - LZ4 compression/decompression
   - ZSTD compression/decompression
   - Compression ratio comparisons

3. **Network** - `network_benchmarks.rs` (TODO)
   - Roundtrip latency
   - Throughput measurements
   - Block transfer performance

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench column_benchmarks

# Generate HTML reports
cargo bench -- --save-baseline my_baseline
```

## Viewing Results

Benchmark results are saved to `target/criterion/` with HTML reports.

## Notes

Benchmarks require a stable environment for accurate measurements:
- Close other applications
- Run on consistent hardware
- Multiple iterations for statistical significance

Criterion automatically:
- Warms up before measurement
- Collects multiple samples
- Performs statistical analysis
- Detects performance regressions

## Implementation Status

- ❌ Column benchmarks - Not yet implemented (API needs stabilization)
- ❌ Compression benchmarks - Not yet implemented (need public API)
- ❌ Network benchmarks - Not yet implemented (requires running server)

**Note:** Benchmarks will be implemented once the core column API is stabilized.
For now, manual testing and profiling can be used for performance analysis.
