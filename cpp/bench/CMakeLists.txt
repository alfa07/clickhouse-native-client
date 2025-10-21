ADD_EXECUTABLE (bench
    bench_simple.cpp
)

# Add Google Benchmark include directory
TARGET_INCLUDE_DIRECTORIES(bench PRIVATE /opt/homebrew/include)

TARGET_LINK_LIBRARIES (bench
    clickhouse-cpp-lib
    /opt/homebrew/lib/libbenchmark.a
    pthread
)
