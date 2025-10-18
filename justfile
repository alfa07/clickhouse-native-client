# Justfile for ClickHouse Client Development

# Start ClickHouse in Docker
start-db:
    @echo "Starting ClickHouse container..."
    docker-compose up -d
    @echo "Waiting for ClickHouse to be ready..."
    @sleep 5
    @echo "ClickHouse is ready on localhost:9000"

# Stop ClickHouse container
stop-db:
    @echo "Stopping ClickHouse container..."
    docker-compose down

# Clean up containers and volumes
clean:
    @echo "Cleaning up ClickHouse containers and volumes..."
    docker-compose down -v
    @rm -rf clickhouse-data

# Run unit tests only
test:
    cargo test --lib

# Run integration tests (requires running ClickHouse)
test-integration:
    cargo test --test integration_test -- --ignored --nocapture

# Run all tests (unit + integration)
test-all: start-db
    cargo test --lib
    @sleep 2
    cargo test --test integration_test -- --ignored --nocapture
    @just stop-db

# Build the project
build:
    cargo build

# Build release version
build-release:
    cargo build --release

# Check code without building
check:
    cargo check

# Format code
fmt:
    cargo fmt

# Run clippy
clippy:
    cargo clippy -- -D warnings

# View ClickHouse logs
logs:
    docker-compose logs -f clickhouse

# Open ClickHouse client (for manual testing)
cli:
    docker exec -it clickhouse-server clickhouse-client

# Show help
help:
    @just --list
