# Justfile for ClickHouse Client Development

# Start ClickHouse in Docker
start-db:
    @echo "Starting ClickHouse container..."
    docker-compose up -d clickhouse
    @echo "Waiting for ClickHouse to be ready..."
    @sleep 5
    @echo "ClickHouse is ready on localhost:9000"

# Stop ClickHouse container
stop-db:
    @echo "Stopping ClickHouse container..."
    docker-compose down clickhouse

# Generate TLS certificates for testing
generate-certs:
    @echo "Generating TLS certificates..."
    @cd certs && bash generate-certs.sh
    @echo "✓ Certificates generated in certs/"

# Start TLS-enabled ClickHouse in Docker
start-db-tls:
    @echo "Checking for certificates..."
    @if [ ! -f certs/ca/ca-cert.pem ]; then \
        echo "Certificates not found. Generating..."; \
        just generate-certs; \
    fi
    @echo "Starting TLS-enabled ClickHouse container..."
    docker-compose up -d clickhouse-tls
    @echo "Waiting for ClickHouse TLS to be ready..."
    @sleep 5
    @echo "ClickHouse TLS is ready on localhost:9440"

# Stop TLS ClickHouse container
stop-db-tls:
    @echo "Stopping TLS ClickHouse container..."
    docker-compose down clickhouse-tls

# Start both standard and TLS ClickHouse servers
start-db-all:
    @echo "Starting both ClickHouse containers..."
    @just start-db
    @just start-db-tls
    @echo "✓ Both servers ready (9000=standard, 9440=TLS)"

# Stop both servers
stop-db-all:
    @echo "Stopping all ClickHouse containers..."
    docker-compose down

# Clean up containers and volumes
clean:
    @echo "Cleaning up ClickHouse containers and volumes..."
    docker-compose down -v
    @rm -rf clickhouse-data clickhouse-data-tls

# Clean TLS artifacts (certificates and data)
clean-tls:
    @echo "Cleaning up TLS data..."
    @rm -rf clickhouse-data-tls
    docker-compose down -v clickhouse-tls

# Clean certificates (will need to regenerate)
clean-certs:
    @echo "Removing generated certificates..."
    @rm -f certs/ca/*.pem certs/server/*.pem certs/client/*.pem
    @echo "✓ Certificates removed (run 'just generate-certs' to regenerate)"

# Run unit tests only
test:
    cargo test --lib

# Run integration tests (requires running ClickHouse)
test-integration:
    cargo test --test integration_test -- --ignored --nocapture

# Run TLS integration tests (requires TLS-enabled ClickHouse)
test-tls:
    @echo "Running TLS integration tests..."
    @just start-db-tls
    @sleep 2
    cargo test --features tls --test tls_integration_test -- --ignored --nocapture
    @just stop-db-tls

# Run all tests (unit + integration, no TLS)
test-all: start-db
    cargo test --lib
    @sleep 2
    cargo test --test integration_test -- --ignored --nocapture
    @just stop-db

# Run ALL tests including TLS
test-all-with-tls:
    @echo "Running complete test suite (unit + integration + TLS)..."
    @just test
    @just start-db
    @sleep 2
    cargo test --test integration_test -- --ignored --nocapture
    @just stop-db
    @echo ""
    @echo "==> Now running TLS tests..."
    @just test-tls
    @echo ""
    @echo "✓ All tests completed successfully!"

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

# Run Claude agent in a git worktree with task from file
run-agent FILE:
    #!/usr/bin/env bash
    set -euo pipefail

    # Check if file exists
    if [ ! -f "{{FILE}}" ]; then
        echo "Error: File '{{FILE}}' not found"
        exit 1
    fi

    # Convert file path to absolute path (for use after cd)
    FILE_PATH=$(realpath "{{FILE}}")
    echo "Task file: ${FILE_PATH}"

    # Extract branch name from filename (remove directory path and .txt extension)
    BRANCH_NAME=$(basename "{{FILE}}" .txt)
    echo "Branch name: ${BRANCH_NAME}"

    # Pull latest changes from main
    echo ""
    echo "==> Pulling latest changes from main branch..."
    CURRENT_BRANCH=$(git branch --show-current)
    git checkout main || { echo "Error: Failed to checkout main branch"; exit 1; }
    git pull origin main || { echo "Error: Failed to pull from origin/main"; exit 1; }

    # Return to original branch if it wasn't main
    if [ "${CURRENT_BRANCH}" != "main" ]; then
        git checkout "${CURRENT_BRANCH}" || true
    fi

    # Check if worktree already exists
    WORKTREE_PATH="../${BRANCH_NAME}"
    if [ -d "${WORKTREE_PATH}" ]; then
        echo ""
        echo "Error: Worktree directory '${WORKTREE_PATH}' already exists"
        echo "To remove it, run: git worktree remove ${WORKTREE_PATH}"
        exit 1
    fi

    # Check if branch already exists
    if git show-ref --verify --quiet "refs/heads/${BRANCH_NAME}"; then
        echo ""
        echo "Error: Branch '${BRANCH_NAME}' already exists"
        echo "To use existing branch: git worktree add ${WORKTREE_PATH} ${BRANCH_NAME}"
        echo "To delete branch: git branch -d ${BRANCH_NAME}"
        exit 1
    fi

    # Create worktree
    echo ""
    echo "==> Creating worktree at '${WORKTREE_PATH}' with branch '${BRANCH_NAME}'..."
    git worktree add "${WORKTREE_PATH}" -b "${BRANCH_NAME}" || {
        echo "Error: Failed to create worktree"
        exit 1
    }

    # Run Claude in the worktree
    echo ""
    echo "==> Running Claude agent in worktree..."
    echo "Task: $(head -n 1 "${FILE_PATH}")"
    echo ""

    cd "${WORKTREE_PATH}" || {
        echo "Error: Failed to cd into worktree"
        exit 1
    }

    claude --dangerously-skip-permissions --permission-mode bypassPermissions -p "$(cat "${FILE_PATH}")" || {
        echo ""
        echo "Error: Claude execution failed"
        echo "Worktree is still available at: ${WORKTREE_PATH}"
        exit 1
    }

    echo ""
    echo "==> Agent completed. Worktree location: ${WORKTREE_PATH}"

# Show help
help:
    @just --list
