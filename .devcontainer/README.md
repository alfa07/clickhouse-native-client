# Dev Container Configuration

This directory contains the development container configuration for the ClickHouse Rust Client project.

## Features

The dev container includes:

### Docker-in-Docker
- **Feature:** `ghcr.io/devcontainers/features/docker-in-docker:2`
- **Purpose:** Run Docker and Docker Compose inside the container
- **Usage:** Start ClickHouse for testing with `just start-db`

### Rust Toolchain
- **Feature:** `ghcr.io/devcontainers/features/rust:1`
- **Version:** Latest stable
- **Profile:** Default (includes rustc, cargo, rustfmt, clippy)
- **Purpose:** Full Rust development environment

### Additional Tools
- **gh:** GitHub CLI for creating PRs and managing issues (installed via Dockerfile)
- **just:** Command runner for project tasks (installed via Dockerfile)
- **cargo-binstall:** Fast binary installation for Rust tools (installed via Dockerfile)

## Rebuilding the Container

After modifying the devcontainer configuration, you need to rebuild:

### Using Gitpod CLI
```bash
gitpod devcontainer rebuild
```

### Using VS Code
1. Open Command Palette (Ctrl+Shift+P / Cmd+Shift+P)
2. Select "Dev Containers: Rebuild Container"

### Using Ona Tool
```bash
# Ona can rebuild the devcontainer automatically
# Just ask: "rebuild the devcontainer"
```

## What's Included

### Docker & Docker Compose
```bash
docker --version
docker compose version
```

### Rust Toolchain
```bash
rustc --version
cargo --version
rustfmt --version
clippy-driver --version
```

### GitHub CLI
```bash
gh --version
gh auth status
```

### Just Command Runner
```bash
just --version
just --list  # Show all available commands
```

## Testing the Setup

After rebuilding, verify everything works:

```bash
# Check Docker
docker ps

# Check Rust
cargo --version

# Check just
just --version

# Start ClickHouse and run tests
just start-db
just test-integration
just stop-db
```

## Troubleshooting

### Docker not available after rebuild
```bash
# Check if Docker daemon is running
docker ps

# If not, the container may need a restart
# Exit and restart the workspace
```

### Rust tools not found
```bash
# Check if Rust is in PATH
which cargo

# If not, source the environment
source $HOME/.cargo/env
```

### just command not found
```bash
# Check if just is installed
which just

# If not, reinstall
curl -L https://github.com/casey/just/releases/download/1.36.0/just-1.36.0-x86_64-unknown-linux-musl.tar.gz \
    | sudo tar xz -C /usr/local/bin just
```

## Customization

### Adding More Features

Edit `devcontainer.json` and add features from:
- [Official Dev Container Features](https://containers.dev/features)
- [Community Features](https://github.com/devcontainers/features)

Example:
```json
"features": {
    "ghcr.io/devcontainers/features/docker-in-docker:2": {},
    "ghcr.io/devcontainers/features/rust:1": {},
    "ghcr.io/devcontainers/features/node:1": {
        "version": "20"
    }
}
```

### Adding System Packages

Edit `Dockerfile` and add to the `apt-get install` line:
```dockerfile
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        curl \
        wget \
        ca-certificates \
        your-package-here \
    && rm -rf /var/lib/apt/lists/*
```

### Adding Rust Tools

Use cargo-binstall for faster installation:
```dockerfile
RUN cargo binstall -y cargo-watch cargo-edit
```

## Changes Made

### From Original Configuration

**Before:**
- Basic Ubuntu 24.04 base image
- No Docker support
- No Rust toolchain
- No just command runner

**After:**
- Docker-in-Docker enabled
- Rust toolchain (latest stable)
- just command runner installed
- cargo-binstall for tool management

### Why These Changes?

1. **Docker-in-Docker:** Required to run ClickHouse in Docker for integration tests
2. **Rust Feature:** Provides complete Rust development environment
3. **just:** Project uses justfile for task automation
4. **cargo-binstall:** Speeds up installation of additional Rust tools

## Next Steps

After rebuilding the container:

1. **Verify Docker:** `docker ps`
2. **Start ClickHouse:** `just start-db`
3. **Run Tests:** `just test-all`
4. **Run Exchange Tables Test:** `./scripts/run_exchange_test.sh`

## References

- [Dev Containers Specification](https://containers.dev/)
- [Docker-in-Docker Feature](https://github.com/devcontainers/features/tree/main/src/docker-in-docker)
- [Rust Feature](https://github.com/devcontainers/features/tree/main/src/rust)
- [just Command Runner](https://github.com/casey/just)
