FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

# Install just command runner and other useful tools
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        curl \
        wget \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install just from GitHub releases
RUN curl -L https://github.com/casey/just/releases/download/1.36.0/just-1.36.0-x86_64-unknown-linux-musl.tar.gz \
    | tar xz -C /usr/local/bin just \
    && chmod +x /usr/local/bin/just

# Install cargo-binstall for easier Rust tool installation
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
