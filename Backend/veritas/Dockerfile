# syntax=docker/dockerfile:1.7

# Use Rust slim for dependency build
FROM rust:1.91-slim AS chef
WORKDIR /usr/src/app
RUN apt-get update && \
    apt-get install -y musl-tools pkg-config && \
    rm -rf /var/lib/apt/lists/* && \
    cargo install cargo-chef && \
    rustup target add x86_64-unknown-linux-musl
# IMPORTANT: ensure Cargo.toml uses rustls (no OpenSSL):
# reqwest = { version = "0.12", default-features = false, features = ["rustls-tls","json"] }

FROM chef AS planner
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /usr/src/app/recipe.json recipe.json
ENV CARGO_BUILD_TARGET=x86_64-unknown-linux-musl
# Cache registry and target during dependency cook
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/app/target \
    cargo chef cook --release --recipe-path recipe.json

# Build application (static), reuse caches
COPY . .
ENV RUSTFLAGS="-C target-feature=+crt-static -C strip=symbols"
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/src/app/target \
    cargo build --release --target x86_64-unknown-linux-musl -p veritas && \
    cp target/x86_64-unknown-linux-musl/release/veritas /usr/local/bin/veritas && \
    strip /usr/local/bin/veritas

# Final minimal image
FROM scratch AS runtime
ARG BINARY=veritas
EXPOSE 80
COPY --from=builder /usr/local/bin/veritas /app
ENTRYPOINT ["/app"]