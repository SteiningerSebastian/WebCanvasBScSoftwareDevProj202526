# Use latest Rust version that supports edition2024
FROM rust:1.91-slim AS chef
WORKDIR /usr/src/app
RUN apt-get update && \
    apt-get install -y musl-tools pkg-config && \
    rm -rf /var/lib/apt/lists/* && \
    cargo install cargo-chef && \
    rustup target add x86_64-unknown-linux-musl

# Plan the build
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Build dependencies only (cached layer)
FROM chef AS builder
COPY --from=planner /usr/src/app/recipe.json recipe.json
ENV CARGO_BUILD_TARGET=x86_64-unknown-linux-musl
RUN cargo chef cook --release --recipe-path recipe.json

# Build the actual application
COPY . .
RUN cargo build --release -p veritas

# Runtime stage - use distroless for smaller image
FROM gcr.io/distroless/static-debian12
ARG BINARY=veritas
EXPOSE 80
COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/${BINARY} /app
ENTRYPOINT ["/app"]