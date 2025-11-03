ARG CARGO_IMAGE=rust:latest
FROM ${CARGO_IMAGE} AS builder
WORKDIR /usr/src/app

# Install musl and other build deps
RUN apt-get update && apt-get install -y musl-tools gcc build-essential ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy workspace manifests first for layer caching
COPY Cargo.toml Cargo.lock ./
COPY veritas/Cargo.toml veritas/Cargo.toml
COPY general/Cargo.toml general/Cargo.toml

# Create minimal placeholder src files so workspace members have a target
# This does NOT build anything — it only satisfies Cargo's manifest checks so `cargo fetch` can run.
RUN mkdir -p veritas/src general/src && \
    echo 'fn main() { println!("placeholder"); }' > veritas/src/main.rs && \
    echo 'pub fn _lib_placeholder() {}' > general/src/lib.rs

# Fetch dependencies (populate cargo cache) WITHOUT producing a binary
RUN cargo fetch

# Add musl target and set target env (for statically linked artifact)
RUN rustup target add x86_64-unknown-linux-musl
ENV CARGO_BUILD_TARGET=x86_64-unknown-linux-musl

# Now copy full workspace source (overwrites the placeholders)
COPY . .

# Build the real package; fail the build if this step errors
RUN cargo build --release -p veritas

# Final image
FROM scratch
ARG BINARY=veritas

EXPOSE 80
COPY --from=builder /usr/src/app/target/x86_64-unknown-linux-musl/release/${BINARY} /app
ENTRYPOINT ["/app"]