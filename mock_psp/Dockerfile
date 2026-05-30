FROM rust:1.78-slim-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY invoice_service/Cargo.toml ./invoice_service/
COPY mock_psp/Cargo.toml ./mock_psp/
RUN mkdir -p invoice_service/src mock_psp/src && \
    echo "fn main(){}" > invoice_service/src/main.rs && \
    echo "fn main(){}" > mock_psp/src/main.rs
RUN cargo build --release --bin mock_psp 2>/dev/null || true
COPY mock_psp/src ./mock_psp/src
RUN touch mock_psp/src/main.rs
RUN cargo build --release --bin mock_psp

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/mock_psp /usr/local/bin/
CMD ["mock_psp"]