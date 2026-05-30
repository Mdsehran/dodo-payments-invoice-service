FROM rust:1.78-slim-bookworm AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY invoice_service/Cargo.toml ./invoice_service/
COPY mock_psp/Cargo.toml ./mock_psp/
# Dummy sources to cache dependencies
RUN mkdir -p invoice_service/src mock_psp/src && \
    echo "fn main(){}" > invoice_service/src/main.rs && \
    echo "fn main(){}" > mock_psp/src/main.rs
RUN cargo build --release --bin invoice_service 2>/dev/null || true
# Real sources
COPY invoice_service/src ./invoice_service/src
RUN touch invoice_service/src/main.rs
RUN cargo build --release --bin invoice_service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/invoice_service /usr/local/bin/
COPY migrations /migrations
CMD ["invoice_service"]