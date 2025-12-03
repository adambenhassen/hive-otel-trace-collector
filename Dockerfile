FROM rust:1.91-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/hive-otel-trace-collector /usr/local/bin/

RUN mkdir -p /var/lib/rust-collector

ENV PORT=4318
ENV BUFFER_DIR=/var/lib/rust-collector

EXPOSE 4318

CMD ["hive-otel-trace-collector"]
