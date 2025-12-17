FROM rust:1.91-slim-bookworm AS builder

WORKDIR /app

# Install build dependencies including cmake for rdkafka
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    build-essential \
    cmake \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src src

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/hive-otel-trace-collector /usr/local/bin/hive-otel-trace-collector

COPY config.*.yaml /etc/otel/

RUN mkdir -p /var/lib/otel-collector/buffer

ENV PORT=4318
ENV HEALTH_PORT=13133
ENV CONFIG_FILE=/etc/otel/config.yaml

EXPOSE 4318 13133

CMD ["sh", "-c", "hive-otel-trace-collector --config ${CONFIG_FILE}"]
