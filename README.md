# Hive OTEL Trace Collector

A high-performance OpenTelemetry trace collector written in Rust. Ingests OTLP traces (Protobuf and JSON) and stores them in ClickHouse.

## Features

- **OTLP/HTTP ingestion** - Accepts traces on `/v1/traces` (port 4318)
- **Protobuf & JSON support** - Auto-detects content type
- **ClickHouse storage** - Batched inserts with configurable workers
- **Disk buffer** - Memory-mapped ring buffer for backpressure handling
- **Authentication** - Validates requests against an external auth endpoint
- **Profiling endpoints** - CPU profiling, flamegraphs, and heap stats via pprof
- **Graceful shutdown** - Drains in-flight batches before terminating

## Building

```bash
# Build release binary
cargo build --release
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `4318` | Main OTLP receiver port |
| `HEALTH_PORT` | `9090` | Health/profiling server port |
| `HEALTHCHECK_PORT` | `13133` | Kubernetes healthcheck port |
| `HIVE_OTEL_AUTH_ENDPOINT` | `http://graphql-api:4000/otel-auth` | Auth validation endpoint |
| `CLICKHOUSE_URL` | - | ClickHouse connection URL |
| `CLICKHOUSE_DATABASE` | - | Target database |
| `CLICKHOUSE_TABLE` | - | Target table |
| `ENABLE_DISK_BUFFER` | `false` | Enable disk buffer for backpressure |

## Endpoints

| Path | Port | Description |
|------|------|-------------|
| `/v1/traces` | 4318 | OTLP trace ingestion |
| `/health` | 9090 | Health check |
| `/ready` | 9090 | Readiness check (basic backpressure awareness) |
| `/debug/pprof/profile` | 9090 | CPU profile |
| `/debug/pprof/flamegraph` | 9090 | Flamegraph SVG |
| `/debug/pprof/heap` | 9090 | Heap profile |
| `/debug/pprof/stats` | 9090 | jemalloc stats |

## Architecture

```
Request → Auth → Parser → Batcher → Workers → ClickHouse
                             ↓         ↑
                        Disk Buffer ───┘
```

- **Batcher**: Accumulates spans and flushes to ClickHouse in configurable batch sizes
- **Disk Buffer**: Memory-mapped ring buffer catches overflow when the channel is full. Large requests (>1MB, configurable) are written directly to disk to avoid memory pressure.
- **Drain Worker**: Background task that replays buffered batches to ClickHouse

## License

MIT License, part of the-guild.