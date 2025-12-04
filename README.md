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
                        ↓                         ↑
                    Disk Buffer ───→ Drain ───────┘
```

```
HTTP POST /v1/traces (4318)
        │
        ├─→ Auth
        │   - External validation via HIVE_OTEL_AUTH_ENDPOINT
        │   - Dual-cache: success (30s TTL), failure (5s TTL)
        │   - In-flight request deduplication via broadcast channels
        │
        ├─→ Parse
        │   - Auto-detects JSON vs Protobuf (Content-Type)
        │   - SIMD-accelerated JSON parsing (simd-json)
        │
        ├─→ Transform
        │   - OTLP spans → ClickHouse row format
        │   - Injects target_id into resource/span attributes
        │
        ├─→ Decision: Large request? (>1MB)
        │   - Yes → Direct to disk buffer
        │   - No → Channel to batcher
        │
        ├─→ Batcher
        │   - Accumulates spans (10K batch size default)
        │   - Flushes on timeout (200ms) or when full
        │   - Channel full → overflow to disk buffer
        │
        ├─→ Disk Buffer
        │   - Memory-mapped circular buffer (1GB default)
        │   - Async flushes to ClickHouse
        │
        └─→ ClickHouse
            - Async batched insert with LZ4 compression
            - Failure → disk buffer fallback
```

## License

MIT License, part of the-guild.