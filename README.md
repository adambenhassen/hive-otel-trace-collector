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
| `HEALTH_PORT` | `13133` | Health/profiling server port |
| `HIVE_OTEL_AUTH_ENDPOINT` | `http://graphql-api:4000/otel-auth` | Auth validation endpoint |
| `DISABLE_AUTH` | - | Set to disable authentication |
| `CLICKHOUSE_URL` | `http://clickhouse:8123` | ClickHouse connection URL |
| `CLICKHOUSE_HOST` | - | ClickHouse host (alternative to URL) |
| `CLICKHOUSE_PORT` | - | ClickHouse port (alternative to URL) |
| `CLICKHOUSE_PROTOCOL` | `http` | ClickHouse protocol (http/https) |
| `CLICKHOUSE_DATABASE` | `default` | Target database |
| `CLICKHOUSE_TABLE` | `otel_traces` | Target table |
| `CLICKHOUSE_USERNAME` | `default` | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | - | ClickHouse password |
| `CLICKHOUSE_ASYNC_INSERT` | `true` | Enable async inserts |
| `CLICKHOUSE_WAIT_FOR_ASYNC_INSERT` | `false` | Wait for async insert confirmation |
| `BATCH_MAX_SIZE` | `10000` | Max spans per batch |
| `BATCH_TIMEOUT_MS` | `200` | Batch flush timeout |
| `BATCH_WORKERS` | `4` | Number of batcher workers |
| `MEM_BUFFER_CAPACITY` | `100000` | Max pending requests in memory before backpressure |
| `DISK_BUFFER_ENABLED` | `false` | Enable disk buffer for backpressure |
| `DISK_BUFFER_DIR` | `/var/lib/otel-collector/buffer` | Disk buffer directory |
| `DISK_BUFFER_MAX_SIZE` | `1073741824` | Max buffer size (1GB) |
| `DISK_BUFFER_SEGMENT_SIZE` | `16777216` | Buffer segment size (16MB) |

## Endpoints

| Path | Port | Description |
|------|------|-------------|
| `/v1/traces` | 4318 | OTLP trace ingestion |
| `/` | 13133 | Liveness probe |
| `/health` | 13133 | Health check |
| `/ready` | 13133 | Readiness check (basic backpressure awareness) |
| `/debug/pprof/profile` | 13133 | CPU profile |
| `/debug/pprof/flamegraph` | 13133 | Flamegraph SVG |
| `/debug/pprof/heap` | 13133 | Heap profile |
| `/debug/pprof/stats` | 13133 | jemalloc stats |

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