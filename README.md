# Hive OTEL Trace Collector

A high-performance OpenTelemetry trace collector written in Rust. Ingests OTLP traces (Protobuf and JSON) and stores them in ClickHouse.

## Features

- **OTLP/HTTP ingestion** - Accepts traces on `/v1/traces` (port 4318)
- **Protobuf & JSON support** - Auto-detects content type
- **ClickHouse storage** - Batched inserts with configurable workers
- **Memory-based backpressure** - Monitors process RSS and spills to disk when limit exceeded
- **Auto memory detection** - Detects cgroup limits (containers) or system memory
- **Disk buffer** - Memory-mapped ring buffer for durable overflow handling
- **Authentication** - Validates requests against an external auth endpoint with caching
- **Profiling endpoints** - CPU profiling, flamegraphs, and heap stats via pprof
- **Graceful shutdown** - Drains in-flight batches before terminating

## Building

```bash
make build          # Debug build
make build-release  # Release build
make build-linux    # Cross-compile for Linux
```

## Testing

```bash
make test             # Run all tests
make test-unit        # Unit tests only
make test-integration # Integration tests (requires Docker)
make test-e2e         # End-to-end tests (requires Docker)
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
| `BATCH_MAX_SPANS` | `10000` | Max spans per batch |
| `BATCH_TIMEOUT_MS` | `200` | Batch flush timeout |
| `BATCH_WORKERS` | `4` | Number of batcher workers |
| `MEM_BUFFER_SIZE_MB` | auto | Memory buffer limit in MB (default: 90% cgroup or 25% system) |
| `DISK_BUFFER_ENABLED` | `true` | Enable disk buffer for backpressure |
| `DISK_BUFFER_DIR` | `/var/lib/otel-collector/buffer` | Disk buffer directory |
| `DISK_BUFFER_MAX_SIZE_MB` | `1024` | Max disk buffer size in MB (1GB) |
| `DISK_BUFFER_SEGMENT_SIZE_MB` | `64` | Buffer segment size in MB |
| `RUST_LOG` | `info` | Logging level (e.g., `info`, `debug`) |

**Note**: `MEM_BUFFER_SIZE_MB` defaults to 90% of cgroup memory limit (when containerized) or 25% of available system memory. When process memory exceeds this limit, new requests are written to disk buffer instead of memory.

## Endpoints

| Path | Port | Description |
|------|------|-------------|
| `/v1/traces` | 4318 | OTLP trace ingestion |
| `/` | 13133 | Liveness probe |
| `/health` | 13133 | Health check |
| `/ready` | 13133 | Readiness check |
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
        |
        ├─→ Decision: RSS over MEM_BUFFER_SIZE_MB?
        │   - Yes → Direct to disk buffer
        │   - No → Channel to batcher
        │
        ├─→ Batcher (Memory buffer)
        │   - Accumulates spans (10K batch size default)
        │   - Flushes on timeout (200ms) or when full
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