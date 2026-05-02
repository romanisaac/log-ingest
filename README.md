# log-ingest

A high-throughput log ingestion and query engine written in Rust. Events flow from Kafka through an in-memory batch buffer, land as columnar Parquet files, and are queryable over SQL in milliseconds.

Built as a portfolio project targeting realistic production constraints: 100k events/sec ingest, bounded memory, sub-second query latency.

---

## Architecture

```
Kafka / Redpanda
      │
      ▼
 BatchBuffer          ← accumulates events in memory (64 MB / 50k records / 1–2s)
      │  flush triggered
      ▼
 Parquet files        ← written to local disk, partitioned by service + hour
      │
      ▼
 SQLite manifest      ← WAL-mode catalog: tracks every file, time range, offsets
      │
      ▼
 DataFusion SQL       ← query-time UNION ALL across relevant Parquet files
      │
      ▼
 HTTP /query API      ← Axum, returns JSON rows
```

**At-least-once delivery.** Kafka offsets are committed only after a successful Parquet flush. Duplicate events are deduplicated at query time via `(kafka_partition, kafka_offset)`.

**Hot/cold tiering.** Recent files live on local disk; older files are promoted to S3-compatible object storage (MinIO in dev). The `logs` view unifies both tiers transparently.

---

## Event schema

Every log event has a fixed envelope plus a typed attribute map for arbitrary metadata.

```json
{
  "timestamp": 1700000000000000000,
  "level": "INFO",
  "service": "api-gateway",
  "message": "request handled",
  "kafka_partition": 3,
  "kafka_offset": 9999,
  "attributes": {
    "request_id": "abc-123",
    "status_code": 200,
    "latency_ms": 42.5,
    "cached": false
  }
}
```

Attribute values are typed: `String`, `Int` (i64), `Float` (f64), or `Bool`. They are serialized as JSON into a single Parquet column, keeping the Arrow schema fixed while allowing per-event flexibility.

---

## Workspace

```
log-ingest/
├── crates/
│   ├── event-schema/     # LogEvent type, Arrow encoding/decoding
│   ├── batch-buffer/     # In-memory buffer with size/count/time flush triggers
│   ├── manifest/         # SQLite WAL catalog (commit, query active files)
│   ├── storage/          # Parquet read/write, local + S3 abstraction
│   ├── query-engine/     # DataFusion session, view construction, dedup
│   └── compactor/        # Background compaction of small Parquet files
├── server/               # Axum HTTP server + flush pipeline
│   └── examples/
│       └── prefill.rs    # Dev helper: seed data without Kafka
└── seed/                 # rdkafka producer: publishes synthetic events to Redpanda
```

Each crate has a single responsibility and a minimal public interface — deep modules, shallow dependencies.

---

## Tech stack

| Concern | Choice | Why |
|---|---|---|
| Language | Rust | Memory safety without GC; predictable latency |
| Ingestion | Kafka / Redpanda | Durable, partitioned, replayable |
| Columnar format | Apache Parquet | Efficient compression and predicate pushdown |
| In-process query | Apache DataFusion | SQL over Parquet with no external query service |
| Catalog | SQLite (WAL mode) | Single writer, ACID, zero operational overhead |
| HTTP layer | Axum | Ergonomic async Rust, tower middleware |
| Dev infra | Docker Compose | Redpanda + MinIO + Prometheus + Grafana |

---

## Getting started

**Prerequisites:** Rust (stable), Docker, `cmake` (for rdkafka).

```bash
# Start Redpanda, MinIO, Prometheus, Grafana
make up

# Publish 1000 synthetic events to Redpanda
make seed

# Run the server
cargo run --bin server

# Query ingested events
curl -s -X POST localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT service, level, message FROM logs LIMIT 10"}' | jq
```

To test the query path without Kafka:

```bash
cargo run --example prefill   # writes 20 events directly to data/ and manifest.db
cargo run --bin server
curl -s -X POST localhost:8080/query \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT * FROM logs"}'
```

---

## Make targets

| Target | Description |
|---|---|
| `make build` | Compile all crates |
| `make test` | Run full test suite |
| `make lint` | Run clippy with `-D warnings` |
| `make up` | Start Docker Compose stack |
| `make down` | Stop Docker Compose stack |
| `make seed` | Create `logs` topic and publish 1000 synthetic events |
| `make clean` | Delete local Parquet files and manifest (resets server state) |

---

## Query API

### `POST /query`

```json
{
  "sql": "SELECT service, count(*) FROM logs WHERE level = 'ERROR' GROUP BY service",
  "time_from": 1700000000000000000,
  "time_to":   1700003600000000000,
  "limit": 1000
}
```

- `time_from` / `time_to` — nanosecond epoch timestamps (optional; defaults to all time)
- `limit` — max rows returned (default 1000)
- Response: JSON array of row objects

### `GET /health`

Returns `200 ok`. Used by load balancers and Docker healthchecks.

---

## Observability

Prometheus metrics are scraped from `:8080/metrics`. A pre-provisioned Grafana dashboard is available at `localhost:3000` (admin / admin) after `make up`.

Structured logs are emitted via the `tracing` crate. Set `RUST_LOG=server=debug` for verbose output.

---

## Design decisions

A full record of every architectural decision — ingestion model, flush triggers, delivery guarantees, storage layout, query strategy, backpressure — is in [`.claude/grill-session.md`](.claude/grill-session.md).

The short version:

- **Dual-trigger flush:** 64 MB or 50k records or 1–2s wall-clock, whichever comes first. Keeps file sizes predictable under both high and low throughput.
- **Per-partition Kafka workers:** eliminates cross-partition coordination; each worker owns its offset commit.
- **SQLite over a bespoke catalog:** covering indexes on `(service, time_bucket)` make file pruning fast; WAL mode keeps reads non-blocking.
- **DataFusion at query time:** no separate query service to operate. SQL is compiled to a physical plan that pushes predicates into Parquet row-group statistics.
