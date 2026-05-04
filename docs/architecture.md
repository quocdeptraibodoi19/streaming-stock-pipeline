# Architecture

## Overview

A real-time financial data streaming pipeline that simulates the patterns found in production fintech data platforms. The system ingests live market data, persists it through a transactional database, captures changes via CDC, fans them out through Kafka, and lands them in a columnar analytical store exposed by a serving API.

The primary learning goals are:
- CDC-based ingestion with Debezium
- Kafka topic design and consumer group management
- ClickHouse data modeling for time-series analytics
- Event-time vs processing-time correctness
- Operational edge cases: late data, connector restarts, schema evolution

---

## End-to-End Data Flow

```
                        ┌──────────────────────────────────────────┐
                        │              SOURCE LAYER                 │
                        │                                          │
   Binance WebSocket ───┤                                          │
   (crypto trades 24/7) │    Ingestion Service (Python)            │
                        │    writes structured rows into           │
   Alpaca Paper Trading─┤    PostgreSQL (the "source system")      │
   (stock OMS lifecycle)│                                          │
                        │    Synthetic Generator                   │
   Synthetic Generator──┤    injects edge cases on demand          │
   (fault injection)    │                                          │
                        └──────────────────┬───────────────────────┘
                                           │
                                    PostgreSQL WAL
                                           │
                        ┌──────────────────▼───────────────────────┐
                        │              CDC LAYER                    │
                        │                                          │
                        │    Debezium (via Kafka Connect)          │
                        │    reads WAL → emits change events       │
                        │    op: c (insert) / u (update)           │
                        │         / d (delete) / r (snapshot)      │
                        └──────────────────┬───────────────────────┘
                                           │
                        ┌──────────────────▼───────────────────────┐
                        │              TRANSPORT LAYER              │
                        │                                          │
                        │    Kafka                                 │
                        │    stock.public.trades    (6 partitions) │
                        │    stock.public.orders    (3 partitions) │
                        │    stock.public.symbols   (1 partition)  │
                        │    stock.public.price_alerts             │
                        └──────────────────┬───────────────────────┘
                                           │
                                  ClickHouse Kafka Engine
                                  (native consumer, no sink connector)
                                           │
                        ┌──────────────────▼───────────────────────┐
                        │           ANALYTICAL STORE                │
                        │                                          │
                        │    ClickHouse                            │
                        │    ├── trades        (MergeTree)         │
                        │    ├── orders        (ReplacingMergeTree)│
                        │    ├── symbols       (ReplacingMergeTree)│
                        │    ├── price_alerts  (ReplacingMergeTree)│
                        │    └── ohlcv_1m      (AggregatingMergeTree│
                        │                       via Materialized View│
                        └──────────────────┬───────────────────────┘
                                           │
                        ┌──────────────────▼───────────────────────┐
                        │             SERVING LAYER                 │
                        │                                          │
                        │    FastAPI                               │
                        │    ├── GET /quotes/latest                │
                        │    ├── GET /quotes/{symbol}/ohlcv        │
                        │    ├── GET /orders                       │
                        │    └── GET /alerts                       │
                        └──────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Source (crypto) | Binance WebSocket | Free, no auth, 24/7, high volume, event/processing time gap is natural |
| Source (stocks/OMS) | Alpaca paper trading | Free paper account, order lifecycle gives INSERT+UPDATE CDC events |
| Source (fault injection) | Synthetic generator | Controllable throughput, inject edge cases on demand |
| Transactional DB | PostgreSQL 16 | Logical replication (`pgoutput`) required by Debezium; WAL-based CDC |
| CDC | Debezium 2.x | Industry standard for PostgreSQL CDC; runs inside Kafka Connect |
| Message bus | Apache Kafka (KRaft mode) | No Zookeeper dependency; KRaft is the modern default |
| Schema registry | Confluent Schema Registry | Enforces schema evolution contracts; Avro serialization |
| Analytical store | ClickHouse | Materialized Views for real-time pre-aggregation; native Kafka engine |
| Serving layer | FastAPI | Async, fast, Pydantic models match ClickHouse schema |
| Observability | Prometheus + Grafana | Kafka consumer lag, ClickHouse query latency, ingestion throughput |
| Container runtime | Docker Compose | Local dev; each service has its own Dockerfile |

---

## Source Systems

### Why PostgreSQL sits between the API and Kafka

In production, CDC tools like Debezium never connect directly to an external API. The pattern is:

```
External API → Application writes to transactional DB → Debezium reads WAL → Kafka
```

This project follows that pattern. The ingestion service is the "application"; PostgreSQL is the "source of truth system"; Debezium captures what changed. This mirrors how a real brokerage's OMS, CRM, or trading system would be wired.

### Source table design

| Table | Source | CDC event types | Notes |
|-------|--------|----------------|-------|
| `trades` | Binance WebSocket | INSERT only | Immutable facts — a trade cannot be un-traded |
| `orders` | Alpaca paper trading | INSERT + UPDATE | Status transitions: NEW → FILLED / CANCELLED |
| `symbols` | Alpaca metadata | INSERT + UPDATE + DELETE | Slowly changing; DELETE on delisting |
| `price_alerts` | This project's API | INSERT + UPDATE + DELETE | User-managed; full lifecycle |

Having all three CDC event types across different tables is intentional — it exercises Debezium's full `op` field range.

---

## Kafka Topic Design

Topics are pre-created (not auto-created) to enforce explicit partition and retention settings.

| Topic | Partitions | Retention | Cleanup policy | Reasoning |
|-------|-----------|-----------|---------------|-----------|
| `stock.public.trades` | 6 | 7 days | delete | High volume; 6 partitions for parallel consumers |
| `stock.public.orders` | 3 | 30 days | delete | Lower volume; longer retention for audit |
| `stock.public.symbols` | 1 | forever | compact | Low volume; compaction keeps only latest state per symbol key |
| `stock.public.price_alerts` | 1 | forever | compact | User config; compaction mirrors ReplacingMergeTree semantics |

**Partition key**: Debezium uses the primary key of the source table as the Kafka message key. This ensures all events for a given `order_id` or `symbol` land on the same partition, preserving order per entity.

---

## CDC Configuration (Debezium)

Debezium connects to PostgreSQL's logical replication stream via the `pgoutput` plugin (built into PostgreSQL 10+, no extension install needed).

Key configuration decisions:

| Setting | Value | Why |
|---------|-------|-----|
| `plugin.name` | `pgoutput` | Native, no extra PostgreSQL extension |
| `slot.name` | `debezium_slot` | One replication slot shared across tables |
| `publication.auto.create.mode` | `filtered` | Only publish tables explicitly listed |
| `transforms` | `ExtractNewRecordState` | Flattens the Debezium envelope (before/after/op) into a flat record |
| `transforms.unwrap.delete.handling.mode` | `rewrite` | Adds `__deleted=true` field instead of a tombstone for DELETE events |
| `decimal.handling.mode` | `precise` | Preserves financial precision; `double` loses precision on prices |

The `ExtractNewRecordState` (unwrap) SMT is critical — without it, every Kafka message contains a nested `{before: {...}, after: {...}, op: "u", source: {...}}` envelope that ClickHouse cannot consume natively.

---

## ClickHouse Data Model

### Table engine selection

| Table | Engine | Reason |
|-------|--------|--------|
| `trades` | `MergeTree` | Append-only; no deduplication or mutation needed |
| `orders` | `ReplacingMergeTree(updated_at)` | Updates arrive as new rows; engine deduplicates by keeping latest `updated_at` per `order_id` |
| `symbols` | `ReplacingMergeTree(updated_at)` | Same pattern; `_deleted` flag handles DELETEs |
| `price_alerts` | `ReplacingMergeTree(updated_at)` | Same pattern |
| `ohlcv_1m` | `AggregatingMergeTree` | Fed by a Materialized View; stores partial aggregation states |

### Kafka engine tables

ClickHouse's native Kafka engine acts as a consumer. Each source topic gets a pair:

```
kafka_<table>_raw   (Kafka engine — reads from topic)
         ↓
kafka_<table>_mv    (Materialized View — moves rows to real table)
         ↓
<table>             (real MergeTree table — queryable)
```

This two-step pattern is required because the Kafka engine table itself is not queryable for analytics — it only acts as a consumer buffer.

### Key ordering and partitioning

```sql
-- trades: ordered by symbol + event time (not ingested_at)
ORDER BY (symbol, trade_ts)
PARTITION BY toYYYYMM(trade_ts)

-- orders: ordered by order_id for ReplacingMergeTree deduplication
ORDER BY order_id
```

**Always use `trade_ts` (event time), not `ingested_at` (processing time)** in `ORDER BY`. Windowing on processing time produces wrong OHLCV bars when there is any lag between the exchange and your pipeline.

### Materialized View for OHLCV

```sql
CREATE MATERIALIZED VIEW ohlcv_1m_mv
ENGINE = AggregatingMergeTree()
ORDER BY (symbol, bucket)
AS SELECT
    symbol,
    toStartOfMinute(trade_ts)    AS bucket,
    argMinState(price, trade_ts) AS open,
    maxState(price)              AS high,
    minState(price)              AS low,
    argMaxState(price, trade_ts) AS close,
    sumState(volume)             AS volume
FROM trades
GROUP BY symbol, bucket;
```

Query with `argMinMerge`, `maxMerge`, etc. to finalize the aggregation states.

---

## Time Concepts in This Pipeline

Every event carries multiple timestamps. Using the wrong one produces incorrect analytics.

```
10:00:00.000  trade_ts      ← when exchange matched the trade   USE THIS for analytics
10:00:00.023  vendor_ts     ← when Binance sent it to us
10:00:00.089  received_at   ← when ingestion service got it
10:00:00.102  ingested_at   ← when PostgreSQL row was written
10:00:00.134  kafka_ts      ← when Kafka received the message
10:00:00.201  processed_at  ← when ClickHouse stored it
```

`trade_ts` is the only timestamp that reflects real-world event ordering. The gap between `trade_ts` and `ingested_at` is where **late-arriving events** live — a tick with a `trade_ts` 30 seconds old that arrives at ClickHouse after newer ticks. The pipeline must handle this without corrupting OHLCV bars.

---

## Edge Cases to Handle

| Edge case | Where it occurs | Handling strategy |
|-----------|----------------|------------------|
| Late-arriving events | `trade_ts` older than latest processed | ClickHouse `ORDER BY trade_ts` — MergeTree inserts out-of-order correctly |
| Duplicate trades | Finnhub resends on reconnect | Dedup on `trade_id` at ingestion; ClickHouse `ReplacingMergeTree` as backup |
| Debezium connector restart | Slot exists, connector re-registers | Replication slot persists; connector resumes from last offset |
| Replication slot bloat | Long Debezium downtime | Monitor `pg_replication_slots`; alert on slot lag > N GB |
| ClickHouse `ReplacingMergeTree` reads before merge | Query returns duplicate rows | Always query with `FINAL` or use `max(updated_at)` GROUP BY |
| Kafka consumer lag spike | Slow ClickHouse or network | Monitor consumer group lag; alert when lag > 10k messages |
| PostgreSQL schema change | Add nullable column to `trades` | Debezium + Schema Registry handle additive changes; breaking changes require connector pause |
| Bulk insert (10k+ rows) | Synthetic generator load test | Kafka handles backpressure; ClickHouse batch inserts via async consumer |
| Market data gap (Binance downtime) | WebSocket disconnect | Ingestion service implements exponential backoff reconnect |

---

## Observability

### Metrics to track

| Metric | Source | Alert threshold |
|--------|--------|----------------|
| Kafka consumer group lag | Kafka JMX / Prometheus | > 10,000 messages |
| Debezium replication slot lag | PostgreSQL `pg_replication_slots` | > 1 GB |
| ClickHouse insert rate | ClickHouse system tables | Drop > 50% from baseline |
| ClickHouse query p99 latency | ClickHouse system tables | > 500ms |
| Ingestion service reconnects | Application metrics | > 3 reconnects/minute |
| Dead letter queue depth | Kafka topic | Any message |

### Key Grafana dashboards

- **Pipeline health**: end-to-end latency from `trade_ts` to ClickHouse insert
- **Kafka**: topic throughput, consumer lag per group
- **ClickHouse**: parts count, merge rate, query latency
- **Ingestion**: WebSocket reconnects, rows/sec written to PostgreSQL

---

## Folder Structure

```
streaming-stock-pipeline/
├── infra/
│   ├── docker-compose.yml
│   ├── docker-compose.override.yml
│   ├── postgres/
│   │   └── init.sql
│   ├── kafka/
│   │   └── topics.yml
│   ├── debezium/
│   │   └── connectors/
│   │       ├── trades.json
│   │       └── orders.json
│   ├── clickhouse/
│   │   └── migrations/
│   │       ├── 001_trades.sql
│   │       ├── 002_orders.sql
│   │       ├── 003_materialized_views.sql
│   │       └── 004_kafka_engine.sql
│   ├── grafana/
│   │   └── dashboards/
│   └── prometheus/
│       ├── prometheus.yml
│       └── alerts.yml
│
├── services/
│   ├── ingestion/          # Binance/Alpaca WebSocket → PostgreSQL
│   ├── generator/          # Synthetic data + edge case injection
│   └── api/                # FastAPI serving layer
│
├── scripts/
│   ├── register_connectors.sh
│   ├── reset_pipeline.sh
│   └── load_test.sh
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── docs/
│   └── architecture.md     # this file
│
├── .env.example
├── Makefile
├── pyproject.toml
└── README.md
```

---

## Build Order

| Phase | Deliverable | Validates |
|-------|------------|-----------|
| 1 | `infra/docker-compose.yml` — full stack up, all services healthy | Infrastructure foundation |
| 2 | `infra/postgres/init.sql` — schema + WAL + publications | Source system is CDC-ready |
| 3 | `services/ingestion/` — Binance WS writing to PostgreSQL | Data is flowing into source |
| 4 | `infra/debezium/` — connector registered, events in Kafka | CDC is capturing changes |
| 5 | `infra/clickhouse/migrations/` — tables + Kafka engine + MVs | Data lands in analytical store |
| 6 | `services/api/` — FastAPI queries ClickHouse | Serving layer is working |
| 7 | `infra/prometheus/` + `infra/grafana/` — dashboards live | Pipeline is observable |
| 8 | `services/generator/scenarios/` — inject edge cases | Pipeline handles failure modes |
