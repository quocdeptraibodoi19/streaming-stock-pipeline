# Business Requirements

## Purpose

This pipeline exists to evaluate **paper trading strategy performance** on Alpaca against real market conditions sourced from Binance.

Alpaca is the trading playground — you control it. Binance is the benchmark — you measure against it. Everything downstream (Kafka, ClickHouse, the API) exists to answer one core question:

> **Did my trades on Alpaca execute well relative to what the market was actually doing?**

---

## Data Sources and Their Business Role

| Source | Data | Role |
|--------|------|------|
| Alpaca paper trading | `orders`, `symbols` | **Your plays** — the thing you control |
| Binance WebSocket | `trades` | **The market** — the benchmark you measure against |
| This project's API | `price_alerts` | **Your rules** — triggers and watchlist conditions |

### Alpaca — the playground

Alpaca provides a paper trading account where you can place and manage orders without real money. Orders have a full lifecycle:

```
NEW → PARTIAL_FILL → FILLED
              └────→ CANCELLED
```

This lifecycle is the core transactional data. Every status transition is a business event worth capturing.

`symbols` tracks the catalog of tradeable assets — whether they are active, delisted, or suspended. It provides the reference dimension that orders and trades join against.

### Binance — the market benchmark

Binance WebSocket streams every real market trade happening on the exchange in real time. These are not your trades — they are the aggregate activity of all participants in the market.

This data answers: *what was the true market price and volume at any given millisecond?* Without this, Alpaca fill prices have no context.

### Price alerts — your intent layer

Price alerts are user-defined threshold rules (e.g., "alert me when BTC crosses $70,000"). They represent trading intent — conditions you are watching for before acting.

---

## Core Business Questions

These are the analytics this pipeline is built to answer.

### 1. Slippage — did I get a good fill?

Compare your Alpaca order fill price against the actual market price at the moment of execution.

```
orders.fill_price  vs  trades WHERE symbol matches AND trade_ts ≈ orders.filled_at
```

A fill significantly worse than market price indicates poor execution timing or low liquidity.

### 2. Execution timing — did I enter at the right moment?

Compare your order placement time against the OHLCV bar at that time window.

```
orders.created_at  →  1-minute OHLCV bar from trades
```

Entering during low volume or at the top of a spike are signals that strategy timing needs adjustment.

### 3. Alert effectiveness — are my triggers well-calibrated?

When a price alert fires, did the price continue in the expected direction or reverse?

```
price_alerts  →  trades WHERE trade_ts > alert_triggered_at  (next N minutes)
```

This tells you whether your alert thresholds are predictive or just noise.

### 4. Symbol-level P&L — which plays performed?

Aggregate filled orders per symbol to compute realized P&L.

```
orders WHERE status = 'FILLED'  →  GROUP BY symbol  →  buy_avg, sell_avg, net
```

### 5. Market context at fill time — what was the market doing when I traded?

Join filled orders against OHLCV bars to see if fills happened during trending or ranging conditions.

---

## Ingestion Architecture

Based on data mutability, ingestion is split into two methods:

### CDC ingestion (via PostgreSQL + Debezium)

Used for data that **mutates** — orders, symbols, price alerts.

```
Alpaca / API  →  Ingestion Service  →  PostgreSQL  →  WAL  →  Debezium  →  Kafka
```

PostgreSQL is the source of truth for current state. Debezium captures every INSERT, UPDATE, and DELETE from the transaction log and emits them as change events into Kafka. This pattern mirrors how production OMS and CRM systems are wired.

### Direct streaming ingestion (WebSocket → Kafka)

Used for data that is **INSERT only and high volume** — market trades from Binance.

```
Binance WebSocket  →  Streaming Ingestion Service  →  Kafka (directly)
```

PostgreSQL is not involved here. Trades are immutable facts — they are never updated or deleted — so CDC adds no value and only increases latency and storage overhead.

### Why the split

| | CDC path | Streaming path |
|---|---|---|
| Data | orders, symbols, alerts | trades |
| Mutation | INSERT + UPDATE + DELETE | INSERT only |
| Volume | Low-medium | High |
| Durability buffer | PostgreSQL WAL | Kafka producer retries |
| Failure isolation | Debezium can lag, trades still flow | Orders safe in Postgres if Kafka is down |

The two paths have independent failure domains. A Debezium restart does not interrupt trade ingestion. A Kafka blip does not lose order data — it is safely committed in PostgreSQL and replayed when Debezium reconnects.

---

## Kafka Topic Design

| Topic | Source | Partitions | Ingestion method |
|-------|--------|-----------|-----------------|
| `stock.public.trades` | Binance | 6 | Direct streaming |
| `stock.public.orders` | Alpaca | 3 | CDC |
| `stock.public.symbols` | Alpaca | 1 | CDC |
| `stock.public.price_alerts` | API | 1 | CDC |

Trades get the most partitions because they are the highest volume and benefit most from parallel consumption into ClickHouse.

---

## Analytical Store (ClickHouse)

ClickHouse is the query layer for all business questions above. Data lands here from Kafka via the native Kafka engine and materialized views.

The `ohlcv_1m` materialized view pre-aggregates trades into 1-minute OHLCV bars in real time — this is the primary structure used to answer execution timing and alert effectiveness questions.

---

## End-to-End Data Flow

```
┌──────────────────────────────────────────────────────────┐
│                      SOURCE LAYER                        │
│                                                          │
│  Binance WebSocket ──► Streaming Ingestion ─────────────┼──► Kafka (trades)
│                                                          │
│  Alpaca Orders/Symbols ──► CDC Ingestion ──► PostgreSQL ─┼──► Debezium ──► Kafka (orders, symbols)
│                                                          │
│  Price Alert API ────────► CDC Ingestion ──► PostgreSQL ─┼──► Debezium ──► Kafka (alerts)
│                                                          │
└──────────────────────────────────────────────────────────┘
                              │
              ┌───────────────▼───────────────┐
              │         TRANSPORT LAYER        │
              │    Kafka (KRaft, no Zookeeper) │
              └───────────────┬───────────────┘
                              │
              ┌───────────────▼───────────────┐
              │       ANALYTICAL STORE         │
              │  ClickHouse                    │
              │  ├── trades (MergeTree)         │
              │  ├── orders (ReplacingMergeTree)│
              │  ├── symbols (ReplacingMergeTree│
              │  ├── price_alerts (Replacing..) │
              │  └── ohlcv_1m (AggregatingMV)  │
              └───────────────┬───────────────┘
                              │
              ┌───────────────▼───────────────┐
              │         SERVING LAYER          │
              │  FastAPI                       │
              │  ├── GET /quotes/latest         │
              │  ├── GET /quotes/{symbol}/ohlcv │
              │  ├── GET /orders                │
              │  └── GET /alerts               │
              └───────────────────────────────┘
```
