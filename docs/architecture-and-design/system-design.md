# System Design

## Table of Contents

- [What ShopStream Is](#what-shopstream-is)
- [The Two Data Paths](#the-two-data-paths)
- [Full Data Flow Diagram](#full-data-flow-diagram)
- [Medallion Architecture — Bronze, Silver, Gold](#medallion-architecture--bronze-silver-gold)
- [Data Modelling Strategy](#data-modelling-strategy)
- [Ingestion Patterns Used](#ingestion-patterns-used)
- [Scheduling and Timing](#scheduling-and-timing)
- [Unity Catalog Layout](#unity-catalog-layout)

---

## What ShopStream Is

ShopStream is a fictional e-commerce company selling electronics, clothing, home goods, and more in the Netherlands, Belgium, and Germany.

It has two main systems:

- **Main app database** — a PostgreSQL database where orders, customers, and products are stored
- **Refund Management System (RMS)** — a separate system that exports returns as a CSV file every night to cloud storage

Your job is to take data from both of these sources and bring it all the way through to clean, analytics-ready Gold tables.

---

## The Two Data Paths

There are two separate paths that bring data into the platform. They run independently and use completely different mechanisms.

### Path 1 — Real-time (orders)

```
Customer clicks "Place Order" on ShopStream website
    ↓
The checkout service publishes an OrderPlaced event to Azure Event Hubs
    ↓
Azure Event Hubs holds the event in a Kafka topic ("orders-stream")
    ↓
Structured Streaming reads from that Kafka topic (always-on, 24/7)
    ↓
helix_bronze.orders.raw  (new row within seconds)
    ↓
fct_orders.py  validates and deduplicates
    ↓
helix_silver.orders.fct_orders
    ↓
revenue_daily.py  aggregates every 5 minutes
    ↓
helix_gold.revenue.fct_revenue_daily  (updated throughout the day)
```

**Latency:** an order appears in the Gold table within ~5 minutes of being placed.

### Path 2 — Batch (customers, products, returns)

```
Every night (scheduled times below):

    Lakeflow Connect (00:30 UTC)
        → reads "customers" table from ShopStream PostgreSQL via JDBC
        → writes directly to helix_bronze.customers.raw
        → only NEW and CHANGED rows since yesterday (CDC)

    Lakeflow Connect (00:45 UTC)
        → reads "products" table from ShopStream PostgreSQL via JDBC
        → writes to helix_bronze.products.raw and helix_bronze.products.pricing_raw

    Auto Loader (23:30 UTC — triggered when RMS drops the file)
        → RMS exports returns_YYYYMMDD.csv to ADLS Gen2 /raw/returns/
        → Auto Loader detects the new file (only new files since last run)
        → writes to helix_bronze.returns.raw

    Silver pipeline (01:00 UTC — after Bronze is ready)
        → dim_customers_scd2.py  → helix_silver.customers.dim_customers  (SCD2)
        → dim_products_scd1.py   → helix_silver.products.dim_products    (SCD1)
        → dim_regions_truncate_load.py → helix_silver.regions.dim_regions (T+L)
        → fct_returns.py         → helix_silver.returns.fct_returns

    Gold pipeline (02:00 UTC — after Silver is ready)
        → customer_metrics.py    → helix_gold.customers.fct_customer_metrics
        → product_performance.py → helix_gold.products.fct_product_performance
        → revenue_daily.py       → helix_gold.revenue.fct_revenue_daily (enriched)
```

**Latency:** Gold tables are ready by 06:00 UTC every morning.

---

## Full Data Flow Diagram

```
═══════════════════════════════════════════════════════════════════════════
  REAL-TIME PATH (always-on)            BATCH PATH (nightly schedule)
═══════════════════════════════════════════════════════════════════════════

  ShopStream checkout                    PostgreSQL DB (ShopStream)
  publishes OrderPlaced (JSON)                   │
        │                               Lakeflow Connect (00:30 & 00:45 UTC)
        │ Kafka                         reads via JDBC, CDC only
        ▼                                        │
  Azure Event Hubs                               │  ADLS Gen2 /raw/returns/
  "orders-stream" topic                          │  RMS drops CSV at 23:00 UTC
        │                                        │       │
        │ Structured Streaming                   │  Auto Loader (23:30 UTC)
        │ (10-min watermark)                     │  detects new file only
        ▼                                        ▼       ▼
  ┌─────────────────────────┐         ┌──────────────────────────────────┐
  │  BRONZE                 │         │  BRONZE                          │
  │  orders.raw             │         │  customers.raw                   │
  │  (append-only)          │         │  products.raw                    │
  └────────────┬────────────┘         │  products.pricing_raw            │
               │                      │  returns.raw                     │
               │ fct_orders.py        └────────────────┬─────────────────┘
               │ validate + dedup                       │
               ▼                                        │ Silver pipeline (01:00 UTC)
  ┌─────────────────────────┐         ┌────────────────▼─────────────────┐
  │  SILVER                 │         │  SILVER                          │
  │  orders.fct_orders      │         │  customers.dim_customers  (SCD2) │
  └────────────┬────────────┘         │  products.dim_products    (SCD1) │
               │                      │  regions.dim_regions      (T+L)  │
               │ 5-min micro-batch    │  returns.fct_returns              │
               ▼                      └────────────────┬─────────────────┘
  ┌─────────────────────────┐                          │
  │  GOLD (live)            │                          │ Gold pipeline (02:00 UTC)
  │  revenue.fct_revenue_   │         ┌────────────────▼─────────────────┐
  │  daily (running totals) │         │  GOLD (batch, ready by 06:00 UTC)│
  └─────────────────────────┘         │  customers.fct_customer_metrics   │
                                       │  products.fct_product_performance │
                                       │  revenue.fct_revenue_daily_enrich │
                                       └──────────────────────────────────┘
═══════════════════════════════════════════════════════════════════════════
```

---

## Medallion Architecture — Bronze, Silver, Gold

This project uses the **Medallion Architecture**, a standard data engineering pattern.

| Layer | Purpose | Key characteristics |
|---|---|---|
| **Bronze** | Raw data exactly as it arrived — no changes | Append-only, schema enforced, partitioned by ingest date |
| **Silver** | Cleaned, validated, and modelled — business rules applied | Deduplicated, typed, null-handled, named with `dim_` or `fct_` prefix |
| **Gold** | Aggregated and ready for analytics and ML | Z-ordered for query speed, Change Data Feed enabled for Feature Store |

**Rule of thumb:**

- If you want to debug "what data came in on 2026-05-03?" → query Bronze
- If you want to answer "what is the current state of a customer?" → query Silver
- If you want to answer "what was last week's revenue by region?" → query Gold

---

## Data Modelling Strategy

Not all Silver tables are built the same way. Each table uses a strategy that matches how its source data behaves.

| Table | Strategy | Plain-language meaning |
|---|---|---|
| `dim_customers` | **SCD Type 2** | When a customer upgrades from Standard to Premium, keep the old row AND add a new row. History is never deleted. |
| `dim_products` | **SCD Type 1** | When a product's price changes, overwrite the old price. No history kept. |
| `dim_regions` | **Truncate and load** | Delete everything and reload from the CSV every night. Simple — the table only has 10 rows. |
| `dim_product_categories` | **Truncate and load** | Same as regions — 22 rows, static lookup. |
| `fct_orders` | **Fact (append-only + dedup)** | Orders never change after they are placed. Add new ones, deduplicate on `order_id`. |
| `fct_returns` | **Fact (append-only + dedup)** | Returns never change after filed. Same pattern as orders. |

**Why does it matter?**

If `dim_customers` used SCD1 (overwrite), you would lose the information that a customer was Standard when they placed an order last year. With SCD2, you can ask: "What segment was this customer in WHEN they placed this order?" — and get the correct historical answer.

---

## Ingestion Patterns Used

| Pattern | What it is | Used for |
|---|---|---|
| **Lakeflow Connect** | Databricks-managed connector that reads from a database via JDBC, with CDC | Customers and products from PostgreSQL |
| **Auto Loader** (`cloudFiles`) | Reads only NEW files that arrived in ADLS Gen2 since last run | Returns CSV from the refund system |
| **Structured Streaming** | Continuously reads events from a Kafka topic (Azure Event Hubs) | Real-time order events |

**Simple decision rule:**

- Data lives in a **database** → use Lakeflow Connect
- Data arrives as **files** → use Auto Loader
- Data arrives as a **real-time event stream** → use Structured Streaming

---

## Scheduling and Timing

```
23:00 UTC   Refund Management System drops returns CSV to ADLS Gen2
23:30 UTC   Auto Loader picks up returns CSV → helix_bronze.returns.raw
00:30 UTC   Lakeflow Connect loads customers → helix_bronze.customers.raw
00:45 UTC   Lakeflow Connect loads products  → helix_bronze.products.raw
01:00 UTC   Silver pipeline starts (waits for Bronze to finish)
02:00 UTC   Gold pipeline starts (waits for Silver to finish)
06:00 UTC   All Gold tables ready for business teams
```

The streaming pipeline runs continuously all day, independent of the batch schedule.

---

## Unity Catalog Layout

All tables are stored in Databricks Unity Catalog, organised into three catalogs:

```
helix_bronze/                     ← Raw, as-ingested data
  customers/
    raw                           (Lakeflow Connect CDC from PostgreSQL)
  products/
    raw                           (Lakeflow Connect CDC)
    pricing_raw                   (Lakeflow Connect CDC — separate table)
  orders/
    raw                           (Structured Streaming from Event Hubs)
  returns/
    raw                           (Auto Loader from ADLS Gen2)

helix_silver/                     ← Cleaned and modelled
  customers/
    dim_customers                 (SCD2 — full history)
  products/
    dim_products                  (SCD1 — latest state)
  regions/
    dim_regions                   (Truncate and load)
  product_categories/
    dim_product_categories        (Truncate and load)
  orders/
    fct_orders                    (Fact table — streaming)
  returns/
    fct_returns                   (Fact table — batch)

helix_gold/                       ← Business-ready aggregates
  customers/
    fct_customer_metrics          (RFM scores, CLV estimate)
  products/
    fct_product_performance       (GMV, trend score)
  revenue/
    fct_revenue_daily             (Daily totals by date + category + region)
```

The naming convention is `catalog.schema.table` — for example, `helix_silver.customers.dim_customers`.
