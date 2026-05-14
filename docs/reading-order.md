# Reading Order

> **Start here.** This page tells you what to read and in what order.
> Total reading time before your first lab: ~60 minutes.

---

## Table of Contents

- [Step 1 — Understand What You Are Building](#step-1--understand-what-you-are-building)
- [Step 2 — Set Up Your Environment](#step-2--set-up-your-environment)
- [Step 3 — Learn the Data Model](#step-3--learn-the-data-model)
- [Step 4 — Read the Pipeline Code](#step-4--read-the-pipeline-code)
- [Step 5 — Run the Labs](#step-5--run-the-labs)
- [All Documents Index](#all-documents-index)

---

## Step 1 — Understand What You Are Building

Read this first. Takes ~20 minutes.

1. [README.md](../README.md) — What this project is and what you will build
2. [docs/architecture-and-design/system-design.md](architecture-and-design/system-design.md) ⭐ **The most important document**
   - The two data paths (real-time orders vs nightly batch)
   - The full data flow diagram from source to Gold
   - The medallion architecture: Bronze, Silver, Gold
   - The four data modelling strategies: SCD1, SCD2, Truncate+Load, Fact
   - The three ingestion patterns: Lakeflow Connect, Auto Loader, Structured Streaming

---

## Step 2 — Set Up Your Environment

Before you run any pipeline or lab, follow this guide:

1. [docs/setup-and-tooling/getting-started.md](setup-and-tooling/getting-started.md)
   - Python setup
   - Azure infrastructure (Terraform)
   - Databricks workspace + Unity Catalog
   - Uploading reference data
   - Generating test data
   - Deploying pipelines

---

## Step 3 — Learn the Data Model

Once you understand the architecture, understand the tables:

1. [docs/architecture-and-design/data-model.md](architecture-and-design/data-model.md)
   - Every table: name, catalog, ingestion method
   - Every column: type, plain-language meaning
   - Foreign key relationships between tables
   - Audit columns (`_ingested_at`, `__START_AT`, `__END_AT`, etc.)

---

## Step 4 — Read the Pipeline Code

Read the actual pipeline files in this order. Each file has a detailed explanation at the top — read those comments carefully before looking at the code.

### Bronze (ingest layer)

Read in this order:

1. `data_platform/pipelines/bronze/ingest_customers_batch.py` — Auto Loader for CSV files (fallback)
2. `data_platform/pipelines/bronze/ingest_returns_autoloader.py` ⭐ — **The main Auto Loader example**. Read the top comment carefully — it explains exactly when to use Auto Loader vs Lakeflow Connect.
3. `data_platform/pipelines/bronze/ingest_orders_streaming.py` — Structured Streaming from Event Hubs
4. `data_platform/ingestion/customers_lakeflow.yml` — Lakeflow Connect config (no Python — just YAML)

### Silver (transform layer)

Read in this order — each file explains its strategy at the top:

1. `data_platform/pipelines/silver/fct_orders.py` — simplest Silver file, good starting point
2. `data_platform/pipelines/silver/fct_returns.py` — same pattern, Auto Loader source
3. `data_platform/pipelines/silver/dim_regions_truncate_load.py` ⭐ — Truncate and Load explained
4. `data_platform/pipelines/silver/dim_products_scd1.py` ⭐ — SCD Type 1 explained with comparison to SCD2
5. `data_platform/pipelines/silver/dim_customers_scd2.py` ⭐ — SCD Type 2 explained with query examples

### Gold (aggregation layer)

1. `data_platform/pipelines/gold/customer_metrics.py` — RFM scoring, window functions
2. `data_platform/pipelines/gold/product_performance.py` — trend score calculation
3. `data_platform/pipelines/gold/revenue_daily.py` — streaming UPSERT with foreachBatch

---

## Step 5 — Run the Labs

Work through the labs in order. Each lab has one learning objective.

1. [docs/hands-on-labs/hands-on-labs.md](hands-on-labs/hands-on-labs.md) — All 13 labs

**Lab order:**

| Lab | Name | What you learn |
|---|---|---|
| DP-01 | Auto Loader — Returns Ingest | How Auto Loader detects and reads only new files |
| DP-02 | Bronze Streaming Ingest | How Structured Streaming reads real-time events |
| DP-03 | SCD Type 2 — dim_customers | How history is tracked with `__START_AT` / `__END_AT` |
| DP-04 | SCD Type 1 — dim_products | How UPSERT overwrites in place with no history |
| DP-05 | Truncate and Load — dim_regions | How full-replace works for small reference tables |
| DP-06 | Data Quality — fct_orders | How `@dlt.expect_or_drop` drops bad rows |
| DP-07 | Gold Aggregations | How Silver feeds Gold via Change Data Feed |
| DP-08 | Delta Time Travel | How to query a table as it was in the past |
| DP-09 | Change Data Feed | How to read only what changed since last run |
| DP-10 | Point-in-Time Join | How SCD2 enables historically accurate joins |
| DP-11 | OPTIMIZE and Z-ORDER | How to make queries faster by sorting files |
| DP-12 | Broadcast Join + AQE | How Databricks optimises joins automatically |
| DP-13 | Delta Sharing | How to share Gold tables with external recipients |

---

## All Documents Index

| Document | What it covers |
|---|---|
| [README.md](../README.md) | Project overview, goals, repo structure |
| [docs/architecture-and-design/system-design.md](architecture-and-design/system-design.md) | Architecture, data flows, modelling strategies, ingestion patterns |
| [docs/architecture-and-design/data-model.md](architecture-and-design/data-model.md) | All table schemas, column definitions, FK relationships |
| [docs/setup-and-tooling/getting-started.md](setup-and-tooling/getting-started.md) | Environment setup, Terraform, Databricks, data generators |
| [docs/hands-on-labs/hands-on-labs.md](hands-on-labs/hands-on-labs.md) | All 13 hands-on labs |
