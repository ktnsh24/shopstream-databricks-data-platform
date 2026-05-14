# ShopStream Databricks Data Platform

## Table of Contents

- [What This Is](#what-this-is)
- [Your Goal](#your-goal)
- [The Business Story](#the-business-story)
- [What You Are Building](#what-you-are-building)
- [How This Connects to the Bigger Platform](#how-this-connects-to-the-bigger-platform)
- [Where to Start](#where-to-start)
- [Repo Structure](#repo-structure)
- [Technology Used](#technology-used)

---

## What This Is

**ShopStream Databricks Data Platform** is your standalone Databricks data engineering project.

You will build the full **Bronze → Silver → Gold medallion pipeline** for a fictional e-commerce company called **ShopStream**. When you are done, your Gold tables will be production-quality data that can feed machine learning models, AI agents, and business dashboards.

You work on this repo independently. You do not need to look at or understand the AI and API parts of the Helix project. Your job is to get clean, reliable data into the Gold layer.

---

## Your Goal

By the end of this project, you will have built:

| What | Where it lives |
|---|---|
| Raw customer data ingested nightly | `helix_bronze.customers.raw` |
| Raw product data ingested nightly | `helix_bronze.products.raw` |
| Raw return data ingested from files | `helix_bronze.returns.raw` |
| Real-time order stream | `helix_bronze.orders.raw` |
| Customer dimension with full change history | `helix_silver.customers.dim_customers` |
| Product dimension with latest state | `helix_silver.products.dim_products` |
| Region and category reference tables | `helix_silver.regions.dim_regions` |
| Validated orders fact table | `helix_silver.orders.fct_orders` |
| Validated returns fact table | `helix_silver.returns.fct_returns` |
| Customer RFM scores + CLV | `helix_gold.customers.fct_customer_metrics` |
| Product performance metrics | `helix_gold.products.fct_product_performance` |
| Daily revenue aggregates | `helix_gold.revenue.fct_revenue_daily` |

---

## The Business Story

**ShopStream** is an e-commerce company selling electronics, clothing, home goods, and more across Netherlands, Belgium, and Germany.

They have three business teams — Revenue, Customer, and Product — who need reliable data to answer questions every morning:

- "What was last week's revenue by region and product category?"
- "Which customer segments are at risk of churning?"
- "Which products are trending and which are declining?"

Your job: build the data pipeline that makes those answers possible.

---

## What You Are Building

### Two data paths run in parallel

**Real-time path (orders):**

```
Customer places an order
    → Azure Event Hubs (Kafka)
    → Structured Streaming (always-on pipeline)
    → helix_bronze.orders.raw
    → helix_silver.orders.fct_orders  (validate + dedup)
    → helix_gold.revenue.fct_revenue_daily  (live aggregation)
```

**Batch path (customers, products, returns):**

```
Every night at 01:00 UTC:
    ShopStream PostgreSQL DB  → Lakeflow Connect → helix_bronze.customers.raw
    ShopStream PostgreSQL DB  → Lakeflow Connect → helix_bronze.products.raw
    Refund system drops CSV   → Auto Loader      → helix_bronze.returns.raw

    Bronze → Silver:
        customers  → dim_customers  (SCD Type 2 — keeps history)
        products   → dim_products   (SCD Type 1 — latest state only)
        regions    → dim_regions    (Truncate and load — small CSV)
        returns    → fct_returns    (Fact table — append-only)

    Silver → Gold:
        fct_customer_metrics    (RFM scores, CLV estimate)
        fct_product_performance (GMV, trend score)
        fct_revenue_daily       (enriched with customer segment + product brand)
```

---

## How This Connects to the Bigger Platform

Once your Gold tables are ready, they are used directly by:

- **AI Agent** — answers natural-language business questions by querying your Gold tables
- **ML Models** — churn prediction + revenue forecasting trained on your features
- **API endpoints** — the `/v1/metrics` and `/v1/forecast` APIs read from your Gold tables
- **Business dashboards** — BI tools connect to your Gold tables via Delta Sharing

You don't build any of those — but your work feeds all of them. The table names you use here (`helix_gold.*`) match exactly what the full platform expects.

---

## Where to Start

**Step 1:** Read the docs in this order:
1. [docs/architecture-and-design/system-design.md](docs/architecture-and-design/system-design.md) — understand the full picture
2. [docs/architecture-and-design/data-model.md](docs/architecture-and-design/data-model.md) — understand every table and column
3. [docs/setup-and-tooling/getting-started.md](docs/setup-and-tooling/getting-started.md) — set up your environment

**Step 2:** Follow the learning order in [docs/reading-order.md](docs/reading-order.md)

**Step 3:** Work through the 13 hands-on labs in [docs/hands-on-labs/](docs/hands-on-labs/)

---

## Repo Structure

```
shopstream-databricks-data-platform/
│
├── data_platform/                    ← ALL your pipeline code lives here
│   ├── pipelines/
│   │   ├── bronze/                   ← Bronze ingest (raw data in)
│   │   │   ├── ingest_customers_batch.py     Auto Loader fallback for customers CSV
│   │   │   ├── ingest_products_batch.py      Auto Loader fallback for products CSV
│   │   │   ├── ingest_orders_streaming.py    Structured Streaming from Event Hubs
│   │   │   └── ingest_returns_autoloader.py  Auto Loader for returns CSV from RMS
│   │   ├── silver/                   ← Silver transforms (clean + model)
│   │   │   ├── dim_customers_scd2.py          SCD Type 2 — customer history
│   │   │   ├── dim_products_scd1.py           SCD Type 1 — product latest state
│   │   │   ├── dim_regions_truncate_load.py   Truncate and load — reference dims
│   │   │   ├── fct_orders.py                  Fact table — orders (streaming)
│   │   │   └── fct_returns.py                 Fact table — returns (batch)
│   │   └── gold/                     ← Gold aggregations (business-ready)
│   │       ├── customer_metrics.py            RFM + CLV per customer
│   │       ├── product_performance.py         GMV + trend score per product
│   │       └── revenue_daily.py               Daily revenue (streaming + batch)
│   ├── ingestion/                    ← Lakeflow Connect configs (DB → Bronze)
│   ├── jobs/                         ← Lakeflow Job schedules
│   ├── optimizations/                ← Delta OPTIMIZE + broadcast join examples
│   ├── sql/                          ← SQL for dashboards and alerts
│   └── notebooks/                    ← Exploration notebooks
│
├── data_generators/                  ← Generate fake ShopStream data for labs
│   ├── generate_customers.py
│   ├── generate_orders.py
│   └── generate_returns.py
│
├── data/reference/                   ← Small reference CSVs (regions, categories)
├── terraform/                        ← Azure + Databricks infrastructure
├── docs/                             ← All documentation
│   ├── reading-order.md              ← START HERE
│   ├── architecture-and-design/
│   ├── setup-and-tooling/
│   └── hands-on-labs/                ← 13 practical labs
│
├── .env.example                      ← Copy to .env and fill in your values
└── databricks.bundle.yml             ← Databricks Asset Bundle config
```

---

## Technology Used

| Technology | What it does in this project |
|---|---|
| **Azure Databricks** | The compute platform — runs all your pipelines |
| **Delta Lake** | The storage format — all your tables are Delta tables |
| **Unity Catalog** | Governance — manages access, lineage, and schema for all tables |
| **Lakeflow Connect** | Pulls data from PostgreSQL directly into Delta (no files needed) |
| **Lakeflow SDP** | Runs your Silver and Gold transform pipelines (Spark + DLT) |
| **Auto Loader** (`cloudFiles`) | Reads new CSV/Parquet files from ADLS Gen2 as they arrive |
| **Structured Streaming** | Reads real-time order events from Azure Event Hubs (Kafka) |
| **Azure ADLS Gen2** | Cloud storage — raw files land here before Auto Loader reads them |
| **Azure Event Hubs** | Kafka-compatible message queue — orders arrive here in real-time |
| **Azure Key Vault** | Stores secrets (DB passwords, connection strings) securely |
| **Databricks Asset Bundles** | Deploys all resources (pipelines, jobs) as code |
| **Terraform** | Provisions Azure + Databricks infrastructure |
| **Python / PySpark** | Language used for all pipeline code |
| **loguru** | Logging library used in all pipelines |
