# Data Model

## Table of Contents

- [Overview](#overview)
- [Bronze Tables](#bronze-tables)
  - [orders.raw](#ordersraw)
  - [customers.raw](#customersraw)
  - [products.raw](#productsraw)
  - [returns.raw](#returnsraw)
- [Silver Tables](#silver-tables)
  - [dim_customers (SCD2)](#dim_customers-scd2)
  - [dim_products (SCD1)](#dim_products-scd1)
  - [dim_regions (Truncate and Load)](#dim_regions-truncate-and-load)
  - [fct_orders (Fact)](#fct_orders-fact)
  - [fct_returns (Fact)](#fct_returns-fact)
- [Gold Tables](#gold-tables)
  - [fct_customer_metrics](#fct_customer_metrics)
  - [fct_product_performance](#fct_product_performance)
  - [fct_revenue_daily](#fct_revenue_daily)
- [Foreign Key Relationships](#foreign-key-relationships)
- [Audit Columns Reference](#audit-columns-reference)

---

## Overview

This document describes every table in the ShopStream Databricks Data Platform: what it contains, the columns, types, and what each column means in plain language.

**Naming convention:**

- `dim_` prefix = dimension table (describes entities: who is a customer, what is a product)
- `fct_` prefix = fact table (records events: an order was placed, a return was filed)
- `_at` suffix = a timestamp column
- `_date` suffix = a date-only column (no time)
- `_` prefix = an audit/system column (not from the source data)

---

## Bronze Tables

Bronze tables are raw — exactly what arrived, no transformations. They are append-only: rows are never updated or deleted.

### orders.raw

**Catalog:** `helix_bronze` | **Full name:** `helix_bronze.orders.raw`
**Ingestion:** Structured Streaming from Azure Event Hubs (real-time)

| Column | Type | Meaning |
|---|---|---|
| `order_id` | STRING | Unique order ID from ShopStream checkout (e.g. `ORD7A3B9F2`) |
| `customer_id` | STRING | Customer who placed the order (FK to `dim_customers`) |
| `product_id` | STRING | Product ordered (FK to `dim_products`) |
| `product_category` | STRING | Category of the product (e.g. `electronics`, `clothing`) |
| `quantity` | STRING | Number of units ordered (stored as string in Bronze — cast in Silver) |
| `amount` | STRING | Order total in EUR (stored as string in Bronze — cast to decimal in Silver) |
| `region` | STRING | Region where the customer is located (e.g. `nl-west`) |
| `status` | STRING | Order status at event time: `placed` or `confirmed` |
| `event_timestamp` | STRING | When the order was placed (ISO 8601 string in Bronze) |
| `_ingested_at` | TIMESTAMP | When this row was written to Bronze by the pipeline |
| `_partition_date` | DATE | Partition column — equals `event_timestamp` truncated to date |

---

### customers.raw

**Catalog:** `helix_bronze` | **Full name:** `helix_bronze.customers.raw`
**Ingestion:** Lakeflow Connect CDC from PostgreSQL (nightly, 00:30 UTC)

| Column | Type | Meaning |
|---|---|---|
| `customer_id` | STRING | Unique customer ID (e.g. `C000042`) |
| `first_name` | STRING | Customer first name (PII — masked by Unity Catalog row filter in Silver) |
| `last_name` | STRING | Customer last name (PII) |
| `email` | STRING | Customer email address (PII) |
| `region` | STRING | Region the customer is in (e.g. `nl-central`) |
| `segment` | STRING | Customer value segment: `standard`, `premium`, or `vip` |
| `registered_at` | TIMESTAMP | When the customer registered on ShopStream |
| `date_of_birth` | DATE | Customer date of birth (PII) |
| `is_active` | STRING | `"true"` or `"false"` in Bronze (cast to boolean in Silver) |
| `_ingested_at` | TIMESTAMP | When this row was written to Bronze |
| `_source_file` | STRING | Which CSV file this row came from (Auto Loader fallback path only) |
| `_partition_date` | DATE | Partition column |

---

### products.raw

**Catalog:** `helix_bronze` | **Full name:** `helix_bronze.products.raw`
**Ingestion:** Lakeflow Connect CDC from PostgreSQL (nightly, 00:45 UTC)

| Column | Type | Meaning |
|---|---|---|
| `product_id` | STRING | Unique product ID (e.g. `P00123`) |
| `name` | STRING | Product display name |
| `category` | STRING | Top-level category (e.g. `electronics`) |
| `sub_category` | STRING | Sub-category (e.g. `smartphones`) |
| `brand` | STRING | Product brand |
| `base_price` | STRING | Listed price in EUR (cast to decimal in Silver) |
| `stock_quantity` | STRING | Current stock count (cast to int in Silver) |
| `is_active` | STRING | Whether this product is currently for sale |
| `_ingested_at` | TIMESTAMP | When this row was written to Bronze |
| `_partition_date` | DATE | Partition column |

---

### returns.raw

**Catalog:** `helix_bronze` | **Full name:** `helix_bronze.returns.raw`
**Ingestion:** Auto Loader (cloudFiles) from ADLS Gen2 `/raw/returns/` (nightly, 23:30 UTC)

| Column | Type | Meaning |
|---|---|---|
| `return_id` | STRING | Unique return ID (e.g. `RA3B9F22`) |
| `order_id` | STRING | The order this return is for (FK to `fct_orders`) |
| `customer_id` | STRING | Customer who filed the return (FK to `dim_customers`) |
| `product_id` | STRING | Product being returned (FK to `dim_products`) |
| `return_date` | DATE | Date the return was filed |
| `reason` | STRING | Why the customer returned it: `damaged`, `wrong_size`, `wrong_item`, `changed_mind`, `not_as_described`, `arrived_late` |
| `refund_amount` | DECIMAL(10,2) | Amount refunded in EUR |
| `status` | STRING | Return processing status: `pending`, `approved`, `rejected` |
| `_ingested_at` | TIMESTAMP | When Auto Loader wrote this row to Bronze |
| `_source_file` | STRING | Which CSV file this row came from |
| `_partition_date` | DATE | Partition column |

---

## Silver Tables

Silver tables are clean, typed, and modelled. They apply business rules and data quality checks on top of Bronze.

### dim_customers (SCD2)

**Catalog:** `helix_silver` | **Full name:** `helix_silver.customers.dim_customers`
**Strategy:** SCD Type 2 — keeps full history of changes to `segment`, `region`, `is_active`

| Column | Type | Meaning |
|---|---|---|
| `customer_id` | STRING | Unique customer ID |
| `first_name` | STRING | First name (PII) |
| `last_name` | STRING | Last name (PII) |
| `email` | STRING | Email (PII) |
| `region` | STRING | Region, lowercased and trimmed |
| `segment` | STRING | Segment: `standard`, `premium`, `vip` |
| `registered_at` | TIMESTAMP | When the customer first registered |
| `date_of_birth` | DATE | Date of birth (PII) |
| `is_active` | BOOLEAN | Whether the customer is active (typed from string) |
| `__START_AT` | TIMESTAMP | **SCD2 system column** — when this version became active |
| `__END_AT` | TIMESTAMP | **SCD2 system column** — when this version was superseded (`NULL` = current row) |
| `_ingested_at` | TIMESTAMP | Source timestamp used for ordering |

**How to read SCD2 data:**

- Get all customers **in their current state**: `WHERE __END_AT IS NULL`
- Get the **history** of one customer: `WHERE customer_id = 'C000042' ORDER BY __START_AT`
- **Point-in-time join** (what segment was this customer in on order date?):

```sql
SELECT o.order_id, o.amount, c.segment
FROM helix_silver.orders.fct_orders o
JOIN helix_silver.customers.dim_customers c
  ON o.customer_id = c.customer_id
 AND o.event_timestamp BETWEEN c.__START_AT AND COALESCE(c.__END_AT, current_timestamp())
```

---

### dim_products (SCD1)

**Catalog:** `helix_silver` | **Full name:** `helix_silver.products.dim_products`
**Strategy:** SCD Type 1 — overwrites with latest values, no history kept

| Column | Type | Meaning |
|---|---|---|
| `product_id` | STRING | Unique product ID |
| `name` | STRING | Product name |
| `category` | STRING | Category, lowercased |
| `sub_category` | STRING | Sub-category, lowercased |
| `brand` | STRING | Brand |
| `base_price` | DECIMAL(10,2) | Current price (overwrites old price on update) |
| `stock_quantity` | INT | Current stock count |
| `is_active` | BOOLEAN | Whether product is for sale |
| `_ingested_at` | TIMESTAMP | When the last update was ingested |

**Note:** With SCD1, if a product's price changes, the old price is gone. Use `helix_bronze.products.pricing_raw` if you need price history.

---

### dim_regions (Truncate and Load)

**Catalog:** `helix_silver` | **Full name:** `helix_silver.regions.dim_regions`
**Strategy:** Truncate and load — rebuilt completely every night from `data/reference/regions.csv`

| Column | Type | Meaning |
|---|---|---|
| `region_code` | STRING | Short code (e.g. `nl-west`) — used as FK in other tables |
| `region_name` | STRING | Display name (e.g. `West Netherlands`) |
| `country` | STRING | Two-letter country code (`NL`, `BE`, `DE`) |
| `timezone` | STRING | Timezone (e.g. `Europe/Amsterdam`) |
| `_loaded_at` | TIMESTAMP | When this version of the table was loaded |

---

### fct_orders (Fact)

**Catalog:** `helix_silver` | **Full name:** `helix_silver.orders.fct_orders`
**Strategy:** Fact table, append-only with dedup on `order_id`

| Column | Type | Meaning |
|---|---|---|
| `order_id` | STRING | Unique order ID |
| `customer_id` | STRING | FK to `dim_customers` |
| `product_id` | STRING | FK to `dim_products` |
| `product_category` | STRING | Category, lowercased |
| `quantity` | INT | Units ordered |
| `amount` | DECIMAL(12,2) | Order total in EUR |
| `region` | STRING | Customer region, trimmed |
| `status` | STRING | `placed` or `confirmed` |
| `event_timestamp` | TIMESTAMP | When the order was placed |
| `_cleaned_at` | TIMESTAMP | When Silver pipeline processed this row |
| `_partition_date` | DATE | Partition column — equals `event_timestamp` date |

---

### fct_returns (Fact)

**Catalog:** `helix_silver` | **Full name:** `helix_silver.returns.fct_returns`
**Strategy:** Fact table, append-only with dedup on `return_id`

| Column | Type | Meaning |
|---|---|---|
| `return_id` | STRING | Unique return ID |
| `order_id` | STRING | FK to `fct_orders` |
| `customer_id` | STRING | FK to `dim_customers` |
| `product_id` | STRING | FK to `dim_products` |
| `return_date` | DATE | Date filed |
| `reason` | STRING | Return reason, lowercased |
| `refund_amount` | DECIMAL(10,2) | EUR refunded |
| `status` | STRING | `pending`, `approved`, `rejected` |
| `_cleaned_at` | TIMESTAMP | When Silver pipeline processed this row |
| `_partition_date` | DATE | Partition column — equals `return_date` |

---

## Gold Tables

Gold tables are aggregated and optimised for analytical queries. They are what business teams and ML models read from.

### fct_customer_metrics

**Catalog:** `helix_gold` | **Full name:** `helix_gold.customers.fct_customer_metrics`
**Updated:** Nightly by `customer_metrics.py`. One row per `customer_id`.

| Column | Type | Meaning |
|---|---|---|
| `customer_id` | STRING | Customer ID |
| `region` | STRING | Current region |
| `segment` | STRING | Current segment |
| `is_active` | BOOLEAN | Whether currently active |
| `last_order_at` | TIMESTAMP | When they last placed an order |
| `days_since_last_order` | INT | How many days ago (lower = more recent = better) |
| `order_count_90d` | INT | Number of orders in the last 90 days (Frequency) |
| `total_spend_90d` | DECIMAL | Total EUR spent in the last 90 days (Monetary) |
| `avg_order_value_90d` | DECIMAL | Average order size in EUR, last 90 days |
| `clv_estimate` | DECIMAL | Simple annual CLV estimate: `avg_order_value * order_count_90d * 4` |
| `_computed_at` | TIMESTAMP | When this row was last calculated |

**RFM explanation:**

- **R** (Recency) = `days_since_last_order` — lower is better
- **F** (Frequency) = `order_count_90d` — higher is better
- **M** (Monetary) = `total_spend_90d` — higher is better

---

### fct_product_performance

**Catalog:** `helix_gold` | **Full name:** `helix_gold.products.fct_product_performance`
**Updated:** Nightly. One row per `product_id`.

| Column | Type | Meaning |
|---|---|---|
| `product_id` | STRING | Product ID |
| `name` | STRING | Product name |
| `category` | STRING | Category |
| `brand` | STRING | Brand |
| `base_price` | DECIMAL | Current price |
| `gmv_7d` | DECIMAL | Gross Merchandise Value last 7 days (EUR) |
| `units_7d` | INT | Units sold last 7 days |
| `gmv_prior_7d` | DECIMAL | GMV in the 7 days before that (days 8–14 ago) |
| `trend_score` | DECIMAL | `(gmv_7d - gmv_prior_7d) / gmv_prior_7d * 100` — positive means growing |
| `_computed_at` | TIMESTAMP | When this row was last calculated |

**trend_score interpretation:**

- `+25` = GMV grew 25% compared to the prior 7 days → trending up
- `-10` = GMV dropped 10% → trending down
- `0` = stable or no prior week data

---

### fct_revenue_daily

**Catalog:** `helix_gold` | **Full name:** `helix_gold.revenue.fct_revenue_daily`
**Updated:** Continuously by streaming (5-min micro-batch) + enriched nightly by batch.
**One row per (order_date, product_category, region).**

| Column | Type | Meaning |
|---|---|---|
| `order_date` | DATE | The calendar date |
| `product_category` | STRING | Product category |
| `region` | STRING | Customer region |
| `total_revenue` | DECIMAL | Total EUR revenue for this date + category + region |
| `order_count` | INT | Number of orders |
| `avg_order_value` | DECIMAL | Average order size in EUR |
| `_updated_at` | TIMESTAMP | When this row was last updated |

---

## Foreign Key Relationships

```
fct_orders.customer_id  →  dim_customers.customer_id
fct_orders.product_id   →  dim_products.product_id
fct_orders.region       →  dim_regions.region_code

fct_returns.order_id    →  fct_orders.order_id
fct_returns.customer_id →  dim_customers.customer_id
fct_returns.product_id  →  dim_products.product_id

fct_customer_metrics.customer_id →  dim_customers.customer_id (current row)
fct_product_performance.product_id → dim_products.product_id
```

**Note:** Delta Lake does not enforce foreign keys. These are logical relationships. Your Silver pipeline data quality checks (`@dlt.expect_or_drop`) catch the worst violations.

---

## Audit Columns Reference

Every table has system columns added by the pipeline (not from source data). They always start with `_` or `__`.

| Column | Meaning | Present in |
|---|---|---|
| `_ingested_at` | When the pipeline wrote this row to Bronze | All Bronze tables |
| `_source_file` | Which file Auto Loader read this row from | Auto Loader Bronze tables |
| `_partition_date` | Date partition for storage efficiency | All Bronze tables |
| `_cleaned_at` | When Silver pipeline processed this row | Silver fct_ tables |
| `_loaded_at` | When the truncate+load ran | dim_regions, dim_product_categories |
| `_computed_at` | When the Gold metric was last calculated | All Gold tables |
| `__START_AT` | SCD2: when this version became active | dim_customers |
| `__END_AT` | SCD2: when this version was replaced (NULL = current) | dim_customers |
