# ShopStream Databricks Data Platform — Project Standards

> This file applies to all work in this repo.
> VS Code Copilot loads it automatically for every file.

---

## What This Repo Is

- **Purpose:** Standalone Azure Databricks Data Engineering project — no AI, no ML, no API layer
- **Scope:** Bronze → Silver → Gold pipelines only
- **Business domain:** ShopStream (fictional e-commerce — orders, customers, products, returns)
- **Data sources:** PostgreSQL (customers, products via Lakeflow Connect) + ADLS Gen2 (returns CSV via Auto Loader) + Azure Event Hubs (orders via Structured Streaming)

## Relationship with shopstream-databricks-ai-platform

The two repos share Unity Catalog table names intentionally:

```
shopstream-databricks-data-platform writes  →  helix_gold.customers.fct_customer_metrics
shopstream-databricks-ai-platform reads     →  helix_gold.customers.fct_customer_metrics
```

**Never rename a Gold table without coordinating both repos.** The AI layer breaks silently if Gold table names drift.

---

## Unity Catalog Layout (source of truth)

| Catalog | Schema | Table | Written by |
|---|---|---|---|
| `helix_bronze` | `customers` | `raw` | Lakeflow Connect |
| `helix_bronze` | `products` | `raw` | Lakeflow Connect |
| `helix_bronze` | `orders` | `raw` | Structured Streaming (Event Hubs) |
| `helix_bronze` | `returns` | `raw` | Auto Loader (ADLS Gen2 CSV) |
| `helix_silver` | `customers` | `dim_customers` | DLT SCD2 (`dim_customers_scd2.py`) |
| `helix_silver` | `products` | `dim_products` | DLT SCD1 (`dim_products_scd1.py`) |
| `helix_silver` | `regions` | `dim_regions` | Truncate+Load (`dim_regions_truncate_load.py`) |
| `helix_silver` | `regions` | `dim_product_categories` | Truncate+Load (same file) |
| `helix_silver` | `orders` | `fct_orders` | DLT append (`fct_orders.py`) |
| `helix_silver` | `returns` | `fct_returns` | DLT append (`fct_returns.py`) |
| `helix_gold` | `customers` | `fct_customer_metrics` | DLT (`customer_metrics.py`) |
| `helix_gold` | `products` | `fct_product_performance` | DLT (`product_performance.py`) |
| `helix_gold` | `revenue` | `fct_revenue_daily` | Streaming + batch enrich (`revenue_daily.py`) |
| `helix_gold` | `orders` | `order_sentiment` | **Written by shopstream-databricks-ai-platform** — `ai_query()` enrichment, do NOT recreate here |

---

## Naming Conventions (MANDATORY — never break these)

### Table names

| Layer | Prefix | Example |
|---|---|---|
| Bronze | none — always called `raw` | `helix_bronze.customers.raw` |
| Silver — dimension | `dim_` | `dim_customers`, `dim_products`, `dim_regions` |
| Silver — fact | `fct_` | `fct_orders`, `fct_returns` |
| Gold | `fct_` | `fct_customer_metrics`, `fct_product_performance`, `fct_revenue_daily` |

**No Gold tables use `dim_` prefix.** Gold tables are always aggregated facts, never slowly changing dimensions.

### File names

| Layer | Pattern | Example |
|---|---|---|
| Bronze | `ingest_{entity}_{method}.py` | `ingest_returns_autoloader.py` |
| Silver — SCD2 | `dim_{entity}_scd2.py` | `dim_customers_scd2.py` |
| Silver — SCD1 | `dim_{entity}_scd1.py` | `dim_products_scd1.py` |
| Silver — Truncate+Load | `dim_{entity}_truncate_load.py` | `dim_regions_truncate_load.py` |
| Silver — Fact | `fct_{entity}.py` | `fct_orders.py` |
| Gold | `{entity}_{metric_type}.py` | `customer_metrics.py` |

---

## Data Modelling Strategies

Four strategies are used in Silver. Each one exists for a specific reason.

| Strategy | Used for | DLT API | Key property |
|---|---|---|---|
| SCD Type 2 | `dim_customers` | `dlt.apply_changes(stored_as_scd_type=2)` | Keeps full history. `__START_AT` / `__END_AT` on every row. |
| SCD Type 1 | `dim_products` | `dlt.apply_changes(stored_as_scd_type=1)` | Overwrites in place. No history. One row per key. |
| Truncate + Load | `dim_regions`, `dim_product_categories` | `spark.write.mode("overwrite")` | Full table replaced every run. Handles deletions naturally. |
| Fact (append) | `fct_orders`, `fct_returns` | `@dlt.table` + streaming read | Append-only. Dedup on primary key. No updates. |

### When to choose each

- **SCD2** — the entity has attributes that change over time AND you need to join it to history
- **SCD1** — the entity can change but you only care about the current state
- **Truncate+Load** — the table is small (< 10,000 rows) AND deletions must be reflected automatically
- **Fact** — rows are immutable events (orders happen once, returns happen once)

---

## Ingestion Patterns

| Pattern | Source | File | Key behaviour |
|---|---|---|---|
| Lakeflow Connect | PostgreSQL (`customers`, `products`) | `ingestion/*.yml` | No Python — YAML only. CDC from DB. |
| Auto Loader | ADLS Gen2 CSV (`returns`) | `bronze/ingest_returns_autoloader.py` | Reads only new files via checkpoint. Never re-reads. |
| Structured Streaming | Azure Event Hubs Kafka (`orders`) | `bronze/ingest_orders_streaming.py` | Always-on. Reads continuously. 10-minute watermark. |

### Auto Loader rules

- Checkpoint path must be unique per pipeline. Never share checkpoints across pipelines.
- `trigger(availableNow=True)` = batch semantics — processes all new files then stops. Use in nightly batch jobs.
- `_source_file` column must be added to every Bronze table produced by Auto Loader.

### Streaming pipeline rules

- **Always stop manually after lab use.** It does not auto-stop and costs money while running.
- Watermark = 10 minutes for orders. Do not lower this.

---

## Code Standards

### Python

- **Logging:** `loguru` only. Never `print()`. Never f-strings in log calls:

  ```python
  # CORRECT
  logger.info("Loaded %s rows from %s", row_count, source_file)

  # WRONG
  logger.info(f"Loaded {row_count} rows from {source_file}")
  ```

- **No bare `except:`** — always catch specific exceptions.
- **DLT pipelines:** never call `spark.read` directly in a `@dlt.table` function. Always use `dlt.read()` or `dlt.read_stream()`.

### DLT (`@dlt.table`)

- Every `@dlt.table` must have a `comment=` argument.
- Expectations must use `@dlt.expect_or_drop` (not `@dlt.expect`) for primary key nullability or negative amounts.
- Expectation names must be descriptive: `@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")`

### SCD2 — mandatory two-part pattern

Every SCD2 file requires:
1. A staging table (reads from Bronze)
2. `dlt.create_streaming_table()` + `dlt.apply_changes()` pair for the final dimension

Never call `dlt.apply_changes()` without `dlt.create_streaming_table()` first — it will fail at runtime.

---

## Audit Columns (MANDATORY on all Silver and Gold tables)

| Column | Type | Meaning |
|---|---|---|
| `_ingested_at` | `TIMESTAMP` | When the row was written to Bronze |
| `_processed_at` | `TIMESTAMP` | When the row was transformed into Silver/Gold |
| `_source_file` | `STRING` | Auto Loader sources only — which CSV file this row came from |
| `__START_AT` | `TIMESTAMP` | SCD2 only — when this row version became valid |
| `__END_AT` | `TIMESTAMP` | SCD2 only — when this row version was superseded (`NULL` = current) |

The double-underscore prefix on `__START_AT` / `__END_AT` is added automatically by DLT. Do not rename them.

---

## Databricks Asset Bundle

- All resources are declared in `databricks.bundle.yml`.
- Deploy: `databricks bundle deploy --target prod`
- Run a job: `databricks bundle run <job-name> --target prod`
- **Never create resources manually in the Databricks UI** — they will not be tracked in the bundle and will drift.

---

## Documentation Standards

### Every new pipeline file must have a header comment

```python
"""
{File name} — {one-line description}

Modelling strategy: {SCD1 | SCD2 | Truncate+Load | Fact append}
Source: helix_bronze.{schema}.raw
Target: helix_silver.{schema}.{table}

Why this strategy:
  {1-2 sentences explaining the choice}
"""
```

### Every new doc file must have a Table of Contents

### Lab format (all labs in `docs/hands-on-labs/hands-on-labs.md`)

```markdown
## Lab DP-{N} — {Title}

| Field | Value |
|---|---|
| Duration | ~{N} minutes |
| Databricks feature | {feature} |
| Estimated cost | ~€{N} |
| Prerequisite | {prior labs or setup steps} |

### What You Will Learn
### Steps
### What to Observe
```

**Never use curl in lab steps.** All verification is via Databricks SQL Editor or Workflows UI.

---

## Key Decisions

| Decision | Choice | Reason |
|---|---|---|
| Shared catalog names with shopstream-databricks-ai-platform | Intentional | Gold tables from this repo feed the AI platform directly |
| SCD2 for customers, SCD1 for products | Customers need history for ML feature joins; product current price is enough | |
| Auto Loader for returns | Returns come as CSV exports, not a live DB | Lakeflow Connect requires JDBC-accessible database |
| Truncate+Load for regions | Tiny table, deletions must reflect automatically | SCD1/SCD2 cannot handle source-side deletes |
| Single environment (`main` = prod) | Team of 2, no staging needed | |
| `trigger(availableNow=True)` in nightly batch | Stops cleanly after processing all new files | Avoids infinite streaming cost |
