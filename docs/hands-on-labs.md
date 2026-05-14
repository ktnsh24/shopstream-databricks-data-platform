# Hands-On Labs — ShopStream Databricks Data Platform

## Table of Contents

- [Purpose](#purpose)
- [Fail-First Learning Contract](#fail-first-learning-contract)
- [Prerequisites](#prerequisites)
- [Lab Map (10 Total Labs)](#lab-map-10-total-labs)
- [Recommended Lab Order](#recommended-lab-order)
- [Cost Guardrails](#cost-guardrails)
- [Lab Descriptions](#lab-descriptions)

---

## Purpose

This lab suite teaches the **ShopStream Databricks Data Platform** by starting from real failure modes and guiding you to recovery. You'll learn how to diagnose data pipeline issues, tune configurations, and validate fixes. Each lab takes 20–45 minutes.

---

## Fail-First Learning Contract

Every lab follows this pattern:

1. **Start With Failure** — A realistic data platform problem (schema drift blocks ingest, late data corruption, SCD logic broken, etc.)
2. **Failure Signals** — Observable symptoms: errors in logs, data quality checks fail, aggregations mismatch, job hangs
3. **Guided Fix Path** — One minimal change per lab: a config knob, a SQL WHERE clause, an environment variable
4. **Before/After Metrics** — Tables showing broken → fixed state with concrete numbers
5. **DE Parallel** — Maps the concept to a data engineering equivalent you already know

**Why fail-first?** Because production incidents start broken, and your job is to diagnose + fix, not only build from scratch. These labs train that muscle.

---

## Prerequisites

- ✅ Databricks workspace access (`shopstream-prod`)
- ✅ Python 3.10+
- ✅ Azure CLI configured (`az login`)
- ✅ ADLS Gen2 read access (`shopstream-prod` storage account)
- ✅ Familiarize yourself with the Lakeflow SDP framework (read [system-design.md](./system-design.md) first)
- ✅ Basic SQL (MERGE, APPLY CHANGES, window functions)

---

## Lab Map (10 Total Labs)

| Lab ID | Title | Layer | Time | Cost | Failure Mode | DE Parallel |
|--------|-------|-------|------|------|--------------|-------------|
| **DP-01** | Auto Loader Schema Drift | Bronze → Silver | 20 min | €0 | Schema evolution breaks ingest | Column addition in source |
| **DP-02** | Streaming Late Data & Watermark | Silver → Gold | 25 min | €1 | Late arrivals corrupt aggregates | Windowed aggregation with late data |
| **DP-03** | SCD2 Row Closure Bug | Dimension | 30 min | €0 | History rows never close (__END_AT NULL forever) | Time-series dimension versioning |
| **DP-04** | SCD1 Duplicate Creation | Dimension | 25 min | €2 | Merge produces duplicates (upsert mode wrong) | Duplicate key violation |
| **DP-05** | Truncate-Load Wipes Ref Data | Batch | 20 min | €0 | Reference data lost mid-pipeline | Data loss from unguarded DELETE |
| **DP-06** | Data Quality Too Lenient | Validation | 20 min | €0 | Bad data passes checks (expectation too weak) | Fuzzy data quality threshold |
| **DP-07** | Gold Aggregation Mismatch | Aggregation | 25 min | €1 | Revenue totals wrong (GROUP BY key wrong) | Incorrect aggregation logic |
| **DP-08** | Table Versioning Gone Wrong | Time Travel | 20 min | €0 | Restore from wrong RESTORE_TABLE() version | Restore from wrong point-in-time |
| **DP-09** | Change Data Feed Not Enabled | CDC | 20 min | €0 | Gold layer doesn't see incremental changes | Missing CDC enablement |
| **DP-10** | Point-in-Time Join Leakage | SCD Join | 30 min | €1 | Join uses future data (SCD2 time bounds violated) | Temporal join with wrong interval |

**Total Cost:** ~€5–7 (lab environment consumption, not including compute reservations)

---

## Recommended Lab Order

**Beginner Path (Medallion Architecture Foundation):**
1. DP-01 (Auto Loader schema)
2. DP-02 (Streaming watermark)
3. DP-03 (SCD2 closure)
4. DP-04 (SCD1 merges)
5. DP-05 (Truncate safety)

**Intermediate Path (Data Quality & Aggregation):**
6. DP-06 (Data quality expectations)
7. DP-07 (Gold aggregation logic)

**Advanced Path (Time Travel & Temporal Joins):**
8. DP-08 (Table versioning)
9. DP-09 (Change Data Feed)
10. DP-10 (SCD2 temporal join)

---

## Cost Guardrails

Each lab is designed to stay within Azure Databricks + ADLS consumption caps:

- **Compute:** 0.5–2 DBU per lab (small cluster, ~5–10 min run)
- **Storage:** ADLS writes <1 GB per lab (cached Bronze/Silver reuse)
- **Total:** ~€0.5–€1 per lab in on-demand costs

**To avoid overages:**
- Use a single **small all-purpose cluster** (i3.xlarge, 8 workers) for all labs
- Reuse Bronze/Silver tables across labs (don't truncate between labs)
- Run labs sequentially, not in parallel
- Set cluster idle timeout to 10 min

---

## Lab Descriptions

### DP-01 — Auto Loader Schema Drift

**Layer:** Bronze → Silver (Streaming)

**Start With Failure:**
Auto Loader ingests orders from ADLS `/raw/orders/`. The source CSV adds a new column `customer_lifetime_value` (LTV). The ingest job crashes with a schema mismatch error.

```
ERROR: Schema of orders has changed. Column 'customer_lifetime_value' not found in registered schema.
```

**Failure Signals:**
- Ingest job fails every 5 min (Lakeflow retries)
- Bronze table has data up to 2024-05-03; no new data after
- CloudFile state in `_autoloader/` shows pending files

**Guided Fix Path:**
Set Auto Loader schema mode from `failOnNewColumns: true` to `addNewColumns: true` in the Lakeflow config:

```python
# In your Bronze ingestion notebook:
dlt.create_streaming_table(
    name="orders_raw",
    comment="Orders from ADLS (Auto Loader)"
)
dlt.expect_or_drop("valid_amount", "amount > 0")

# Add this hint:
# @dlt.config
# {
#   "cloudFiles.schemaLocation": f"/Volumes/{schema_name}/auto_loader_schema",
#   "cloudFiles.schemaHints": "customer_lifetime_value DECIMAL(12,2)",
#   "cloudFiles.schemaEvolutionMode": "addNewColumns"
# }
```

**Before/After Metrics:**

| Metric | Before | After | Fix Applied |
|--------|--------|-------|------------|
| Ingest success rate | 0% (crashes every 5 min) | 100% ✓ | `schemaEvolutionMode: addNewColumns` |
| New column visibility | ❌ Not loaded | ✓ Loaded | Schema hint + evolution mode |
| Row count (Bronze) | 10.2M (stale) | 10.8M (fresh) | New data ingested |
| Ingest lag | ∞ (failed) | 2 min | Streaming resumed |

**Config Knobs Explained:**
- `cloudFiles.schemaEvolutionMode` — controls how Auto Loader reacts to source schema changes. Set to `addNewColumns` to auto-discover new columns; use `failOnNewColumns` only if you want to freeze the schema.
- `cloudFiles.schemaHints` — helps Auto Loader infer correct types for new columns (e.g., `customer_lifetime_value DECIMAL` instead of defaulting to STRING).

**DE Parallel:**
This mirrors **source system column additions**. In a traditional data warehouse, a new column in the source system (e.g., a marketing table adds a campaign ID) requires a warehouse schema migration. Lakeflow Auto Loader handles this via config, not manual ALTER TABLE statements.

---

### DP-02 — Streaming Late Data & Watermark Tuning

**Layer:** Silver → Gold (Streaming Aggregation)

**Start With Failure:**
Gold layer computes `revenue_by_region_daily` using a 5-min watermark. Orders arrive late (network blip, out-of-order Kafka), but the aggregation has already fired. Revenue for a given date is 12% lower than expected.

```
Revenue mismatch:
- Expected (based on raw event counts): €156,000
- Gold aggregate: €137,000
- Difference: €19,000 (12%)
```

**Failure Signals:**
- Daily revenue report shows dips (12–15% lower than previous day, no holiday/promo context)
- Downstream dashboards flag data freshness warnings
- Audit log shows 3,400 orders arrived >5 min after their event_timestamp

**Guided Fix Path:**
Increase the watermark from 5 minutes to 15 minutes in the Silver→Gold transformation:

```python
# Before (broken):
.withWatermark("event_timestamp", "5 minutes")
.groupBy(window("event_timestamp", "1 day"), "region")
.agg(sum("amount").alias("daily_revenue"))

# After (fixed):
.withWatermark("event_timestamp", "15 minutes")
.groupBy(window("event_timestamp", "1 day"), "region")
.agg(sum("amount").alias("daily_revenue"))
```

**Before/After Metrics:**

| Metric | Before | After | Fix Applied |
|--------|--------|-------|------------|
| Watermark lag | 5 min | 15 min | Increased window |
| Revenue variance | ±12% | ±2% | Catches late orders |
| Late-arriving orders captured | 0 (dropped) | 3,400 ✓ | Extended grace period |
| Aggregation latency | 5 min | 15 min | Trade-off: freshness vs completeness |

**Config Knobs Explained:**
- `withWatermark("event_timestamp", "X")` — sets the grace period for late data. Events arriving within X minutes of their event_timestamp are included in the aggregation; older late arrivals are dropped.
- Trade-off: Longer watermark = more complete aggregates but higher latency before publish.

**DE Parallel:**
This mirrors **time-windowed aggregations with out-of-order data**. In traditional ETL (Spark batch), you'd load data, wait 1–2 hours for slow systems to catch up, then run the aggregation. Streaming makes this explicit via watermarks.

---

### DP-03 — SCD2 Row Closure Bug

**Layer:** Dimension (Batch)

**Start With Failure:**
`dim_customers` is SCD2 (slowly changing dimension, Type 2 — track history). When a customer's address changes, a new row is inserted and the old row's `__END_AT` should be set to the event time. Instead, `__END_AT` stays NULL forever.

Query for customer 12345 in `dim_customers`:

```sql
SELECT * FROM dim_customers WHERE customer_id = 12345 ORDER BY __START_AT DESC;
```

Output:
```
customer_id | address | __START_AT | __END_AT
12345 | "123 Old St" | 2024-05-01 | NULL -- ❌ Should be 2024-05-15
12345 | "456 New St" | 2024-05-15 | NULL -- ✓ Current row
```

**Failure Signals:**
- Auditors find duplicate active records for the same key
- Temporal joins break (ambiguous which row to use)
- Dimension queries return duplicate rows for a given point-in-time

**Guided Fix Path:**
Use Databricks `APPLY CHANGES INTO` with `ASSERT_EXISTS` to close old rows before inserting new ones:

```sql
-- Before (broken SCD2):
MERGE INTO dim_customers t USING staged_changes s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET address = s.address
WHEN NOT MATCHED THEN INSERT (customer_id, address, __START_AT, __END_AT) 
  VALUES (s.customer_id, s.address, s.updated_at, NULL);

-- After (fixed SCD2 with APPLY CHANGES):
APPLY CHANGES INTO dim_customers
  FROM staged_changes
  KEYS (customer_id)
  APPLY AS UPSERT
  STORED AS SCD TYPE 2;
```

**Before/After Metrics:**

| Metric | Before | After | Fix Applied |
|--------|--------|-------|------------|
| Closed history rows (__END_AT ≠ NULL) | 0 | 1,204 ✓ | APPLY CHANGES SCD TYPE 2 |
| Active rows per customer | 2–4 (wrong) | 1 (correct) ✓ | Row closure enforced |
| Temporal join ambiguity | Errors | ✓ Resolved | Historical rows properly closed |
| Audit compliance | ❌ Failed | ✓ Passed | Audit trail complete |

**Config Knobs Explained:**
- `APPLY CHANGES INTO ... STORED AS SCD TYPE 2` — Databricks' built-in SCD2 logic. Automatically closes old rows (`__END_AT = <change_timestamp>`) and inserts new ones.
- Alternative: Manual MERGE with logic to set `__END_AT = (SELECT MAX(updated_at) FROM...)` before update (error-prone).

**DE Parallel:**
This mirrors **dimensional change tracking in traditional data warehouses**. Without proper row closure, your fact table temporal joins become ambiguous (which dimension row was active on date X?).

---

### DP-04 — SCD1 Duplicate Creation

**Layer:** Dimension (Batch)

**Start With Failure:**
`dim_products` is SCD1 (slowly changing dimension, Type 1 — overwrite, no history). When a product's price changes, the row is updated in-place. Instead, the MERGE upsert mode is creating duplicates.

Query for product 789 in `dim_products`:

```sql
SELECT * FROM dim_products WHERE product_id = 789;
```

Output:
```
product_id | name | price | __UPDATED_AT
789 | "Widget A" | 19.99 | 2024-05-01
789 | "Widget A" | 21.99 | 2024-05-15 -- ❌ Duplicate! Should overwrite, not insert
```

**Failure Signals:**
- Product dimension has duplicates (2–3 rows per product)
- Fact table joins become 1-to-many (fact rows multiply)
- Revenue reports inflated by 2–3×

**Guided Fix Path:**
Fix the MERGE upsert mode — use `whenMatchedUpdate` with proper key matching and ensure no multi-row matches:

```sql
-- Before (broken):
MERGE INTO dim_products t USING staged_changes s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET price = s.price
WHEN NOT MATCHED THEN INSERT (product_id, name, price) VALUES (...)
-- ❌ Problem: If multiple rows in staged_changes have same product_id, 
--    the first UPDATE succeeds, but then multiple INSERTs happen

-- After (fixed):
MERGE INTO dim_products t USING (
  SELECT * FROM staged_changes
  WHERE rn = 1  -- ✓ Deduplicate: keep only latest change per product
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC) = 1
) s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET price = s.price, __UPDATED_AT = s.updated_at
WHEN NOT MATCHED THEN INSERT (product_id, name, price, __UPDATED_AT) VALUES (...)
```

**Before/After Metrics:**

| Metric | Before | After | Fix Applied |
|--------|--------|-------|------------|
| Duplicate products | 342 ✗ | 0 ✓ | Added QUALIFY dedup |
| Revenue accuracy | -200% (inflated) | ✓ Correct | Deduplicated joins |
| Fact table row count | 45.2M (wrong) | 15.1M ✓ | Removed duplicate matches |
| Join cardinality | 1-to-many ✗ | 1-to-1 ✓ | Enforced PK |

**Config Knobs Explained:**
- `MERGE ... WHEN MATCHED ... WHEN NOT MATCHED` — SQL's upsert pattern. If the source has duplicates on the merge key, `WHEN MATCHED` fires multiple times, producing unexpected behavior.
- Always deduplicate source data before MERGE: `QUALIFY ROW_NUMBER() OVER (PARTITION BY key ...) = 1`.

**DE Parallel:**
This mirrors **dimension key violations**. In traditional ETL, if your dimension loading script loads duplicate SKUs, the fact table's foreign key join becomes 1-to-many instead of 1-to-1, inflating metrics.

---

### DP-05 — Truncate-Load Wipes Ref Data

**Layer:** Batch Pipeline

**Start With Failure:**
The reference data load job (products, regions, categories) uses a truncate-then-insert pattern. A bug causes the TRUNCATE to fire but the subsequent INSERT to fail. Reference tables are now empty.

```
Bronze reference tables: 0 rows
├─ dim_products: 0 rows (expected 8,000)
├─ dim_regions: 0 rows (expected 24)
└─ dim_categories: 0 rows (expected 150)
```

Cascading failures:
- Fact table loads fail (foreign key constraint violations)
- Revenue reports show NULL region/product names
- Dashboards blank out

**Failure Signals:**
- Reference table row count suddenly drops to 0
- Data validation checks fail: `ASSERT row_count > 1000`
- Fact table foreign key joins break
- Null rates spike in reports

**Guided Fix Path:**
Replace truncate-then-insert with a staged MERGE + validation guardrail:

```python
# Before (broken):
spark.sql("""
  TRUNCATE TABLE dim_products;
  INSERT INTO dim_products SELECT * FROM staged_products;
""")
# ❌ If INSERT fails, dim_products is left empty

# After (fixed):
spark.sql("""
  BEGIN TRANSACTION;
  
  -- Load into temp table first
  CREATE TEMP TABLE staged_products_validated AS
  SELECT * FROM staged_products
  WHERE product_id IS NOT NULL;
  
  -- Validate row count before merge
  ASSERT (SELECT COUNT(*) FROM staged_products_validated) > 5000
    ERROR "Staged products row count < 5000, aborting merge";
  
  -- Merge with validation
  MERGE INTO dim_products t
  USING staged_products_validated s
  ON t.product_id = s.product_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
  
  COMMIT;
""")
```

**Before/After Metrics:**

| Metric | Before | After | Fix Applied |
|--------|--------|-------|------------|
| Reference row count | 0 ✗ (lost) | 8,000 ✓ | Staged merge + assert |
| Data loss recovery time | 4 hours (restore from backup) | N/A (prevented) ✓ | Pre-load validation |
| Fact table load success | ❌ 0% (FK violations) | ✓ 100% | Valid reference data |
| Uptime | 3.5 hour outage | No outage ✓ | Validation guardrail |

**Config Knobs Explained:**
- `ASSERT row_count > threshold` — data quality check that fails the job if validation fails, preventing data loss.
- **Staged load pattern** — load to a temp table, validate, then merge to production. If validation fails, production remains untouched.

**DE Parallel:**
This mirrors **data warehouse load failure recovery**. Truncate-then-insert is risky; truncate-then-insert-with-validation (or staging + merge) is the standard safe pattern.

---

### DP-06 — Data Quality Too Lenient

**Layer:** Validation

**Start With Failure:**
DLT expectation for `fct_orders.amount` is too lenient: `amount > 0`. A typo in the source creates orders with `amount = -50` (negative). The check passes (only rejects NULL and zero), but downstream revenue calculations are now negative.

```
Revenue by day:
2024-05-14: €156,000 ✓
2024-05-15: -€23,000 ✗ (negative!)
```

**Failure Signals:**
- Revenue reports show negative values
- Dashboard alerts: "Revenue < 0"
- Data quality tests pass (because the check was weak)
- Auditors flag data anomaly

**Guided Fix Path:**
Tighten the data quality expectation to reject known invalid values:

```python
# Before (broken):
dlt.expect("positive_amount", "amount > 0")

# After (fixed):
dlt.expect("positive_amount", "amount > 0 AND amount < 1000000")  # ← Cap check
dlt.expect("no_suspicious_orders", "NOT (order_type = 'return' AND amount > 0)")  # ← Business logic
dlt.expect("region_valid", "region IN ('EMEA', 'APAC', 'Americas', 'Other')")  # ← Enumeration
```

**Before/After Metrics:**

| Metric | Before | After | Fix Applied |
|--------|--------|-------|------------|
| Negative amount rows passed | 342 ✗ | 0 ✓ | Added amount < 1M cap |
| Invalid region rows passed | 1,204 ✗ | 0 ✓ | Region enum validation |
| Revenue accuracy | ❌ -€23k anomaly | ✓ Correct | Tightened checks |
| Data quality score | 95% (false) | 87% (true) | More realistic checks |

**Config Knobs Explained:**
- Data quality expectations should check both **bounds** (min/max) and **enumerations** (valid values), not just "not null."
- Use multiple `dlt.expect()` calls, each with a specific, auditable rule.

**DE Parallel:**
This mirrors **data validation layer improvements**. A weak constraint passes bad data through; strong constraints catch anomalies before they cascade.

---

### DP-07 — Gold Aggregation Mismatch

**Layer:** Aggregation (Gold)

**Start With Failure:**
`fct_revenue_by_category` computes daily revenue per category. The aggregation groups by `CATEGORY_ID` but joins back to `DIM_CATEGORIES` using `CATEGORY_NAME` (key mismatch). Revenue is allocated to wrong categories.

```
Query 1 (correct): SELECT category_id, SUM(amount) FROM fct_orders GROUP BY category_id
Result: Electronics: €156k, Clothing: €98k, Home: €62k

Query 2 (fct_revenue_by_category): SELECT category_name, SUM(amount) FROM fct_orders JOIN dim_categories ON name = category_name GROUP BY category_name
Result: Electronics: €78k ✗, Clothing: €156k ✗, Home: €98k ✗ (wrong!)
```

**Failure Signals:**
- Revenue totals match (€316k overall) but per-category splits wrong
- Business team queries raw tables, gets different numbers than reports
- Audit finds category mismatches

**Guided Fix Path:**
Fix the join to use the correct key (category_id) and validate the GROUP BY is at the right grain:

```sql
-- Before (broken):
SELECT 
  dc.category_name,
  SUM(fo.amount) as daily_revenue
FROM fct_orders fo
JOIN dim_categories dc ON fo.category_name = dc.category_name  -- ❌ String join, lossy
GROUP BY dc.category_name;

-- After (fixed):
SELECT 
  dc.category_id,
  dc.category_name,
  SUM(fo.amount) as daily_revenue
FROM fct_orders fo
JOIN dim_categories dc ON fo.category_id = dc.category_id  -- ✓ ID join, precise
GROUP BY dc.category_id, dc.category_name;  -- ✓ GROUP BY includes join key
```

**Before/After Metrics:**

| Metric | Before | After | Fix Applied |
|--------|--------|-------|------------|
| Electronics revenue | €78k ✗ | €156k ✓ | ID join + correct GROUP BY |
| Clothing revenue | €156k ✗ | €98k ✓ | ID join + correct GROUP BY |
| Revenue reconciliation | ❌ Fails | ✓ Passes | Aggregation logic fixed |
| Data lineage clarity | Ambiguous | ✓ Clear | ID-based joins only |

**Config Knobs Explained:**
- Always join dimensions on **surrogate keys** (category_id), not business keys (category_name). Business keys can change; surrogate keys are stable.
- GROUP BY should include the join key to ensure 1-to-1 cardinality.

**DE Parallel:**
This mirrors **fact table grain mismatches**. If you GROUP BY at the wrong granularity (e.g., by date instead of date+category), you lose detail or create wrong aggregates.

---

### DP-08 — Table Versioning Gone Wrong

**Layer:** Time Travel (Delta Lake)

**Start With Failure:**
A data quality check failed yesterday (May 15), but the table was restored from a backup. The restore was from the wrong point-in-time: `fct_orders` was restored to version 120 (May 10), losing 5 days of data.

```
Current version (correct): 10.8M rows (May 15 end-of-day)
Restored version (wrong): 9.2M rows (May 10, 5 days old)
Data loss: 1.6M rows
```

**Failure Signals:**
- Row counts drop unexpectedly
- Time-series aggregations show backfill needed
- Audit trails show `RESTORE_TABLE` from old version

**Guided Fix Path:**
Use explicit version pinning and validation to restore from the correct point-in-time:

```python
# Before (broken):
spark.sql("RESTORE TABLE fct_orders TO VERSION 120")  # ❌ Wrong version

# After (fixed):
# Step 1: Find the correct version before data loss
correct_version = spark.sql("""
  DESCRIBE HISTORY fct_orders 
  WHERE timestamp > '2024-05-14 00:00:00' AND timestamp < '2024-05-15 00:00:00'
  ORDER BY version DESC LIMIT 1
""").collect()[0]["version"]

# Step 2: Restore with validation
spark.sql(f"RESTORE TABLE fct_orders TO VERSION {correct_version}")

# Step 3: Validate row count
restored_count = spark.sql("SELECT COUNT(*) as cnt FROM fct_orders").collect()[0]["cnt"]
assert restored_count > 10_000_000, f"Restored row count {restored_count} suspiciously low!"

print(f"✓ Restored fct_orders to version {correct_version} with {restored_count} rows")
```

**Before/After Metrics:**

| Metric | Before | After | Fix Applied |
|--------|--------|-------|------------|
| Row count | 9.2M (old) | 10.8M ✓ (current) | Correct version restored |
| Date range | 2024-05-01 to 2024-05-10 ✗ | 2024-05-01 to 2024-05-15 ✓ | Full data preserved |
| Data loss | 1.6M rows lost | 0 rows lost ✓ | Proper version pinning |
| Validation guard | None | ✓ Assert row count | Prevents future mistakes |

**Config Knobs Explained:**
- `DESCRIBE HISTORY table_name` — lists all versions with timestamps. Query this to find the correct restore point.
- Always validate row count / date range after restore to catch mistakes.

**DE Parallel:**
This mirrors **database point-in-time recovery (PITR)**. You must identify the correct snapshot before restore, or you lose data silently.

---

### DP-09 — Change Data Feed Not Enabled

**Layer:** CDC (Change Data Feed)

**Start With Failure:**
Gold layer tries to read incremental changes from Silver via Change Data Feed (CDF). Query fails:

```
ERROR: Change data feed is not enabled on table helix_silver.fct_orders.
```

Gold layer falls back to full-table scans every run, causing:
- 10× more I/O
- 30 min job instead of 3 min
- €25 cost per run instead of €2.50

**Failure Signals:**
- CDF read fails with "not enabled" error
- Gold job runs slow (30+ min vs expected 3 min)
- Compute costs spike (€25 vs €2.50 per run)
- Logs show full-table scans instead of incremental

**Guided Fix Path:**
Enable CDF on the Silver table and update the Gold pipeline to read incremental changes:

```sql
-- Step 1: Enable CDF on Silver table
ALTER TABLE helix_silver.fct_orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Step 2: Backfill Gold layer once (full table)
INSERT INTO helix_gold.fct_revenue_daily
SELECT 
  event_date,
  region,
  SUM(amount) as daily_revenue
FROM helix_silver.fct_orders
WHERE _change_type IN ('insert', 'update_postimage')
GROUP BY event_date, region;

-- Step 3: Update Gold incremental job to read CDF
SELECT 
  event_date,
  region,
  SUM(amount) as daily_revenue
FROM table_changes("helix_silver.fct_orders", startVersion => 120)  -- ← CDF read
WHERE _change_type IN ('insert', 'update_postimage')
GROUP BY event_date, region;
```

**Before/After Metrics:**

| Metric | Before | After | Fix Applied |
|--------|--------|-------|------------|
| Gold job runtime | 30 min | 3 min ✓ | CDF incremental read |
| Cost per run | €25 | €2.50 ✓ | 10× I/O reduction |
| Data freshness | 30 min lag | 3 min lag ✓ | Faster pipeline |
| CDF enabled | ❌ False | ✓ True | `ALTER TABLE ... SET TBLPROPERTIES` |

**Config Knobs Explained:**
- `delta.enableChangeDataFeed = true` — must be set on Silver table for CDF to work.
- `table_changes(table_name, startVersion => X)` — read incremental changes from version X onwards.
- Always read `_change_type IN ('insert', 'update_postimage')` to exclude deletes or pre-images.

**DE Parallel:**
This mirrors **CDC enablement in traditional data warehouses**. Without CDC, you're forced to full-scan; with it, you read only changes.

---

### DP-10 — Point-in-Time Join Leakage

**Layer:** SCD Joins (Temporal Join)

**Start With Failure:**
`fct_orders` is joined to `dim_customers SCD2` (slowly changing dimension) using a point-in-time filter. A customer's address changed on May 12. Orders placed on May 11 are being joined to the new (post-May 12) address, violating the SCD2 semantics.

```
Order on 2024-05-11: customer_id = 456
Join result (wrong): 456's address = "456 New St" (active May 12+)
Expected result: 456's address = "123 Old St" (active May 11)
```

**Failure Signals:**
- Historical orders show "future" customer data (addresses, phone numbers)
- Audit finds temporal join violations
- GDPR audit: historical orders linked to wrong addresses

**Guided Fix Path:**
Use SCD2 time bounds (`__START_AT` / `__END_AT`) in the join condition to enforce point-in-time accuracy:

```sql
-- Before (broken):
SELECT 
  o.order_id,
  o.order_date,
  c.address
FROM fct_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
  AND o.order_date >= c.__START_AT  -- ❌ Missing upper bound; uses future rows
;

-- After (fixed):
SELECT 
  o.order_id,
  o.order_date,
  c.address
FROM fct_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
  AND o.order_date >= c.__START_AT
  AND o.order_date < COALESCE(c.__END_AT, '2999-12-31')  -- ✓ Upper bound enforces point-in-time
WHERE c.__END_AT IS NULL OR o.order_date < c.__END_AT
;
```

**Before/After Metrics:**

| Metric | Before | After | Fix Applied |
|--------|--------|-------|------------|
| Orders with future customer data | 12,340 ✗ | 0 ✓ | SCD2 time bounds enforced |
| Point-in-time accuracy | ❌ Failed audit | ✓ Passed | BETWEEN __START_AT/__END_AT |
| Address mismatches | 8,504 found ✗ | 0 ✓ | Correct historical data |
| Temporal join correctness | Broken | ✓ Fixed | Explicit time bounds |

**Config Knobs Explained:**
- SCD2 temporal join must use `BETWEEN __START_AT AND __END_AT` (or `>=` / `<` combined).
- Always use `COALESCE(c.__END_AT, '2999-12-31')` for current rows (no end date = still active).

**DE Parallel:**
This mirrors **as-of joins in time-series databases**. You must match on both the key AND the timestamp to ensure you get the right version of the dimension.

---

## Next Steps

After completing the labs:

1. **Review `DESCRIBE HISTORY`** for the tables you modified (see how Delta versioning tracks your fixes)
2. **Extend to your own data** — apply these patterns to your operational pipelines
3. **Share findings** — document which labs were most useful for your role (DE, Analytics, Data Science)

---

## Resources

- [Databricks Lakeflow SDP Guide](https://docs.databricks.com/en/lakeflow/index.html)
- [Delta Lake Time Travel](https://docs.databricks.com/en/delta/history.html)
- [SCD Type 1 & 2 Patterns](https://docs.databricks.com/en/delta/best-practices.html#scd-patterns)
- [Auto Loader Schema Evolution](https://docs.databricks.com/en/ingestion/auto-loader/schema.html)
- [Change Data Feed (CDF)](https://docs.databricks.com/en/delta/change-data-feed.html)
