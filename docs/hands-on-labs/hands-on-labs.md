# Hands-On Labs — ShopStream Databricks Data Platform

> **Who this is for:** The data engineer builds every lab. Ketan builds the same in `shopstream-databricks-ai-platform`.
> **All pipeline files start empty.** You write every line as you follow these labs.
> **ShopStream** is a fictional Dutch e-commerce company (electronics, clothing, sports equipment).

---

## Table of Contents

- [Lab DP-01 — Auto Loader: Ingest Returns CSV](#lab-dp-01--auto-loader-ingest-returns-csv)
- [Lab DP-02 — Structured Streaming: Ingest Orders from Event Hubs](#lab-dp-02--structured-streaming-ingest-orders-from-event-hubs)
- [Lab DP-03 — SCD Type 2: Customer Dimension](#lab-dp-03--scd-type-2-customer-dimension)
- [Lab DP-04 — SCD Type 1: Product Dimension](#lab-dp-04--scd-type-1-product-dimension)
- [Lab DP-05 — Truncate and Load: Regions](#lab-dp-05--truncate-and-load-regions)
- [Lab DP-06 — Data Quality: Fact Orders](#lab-dp-06--data-quality-fact-orders)
- [Lab DP-07 — Gold Aggregations](#lab-dp-07--gold-aggregations)
- [Lab DP-08 — Delta Time Travel](#lab-dp-08--delta-time-travel)
- [Lab DP-09 — Change Data Feed](#lab-dp-09--change-data-feed)
- [Lab DP-10 — Point-in-Time Join](#lab-dp-10--point-in-time-join)
- [Lab DP-11 — OPTIMIZE and Z-ORDER](#lab-dp-11--optimize-and-z-order)
- [Lab DP-12 — Broadcast Join and AQE](#lab-dp-12--broadcast-join-and-aqe)
- [Lab DP-13 — Delta Sharing](#lab-dp-13--delta-sharing)

---

## Lab DP-01 — Auto Loader: Ingest Returns CSV

| Field | Value |
|---|---|
| Duration | ~45 minutes |
| File to open | `data_platform/pipelines/bronze/ingest_returns_autoloader.py` |
| Databricks features | Auto Loader (`cloudFiles`), Delta Lake, Unity Catalog |
| Estimated cost | ~€0.05 (one job run on a small cluster) |

### What is Auto Loader?

ShopStream exports a CSV file of new returns every hour into an ADLS Gen2 path like `raw/returns/2026/05/04/returns_20260504_1400.csv`. Every hour, new files appear. Your pipeline must read only the new files — not re-read the thousands of files from previous hours.

Auto Loader solves this with a **checkpoint**. After processing a batch of files, Auto Loader writes "I finished processing up to file X" into a checkpoint path. On the next run, it reads the checkpoint and skips all previously processed files. Only new files are read.

Think of it as a postal worker with a stamp. She stamps each letter after processing it. Tomorrow, she only opens unstamped letters.

**Why not just list all files and read them?** Two reasons:
1. As months pass, you accumulate thousands of files. Re-reading them all is slow and wastes compute.
2. You might process duplicates and double-count your data.

**What is `trigger(availableNow=True)`?** Spark Streaming has two modes. Continuous mode runs the job forever, polling every few seconds. Available-now mode runs the job once, processes all new files, then stops. For hourly batch jobs you want available-now — the cluster stops when done and you stop paying.

**What is Delta Lake?** Delta Lake adds a transaction log to plain Parquet files. Every write creates a log entry. If a job fails halfway, the partial write is ignored — the log was never committed. If two jobs write simultaneously, the transaction log ensures they do not interfere. This gives you reliable tables on top of a cloud storage bucket.

### What you will build

`ingest_returns_autoloader.py` reads new return CSV files from ADLS Gen2, adds ingestion metadata columns (`_ingested_at`, `_source_file`), and appends them to the Delta table `helix_bronze.returns.returns_raw`.

### Step 1 — Open the file

Open `data_platform/pipelines/bronze/ingest_returns_autoloader.py`.
It is empty right now.

### Step 2 — Write the imports

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
```

What each import is for:
- `SparkSession` — the main entry point for everything in Spark. On Databricks, one already exists as the variable `spark`. You import it so the function type hints are correct.
- `col` — references a column by name inside a transformation, e.g. `col("customer_id")`.
- `current_timestamp` — Spark function that returns the current time. Added to every Bronze row so you know exactly when it was loaded.
- `input_file_name` — returns the ADLS path of the source file for that row. Useful for debugging: if a row looks wrong, you know which CSV file it came from.
- `StructType`, `StructField` — used to define the schema explicitly. You always define the schema for CSV ingestion — never let Spark infer it. Inference is slow and unreliable.

### Step 3 — Define the schema

```python
RETURNS_SCHEMA = StructType([
    StructField("return_id",     StringType(),  nullable=False),
    StructField("order_id",      StringType(),  nullable=False),
    StructField("customer_id",   StringType(),  nullable=False),
    StructField("product_id",    StringType(),  nullable=False),
    StructField("return_date",   StringType(),  nullable=True),
    StructField("reason_code",   StringType(),  nullable=True),
    StructField("refund_amount", DoubleType(),  nullable=True),
])
```

`nullable=False` on `return_id` — if a CSV row has a blank `return_id`, Spark raises an error. This is correct: a return without an ID is bad data that should fail loudly.

`return_date` is `StringType` — never parse timestamps in the Bronze layer. Store the raw string exactly as it arrived. Parse it in Silver where you control the format. Bronze is a faithful copy of the source.

### Step 4 — Define paths

```python
STORAGE_ACCOUNT = "helixdata001"
SOURCE_PATH     = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/returns/"
TARGET_PATH     = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/returns/returns_raw/"
CHECKPOINT_PATH = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/returns/"
```

`abfss://` is the ADLS Gen2 URL scheme. `container@storageaccount.dfs.core.windows.net/path`. The `s` at the end of `abfss` means TLS-encrypted.

The checkpoint path must be different from the source and target. Auto Loader writes small JSON files here to track which files have been processed.

### Step 5 — Write the ingestion function

```python
def ingest_returns(spark: SparkSession) -> None:
    stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "_schema")
        .option("header", "true")
        .schema(RETURNS_SCHEMA)
        .load(SOURCE_PATH)
    )

    enriched = stream.select(
        "*",
        current_timestamp().alias("_ingested_at"),
        input_file_name().alias("_source_file"),
    )

    (
        enriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable("helix_bronze.returns.returns_raw")
    )
```

Line by line:
- `.format("cloudFiles")` — activates Auto Loader. Without this you would use `.format("csv")` which reads all files every time.
- `.option("cloudFiles.format", "csv")` — what format are the source files?
- `.option("cloudFiles.schemaLocation", ...)` — Auto Loader can infer schema if you do not provide one. Even when you provide your own, this path is required as a fallback store.
- `.outputMode("append")` — only write new rows. Bronze tables never update or delete rows — they grow by appending.
- `.trigger(availableNow=True)` — process all new files then stop.
- `.toTable("helix_bronze.returns.returns_raw")` — write to a managed Unity Catalog table. Databricks creates the table automatically on the first run.

### Step 6 — Add the entry point

```python
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    ingest_returns(spark)
```

`getOrCreate()` — returns the existing Spark session if one is running (Databricks always has one), or creates a new one otherwise.

### Step 7 — Run it

In the Databricks UI: Workflows → Create job → add a task → Python script → point to this file → run.

Or via DAB:
```bash
databricks bundle run ingest_returns
```

### What to observe

You should see the job run for 30-60 seconds then complete with status `Succeeded`. The first run processes all existing files in the source path.

### Verify

Open the Databricks SQL Editor and run:
```sql
SELECT COUNT(*) AS row_count FROM helix_bronze.returns.returns_raw;
```

Expected: the count equals the number of rows across all CSV files in the source path.

```sql
SELECT * FROM helix_bronze.returns.returns_raw LIMIT 5;
```

You should see `return_id`, `order_id`, `customer_id`, `refund_amount`, plus `_ingested_at` (today's timestamp) and `_source_file` (the ADLS path of the source CSV).

---

## Lab DP-02 — Structured Streaming: Ingest Orders from Event Hubs

| Field | Value |
|---|---|
| Duration | ~60 minutes |
| File to open | `data_platform/pipelines/bronze/ingest_orders_streaming.py` |
| Databricks features | Structured Streaming, Kafka connector, Delta Lake |
| Estimated cost | ~€0.15/hour while running (continuous job) |

### What is Structured Streaming from Event Hubs?

When a ShopStream customer places an order on the website, the order data is immediately published to Azure Event Hubs — a cloud messaging service. Event Hubs acts as a buffer: the website publishes at whatever rate orders arrive; your Databricks job reads at whatever rate it can process.

**What is Kafka protocol?** Apache Kafka is an open-source streaming platform. Azure Event Hubs is wire-compatible with Kafka — your Spark code talks to Event Hubs using the exact same Kafka connector it would use for a real Kafka cluster. No Kafka installation needed, but the concepts (topics = Event Hubs, partitions, offsets, consumer groups) are identical.

**What is a partition?** Event Hubs splits a topic into multiple partitions (you created 2 in Phase 01). Each partition is an ordered, immutable log of messages. Spark reads from each partition in parallel — one Spark task per partition. With 2 partitions, 2 Spark tasks can read simultaneously.

**What is an offset?** An offset is the position of a message within a partition. Think of it as a line number. Message at offset 0 is the first message ever published. Offset 1000 is the 1001st. When Spark writes a checkpoint, it saves the last offset it processed. On restart, it reads from that offset — not from the beginning.

**What is a consumer group?** A consumer group is an independent read cursor. Two jobs can read the same Event Hub without interfering with each other's progress, as long as each has its own consumer group. The `databricks-streaming` consumer group you created in Phase 01 is exclusively for your Spark streaming job.

**What is a watermark?** Orders can arrive slightly out of order — a request takes 3 seconds longer than usual due to network congestion, so an order timestamped 14:05 arrives at the Event Hub at 14:08. Without a watermark, Spark would wait forever for any late order before closing a time window. A watermark of 10 minutes says: "I will wait up to 10 minutes for late events. Anything later than that is dropped."

### What you will build

`ingest_orders_streaming.py` continuously reads order events from Event Hubs, parses the JSON payload, and appends rows to `helix_bronze.orders.orders_raw`. This job runs continuously (not triggered like Auto Loader).

### Step 1 — Open the file

Open `data_platform/pipelines/bronze/ingest_orders_streaming.py`.
It is empty right now.

### Step 2 — Write the imports

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
```

- `from_json` — parses a JSON string into a struct. Event Hubs delivers each message as bytes. Spark reads the bytes as a string. `from_json` turns the string into typed columns.

### Step 3 — Define schema and connection config

```python
ORDER_SCHEMA = StructType([
    StructField("order_id",    StringType(),  nullable=False),
    StructField("customer_id", StringType(),  nullable=False),
    StructField("product_id",  StringType(),  nullable=False),
    StructField("quantity",    IntegerType(), nullable=True),
    StructField("unit_price",  DoubleType(),  nullable=True),
    StructField("order_ts",    StringType(),  nullable=True),
])

STORAGE_ACCOUNT = "helixdata001"
CHECKPOINT_PATH = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/orders/"
NAMESPACE       = "helix-events-abc123"
EVENT_HUB       = "orders"
CONSUMER_GROUP  = "databricks-streaming"
```

### Step 4 — Read the connection string from Databricks secrets

```python
def get_kafka_options() -> dict:
    conn_str = dbutils.secrets.get(scope="helix", key="event-hub-connection-string")
    sasl = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn_str}";'
    )
    return {
        "kafka.bootstrap.servers":   f"{NAMESPACE}.servicebus.windows.net:9093",
        "subscribe":                  EVENT_HUB,
        "kafka.group.id":             CONSUMER_GROUP,
        "kafka.security.protocol":    "SASL_SSL",
        "kafka.sasl.mechanism":       "PLAIN",
        "kafka.sasl.jaas.config":     sasl,
        "startingOffsets":            "latest",
        "failOnDataLoss":             "false",
    }
```

`dbutils.secrets.get(scope="helix", key="event-hub-connection-string")` — reads from Key Vault through the secret scope created in Phase 01. The connection string never appears in code, logs, or notebooks.

`startingOffsets = "latest"` — start reading from now, skip historical messages. Use `"earliest"` if you want to replay all messages from the beginning.

### Step 5 — Write the streaming function

```python
def ingest_orders(spark: SparkSession):
    raw = (
        spark.readStream
        .format("kafka")
        .options(**get_kafka_options())
        .load()
    )

    parsed = (
        raw
        .select(
            col("value").cast("string").alias("raw_payload"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("event_time"),
        )
        .withWatermark("event_time", "10 minutes")
        .withColumn("order", from_json(col("raw_payload"), ORDER_SCHEMA))
        .select(
            "order.*",
            "raw_payload",
            "kafka_partition",
            "kafka_offset",
            "event_time",
            current_timestamp().alias("_ingested_at"),
        )
    )

    return (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .toTable("helix_bronze.orders.orders_raw")
    )

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    query = ingest_orders(spark)
    query.awaitTermination()
```

`col("value").cast("string")` — Kafka delivers payloads as bytes. Cast to string before parsing as JSON.

`"order.*"` — expands all sub-columns of the `order` struct into top-level columns. After `from_json`, you have one column called `order` with sub-columns `order_id`, `customer_id`, etc. The `.*` flattens them.

`query.awaitTermination()` — keeps the Python script running. The streaming query runs in a background thread; without this, the script exits immediately and kills the query.

### Step 7 — Run it

Deploy as a continuously running job in Databricks:
```bash
databricks bundle run ingest_orders_stream
```

The job starts and stays running. New orders arrive and are written to the Delta table within seconds.

### Verify

```sql
SELECT COUNT(*) FROM helix_bronze.orders.orders_raw;
-- run again 30 seconds later — count should increase
SELECT MAX(_ingested_at) FROM helix_bronze.orders.orders_raw;
-- should be within the last few seconds
```

---

## Lab DP-03 — SCD Type 2: Customer Dimension

| Field | Value |
|---|---|
| Duration | ~60 minutes |
| File to open | `data_platform/pipelines/silver/dim_customers_scd2.py` |
| Databricks features | Delta MERGE, DeltaTable API, SCD Type 2 |
| Estimated cost | ~€0.05 |

### What is SCD Type 2?

SCD stands for Slowly Changing Dimension. Customer data changes slowly over time: Maarten de Vries moves from Amsterdam to Utrecht. Jana Novák changes her email address.

**SCD Type 1 (overwrite)**: update the existing row. The old city is gone. You know Maarten currently lives in Utrecht, but you cannot answer "where did he live when he placed this order in January?"

**SCD Type 2 (preserve history)**: instead of updating, expire the old row and insert a new one.

| customer_id | city | is_current | valid_from | valid_to |
|---|---|---|---|---|
| C001 | Amsterdam | false | 2024-01-15 | 2026-04-15 |
| C001 | Utrecht | true | 2026-04-16 | 9999-12-31 |

When an analyst joins orders from January 2025 to this dimension table, they use the row where `valid_from <= order_date <= valid_to`. They get Amsterdam — the correct city at the time of the order.

**Why `valid_to = 9999-12-31` for current rows?** When filtering for "current rows" you write `valid_to = '9999-12-31'` or `is_current = true`. Using a far-future date makes the point-in-time join syntax simpler (you can use `BETWEEN`).

### What you will build

`dim_customers_scd2.py` reads the latest customer records from `helix_bronze.customers.customers`, detects changes by comparing a hash of slowly-changing columns, expires changed rows, and inserts new current rows into `helix_silver.customers.dim_customers`.

### Step 1 — Open the file

Open `data_platform/pipelines/silver/dim_customers_scd2.py`.
It is empty right now.

### Step 2 — Write the imports

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws
from delta.tables import DeltaTable
```

- `sha2` — SHA-256 hash function. Hash all slowly-changing columns together. If the hash changes, the row changed.
- `concat_ws("|", col1, col2, ...)` — concatenates column values with a pipe separator before hashing. You need a consistent way to combine multiple columns into one string.
- `DeltaTable` — the Delta Lake Python API. Provides the `.merge()` method for MERGE INTO operations.

### Step 3 — Write the function

```python
def build_dim_customers(spark: SparkSession) -> None:
    source = spark.read.table("helix_bronze.customers.customers")

    source_hashed = source.withColumn(
        "row_hash",
        sha2(concat_ws("|",
            col("email"), col("city"), col("country"), col("phone")
        ), 256)
    )

    target_table = "helix_silver.customers.dim_customers"

    if not spark.catalog.tableExists(target_table):
        (
            source_hashed.select(
                col("customer_id"), col("first_name"), col("last_name"),
                col("email"), col("city"), col("country"), col("phone"),
                col("row_hash"),
                lit(True).alias("is_current"),
                current_timestamp().alias("valid_from"),
                lit("9999-12-31").cast("timestamp").alias("valid_to"),
            )
            .write.format("delta").saveAsTable(target_table)
        )
        return

    target = DeltaTable.forName(spark, target_table)

    target.alias("t").merge(
        source_hashed.alias("s"),
        "t.customer_id = s.customer_id AND t.is_current = true"
    ).whenMatchedUpdate(
        condition = "t.row_hash != s.row_hash",
        set       = {"is_current": "false", "valid_to": "current_timestamp()"},
    ).execute()

    new_rows = (
        source_hashed
        .join(
            spark.read.table(target_table).filter(col("is_current")),
            on = "customer_id",
            how = "left_anti",
        )
        .select(
            col("customer_id"), col("first_name"), col("last_name"),
            col("email"), col("city"), col("country"), col("phone"),
            col("row_hash"),
            lit(True).alias("is_current"),
            current_timestamp().alias("valid_from"),
            lit("9999-12-31").cast("timestamp").alias("valid_to"),
        )
    )

    new_rows.write.format("delta").mode("append").saveAsTable(target_table)

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    build_dim_customers(spark)
```

The merge has two parts:
1. **Expire**: find current rows whose hash changed → mark them expired (`is_current=false`, `valid_to=now`)
2. **Insert new**: find all source rows that have no current match in the target (either new customers, or those just expired) → insert as new current rows

### Verify

```sql
-- After first run: everyone is current
SELECT customer_id, city, is_current, valid_from, valid_to
FROM helix_silver.customers.dim_customers
WHERE customer_id = 'C001';
```

Now simulate a change: update Maarten's city in the Bronze table, then run the pipeline again.

```sql
-- After second run: you should see two rows for C001
SELECT customer_id, city, is_current, valid_from, valid_to
FROM helix_silver.customers.dim_customers
WHERE customer_id = 'C001'
ORDER BY valid_from;
-- Row 1: Amsterdam, is_current=false, valid_to = yesterday
-- Row 2: Utrecht,   is_current=true,  valid_to = 9999-12-31
```

---

## Lab DP-04 — SCD Type 1: Product Dimension

| Field | Value |
|---|---|
| Duration | ~30 minutes |
| File to open | `data_platform/pipelines/silver/dim_products_scd1.py` |
| Databricks features | Delta MERGE, upsert pattern |
| Estimated cost | ~€0.03 |

### What is SCD Type 1?

SCD Type 1 overwrites the current row when data changes. No history is preserved.

Use SCD1 when historical accuracy is not needed. ShopStream's products use SCD1 because when a product's name changes (from "Wireless Headphones 2024" to "Wireless Headphones Pro"), no analyst needs to know the old name. They just want the current name.

Compare to SCD2: if a customer moves cities, knowing their historical city matters for "where was this order delivered?" For a product name change, historical queries do not need the old name.

### What you will build

`dim_products_scd1.py` reads products from `helix_bronze.products.products` and upserts them into `helix_silver.products.dim_products`. Updated products overwrite the existing row. New products are inserted.

### Step 1 — Open the file

Open `data_platform/pipelines/silver/dim_products_scd1.py`.
It is empty right now.

### Step 2 — Write the function

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

def build_dim_products(spark: SparkSession) -> None:
    source = spark.read.table("helix_bronze.products.products")
    target_table = "helix_silver.products.dim_products"

    if not spark.catalog.tableExists(target_table):
        (
            source.select(
                col("product_id"), col("product_name"), col("category"),
                col("unit_price"), col("supplier_id"),
                current_timestamp().alias("_last_updated"),
            )
            .write.format("delta").saveAsTable(target_table)
        )
        return

    target = DeltaTable.forName(spark, target_table)

    target.alias("t").merge(
        source.alias("s"),
        "t.product_id = s.product_id"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    build_dim_products(spark)
```

`whenMatchedUpdateAll()` — if `product_id` exists in both source and target, overwrite all columns.
`whenNotMatchedInsertAll()` — if `product_id` is in source but not in target, insert the row.

No `valid_from`/`valid_to` columns. SCD1 is simpler — only the current state.

### Verify

```sql
SELECT product_id, product_name, category, unit_price
FROM helix_silver.products.dim_products
LIMIT 10;
```

---

## Lab DP-05 — Truncate and Load: Regions

| Field | Value |
|---|---|
| Duration | ~20 minutes |
| File to open | `data_platform/pipelines/silver/dim_regions_truncate_load.py` |
| Databricks features | Delta overwrite, schema evolution |
| Estimated cost | ~€0.02 |

### What is Truncate and Load?

Truncate and Load deletes all rows from the target table and inserts all rows from the source in one operation. It is the simplest transformation pattern.

**When to use it:**
- The table is small (hundreds of rows, not millions)
- The entire table changes when updated (not just a few rows)
- Historical values are not needed

ShopStream's regions table maps Dutch region codes to DHL delivery zones. It has about 80 rows. When a new postal code zone is added or a zone boundary changes, the entire mapping is replaced. SCD1 or SCD2 would be overkill — just delete and reload.

**Risk:** if the new data has fewer rows than expected, you might truncate valid data. Always validate row counts before overwriting critical tables.

### Step 1 — Open the file

Open `data_platform/pipelines/silver/dim_regions_truncate_load.py`.
It is empty right now.

### Step 2 — Write the function

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def load_dim_regions(spark: SparkSession) -> None:
    source = spark.read.table("helix_bronze.regions.regions")

    source_count = source.count()
    if source_count < 50:
        raise ValueError(f"Source has only {source_count} rows — expected at least 50. Aborting to protect target.")

    (
        source
        .withColumn("_loaded_at", current_timestamp())
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("helix_silver.regions.dim_regions")
    )

    print(f"Loaded {source_count} regions")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    load_dim_regions(spark)
```

`.mode("overwrite")` — delete all existing rows before writing.
`.option("overwriteSchema", "true")` — also allow schema changes. Without this, Delta refuses to overwrite if the column types or names changed.

The row count check is a safety guard: if something goes wrong upstream and the regions table becomes empty, this pipeline raises an error instead of deleting all your Silver data.

### Verify

```sql
SELECT COUNT(*) FROM helix_silver.regions.dim_regions;
-- Should be ~80

SELECT * FROM helix_silver.regions.dim_regions LIMIT 5;
```

---

## Lab DP-06 — Data Quality: Fact Orders

| Field | Value |
|---|---|
| Duration | ~60 minutes |
| File to open | `data_platform/pipelines/silver/fct_orders.py` |
| Databricks features | Delta append, data quality checks, quarantine pattern |
| Estimated cost | ~€0.05 |

### What is a Fact Table?

A fact table records events. Each row is one thing that happened. In ShopStream, each row is one order line: "customer C001 bought 2 units of product P042 for €29.99 each on 4 May 2026."

Facts are always append-only. You never delete or update historical order rows — that would change the historical record. If an order is cancelled, you add a cancellation event, not delete the original order row.

### What is a quarantine table?

Not all data in Bronze is valid. A Bronze row might have:
- A `customer_id` that does not exist in `dim_customers` (the customer was deleted from the source)
- A negative `unit_price` (data entry error)
- A null `order_id` (upstream bug)

If you load this bad data into the Silver fact table, your ML models train on wrong data. Instead, route bad rows to a quarantine table (`fct_orders_quarantine`) for investigation. Clean rows go to the main table.

### Step 1 — Open the file

Open `data_platform/pipelines/silver/fct_orders.py`.
It is empty right now.

### Step 2 — Write the imports

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit
```

### Step 3 — Write the function

```python
def build_fct_orders(spark: SparkSession) -> None:
    raw       = spark.read.table("helix_bronze.orders.orders_raw")
    customers = spark.read.table("helix_silver.customers.dim_customers") \
        .filter(col("is_current")).select("customer_id")
    products  = spark.read.table("helix_silver.products.dim_products").select("product_id")

    valid_customer_ids = [r.customer_id for r in customers.collect()]
    valid_product_ids  = [r.product_id  for r in products.collect()]

    parsed = raw.withColumn(
        "order_ts_parsed",
        to_timestamp(col("order_ts"), "yyyy-MM-dd'T'HH:mm:ss")
    )

    is_valid = (
        col("order_id").isNotNull()
        & col("customer_id").isin(valid_customer_ids)
        & col("product_id").isin(valid_product_ids)
        & (col("unit_price") > 0)
        & col("order_ts_parsed").isNotNull()
    )

    good = parsed.filter(is_valid)
    bad  = parsed.filter(~is_valid)

    (
        good.select(
            col("order_id"), col("customer_id"), col("product_id"),
            col("quantity"), col("unit_price"),
            (col("quantity") * col("unit_price")).alias("line_total"),
            col("order_ts_parsed").alias("order_ts"),
            current_timestamp().alias("_loaded_at"),
        )
        .write.format("delta").mode("append")
        .saveAsTable("helix_silver.orders.fct_orders")
    )

    if bad.count() > 0:
        print(f"WARNING: {bad.count()} rows failed validation — writing to quarantine")
        (
            bad.withColumn("_quarantine_reason", lit("validation_failed"))
               .withColumn("_loaded_at", current_timestamp())
               .write.format("delta").mode("append")
               .saveAsTable("helix_silver.orders.fct_orders_quarantine")
        )

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    build_fct_orders(spark)
```

`(col("quantity") * col("unit_price")).alias("line_total")` — compute `line_total` at load time. Every downstream query that needs order revenue no longer recomputes this multiplication. Pre-computed columns save query time in Gold and ML.

### Verify

```sql
SELECT COUNT(*) AS valid_orders FROM helix_silver.orders.fct_orders;
SELECT COUNT(*) AS quarantined  FROM helix_silver.orders.fct_orders_quarantine;

SELECT order_id, customer_id, product_id, quantity, unit_price, line_total
FROM helix_silver.orders.fct_orders
LIMIT 5;
```

---

## Lab DP-07 — Gold Aggregations

| Field | Value |
|---|---|
| Duration | ~60 minutes |
| Files to open | `gold/customer_metrics.py`, `gold/product_performance.py`, `gold/revenue_daily.py` |
| Databricks features | Delta overwrite, aggregations, Gold layer |
| Estimated cost | ~€0.08 |

### Why aggregate into Gold?

Silver tables are normalised and correct, but joining `fct_orders` (100 million rows) to `dim_customers` to compute "total spend per customer" takes minutes. A ML model that needs this for every training example would be unusably slow.

Gold tables pre-compute the joins and aggregations. The ML churn model reads one row per customer from `fct_customer_metrics` — each row already has `total_orders`, `total_spend`, `avg_order_value`, `days_since_last_order`. No joins needed at training time.

Gold is rebuilt completely from Silver on every run (`mode("overwrite")`). This is safe because Silver is the source of truth. Gold is derived data — a view materialised as a table.

### customer_metrics.py

Open `data_platform/pipelines/gold/customer_metrics.py` — it is empty right now.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max,
    datediff, current_date, round as spark_round
)

def build_customer_metrics(spark: SparkSession) -> None:
    orders    = spark.read.table("helix_silver.orders.fct_orders")
    returns   = spark.read.table("helix_silver.returns.fct_returns")
    customers = spark.read.table("helix_silver.customers.dim_customers") \
        .filter(col("is_current")) \
        .select("customer_id", "first_name", "last_name", "city", "country")

    order_metrics = (
        orders.groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("line_total").alias("total_spend"),
            avg("line_total").alias("avg_order_value"),
            spark_max("order_ts").alias("last_order_ts"),
        )
        .withColumn("days_since_last_order",
            datediff(current_date(), col("last_order_ts").cast("date")))
        .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
    )

    return_metrics = (
        returns.groupBy("customer_id")
        .agg(
            count("return_id").alias("total_returns"),
            spark_sum("refund_amount").alias("total_refunded"),
        )
    )

    result = (
        customers
        .join(order_metrics,  on="customer_id", how="left")
        .join(return_metrics, on="customer_id", how="left")
        .fillna(0, subset=["total_orders","total_spend","avg_order_value",
                            "total_returns","total_refunded"])
    )

    (
        result.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("helix_gold.customers.fct_customer_metrics")
    )

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    build_customer_metrics(spark)
```

`how="left"` join — keep all customers even if they have no orders yet (new sign-ups who never bought). `.fillna(0)` replaces the resulting nulls with zeros.

Write `product_performance.py` and `revenue_daily.py` following the same pattern (see the Phase 02 doc for the full code).

### Verify

```sql
SELECT customer_id, total_orders, total_spend, days_since_last_order
FROM helix_gold.customers.fct_customer_metrics
ORDER BY total_spend DESC
LIMIT 10;

SELECT order_date, num_orders, gross_revenue, total_refunds, net_revenue
FROM helix_gold.revenue.fct_revenue_daily
ORDER BY order_date DESC
LIMIT 7;
```

---

## Lab DP-08 — Delta Time Travel

| Field | Value |
|---|---|
| Duration | ~30 minutes |
| File to open | None — run SQL queries in the Databricks SQL Editor |
| Databricks features | Delta time travel, transaction log |
| Estimated cost | ~€0.01 |

### What is Delta time travel?

Every write to a Delta table creates a new version in the transaction log. Delta keeps the Parquet files for old versions (unless you run `VACUUM`). This means you can query any previous version of the table.

When is this useful?
- A pipeline ran with a bug and wrote wrong data. You need to restore the table to before the bug.
- A business analyst asks "what did this table look like last Monday?" for an audit.
- You are building a point-in-time join and need a specific historical snapshot.

Think of Git commits: you can `git checkout v3` to see the repository at a previous state. Delta time travel is the same concept for data.

### Step 1 — See the current version

```sql
DESCRIBE HISTORY helix_silver.orders.fct_orders;
```

This shows every version of the table: who wrote it, when, what operation (WRITE, MERGE, DELETE), how many rows were added or removed.

### Step 2 — Query a specific version

```sql
-- Query version 0 (the initial load)
SELECT COUNT(*) FROM helix_silver.orders.fct_orders VERSION AS OF 0;

-- Query the current version
SELECT COUNT(*) FROM helix_silver.orders.fct_orders;
```

The count for version 0 should be lower if subsequent runs added more rows.

### Step 3 — Query by timestamp

```sql
-- Query the table as it was at a specific time
SELECT COUNT(*)
FROM helix_silver.orders.fct_orders
TIMESTAMP AS OF '2026-05-01 12:00:00';
```

### Step 4 — Restore a table to a previous version

```sql
-- Restore to version 2 (use carefully — this overwrites current state)
RESTORE TABLE helix_silver.orders.fct_orders TO VERSION AS OF 2;
```

You would use this if a pipeline bug corrupted the table and you need to roll back.

### Step 5 — What happens if you run VACUUM?

```sql
-- VACUUM removes old Parquet files older than 7 days (default retention)
-- After VACUUM, time travel to versions older than 7 days is no longer possible
VACUUM helix_silver.orders.fct_orders;
-- To check what would be deleted without deleting: add DRY RUN
VACUUM helix_silver.orders.fct_orders DRY RUN;
```

---

## Lab DP-09 — Change Data Feed

| Field | Value |
|---|---|
| Duration | ~30 minutes |
| File to open | None — enable via SQL, observe via SQL |
| Databricks features | Delta Change Data Feed (CDF) |
| Estimated cost | ~€0.01 |

### What is Change Data Feed?

Change Data Feed (CDF) is a feature that records every change to a Delta table — inserts, updates, and deletes — in a separate change log. A downstream job can read only the changes since its last run instead of reading the entire table.

Example: `fct_customer_metrics` has 500,000 rows. After a daily run of the Gold pipeline, only 2,000 customers changed. Without CDF, a downstream ML feature pipeline reads all 500,000 rows every run. With CDF, it reads only the 2,000 changed rows — 250x less data.

CDF is especially valuable for incremental ML feature pipelines and real-time CDC (Change Data Capture) from source systems.

### Step 1 — Enable CDF on a table

```sql
ALTER TABLE helix_gold.customers.fct_customer_metrics
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

CDF only records changes after it is enabled. Historical changes are not retroactively logged.

### Step 2 — Make some changes

Run your `customer_metrics.py` pipeline again (or `UPDATE`/`INSERT` a few rows manually) to generate some changes.

### Step 3 — Read the changes

```sql
-- Read all changes from version 1 onwards
SELECT * FROM table_changes('helix_gold.customers.fct_customer_metrics', 1);
```

You will see a `_change_type` column with values: `insert`, `update_preimage` (old row before update), `update_postimage` (new row after update), `delete`.

```sql
-- Read only inserts and updates since a timestamp
SELECT customer_id, total_spend, _change_type, _commit_timestamp
FROM table_changes('helix_gold.customers.fct_customer_metrics', '2026-05-01')
WHERE _change_type IN ('insert', 'update_postimage')
ORDER BY _commit_timestamp DESC;
```

---

## Lab DP-10 — Point-in-Time Join

| Field | Value |
|---|---|
| Duration | ~30 minutes |
| File to open | None — run SQL queries in the Databricks SQL Editor |
| Databricks features | Delta AS OF, point-in-time joins |
| Estimated cost | ~€0.01 |

### What is a Point-in-Time Join?

A point-in-time join answers: "what was true about this dimension *at the time of this event*?"

ShopStream example: an order was placed on 1 March 2026. Maarten de Vries moved from Amsterdam to Utrecht on 15 April 2026. If you join the March order to the *current* customer dimension, you get Utrecht — wrong. You want Amsterdam, the city where he lived when he placed the order.

With SCD Type 2, every version of a customer's record is stored with `valid_from` and `valid_to`. A point-in-time join finds the row where the order date falls between `valid_from` and `valid_to`.

### Step 1 — Plain point-in-time join

```sql
SELECT
    o.order_id,
    o.order_ts,
    o.line_total,
    c.city       AS customer_city_at_order_time,
    c.country,
    c.is_current AS customer_currently_active
FROM helix_silver.orders.fct_orders o
JOIN helix_silver.customers.dim_customers c
  ON o.customer_id = c.customer_id
 AND o.order_ts BETWEEN c.valid_from AND c.valid_to
LIMIT 20;
```

The `BETWEEN` condition selects the SCD2 row that was active at the time of the order.

### Step 2 — Using Delta AS OF for time travel joins

```sql
-- Join orders from the current table to the customer dimension as it was 30 days ago
SELECT
    o.order_id,
    o.line_total,
    c_old.city AS city_30_days_ago
FROM helix_silver.orders.fct_orders o
JOIN (
    SELECT * FROM helix_silver.customers.dim_customers
    TIMESTAMP AS OF (current_timestamp() - INTERVAL 30 DAYS)
) c_old
  ON o.customer_id = c_old.customer_id
 AND c_old.is_current = true
LIMIT 10;
```

---

## Lab DP-11 — OPTIMIZE and Z-ORDER

| Field | Value |
|---|---|
| Duration | ~30 minutes |
| File to open | None — run SQL commands in the Databricks SQL Editor |
| Databricks features | Delta OPTIMIZE, Z-ORDER, file compaction |
| Estimated cost | ~€0.05 |

### What is the small-file problem?

Every Spark write job creates multiple Parquet files — one per partition, per worker. An append-heavy table like `fct_orders` accumulates thousands of tiny Parquet files over time. Reading the table means opening thousands of files, which has high overhead even if the total data is small.

OPTIMIZE compacts small files into larger ones. One large file (128 MB) is much faster to read than 1,000 files of 128 KB each.

### What is Z-ORDER?

Z-ORDER is a data layout optimisation. It physically co-locates rows that have similar values in a column.

Example: most queries on `fct_orders` filter by `customer_id` and date:
```sql
SELECT * FROM helix_silver.orders.fct_orders WHERE customer_id = 'C001' AND order_ts > '2026-01-01'
```

Without Z-ORDER, rows for C001 are scattered across all Parquet files. Spark must scan every file.

With `ZORDER BY (customer_id, order_ts)`, rows for C001 are co-located in a few files. Spark reads only those files — 10x fewer I/O operations.

Z-ORDER works best on high-cardinality columns you frequently filter on. Use it on `customer_id`, `product_id`, `order_date` — not on `country` (low cardinality, only a few distinct values).

### Step 1 — Run OPTIMIZE without Z-ORDER (compaction only)

```sql
OPTIMIZE helix_silver.orders.fct_orders;
```

After this: `DESCRIBE DETAIL helix_silver.orders.fct_orders` shows the number of files. It should decrease significantly.

### Step 2 — Run OPTIMIZE with Z-ORDER

```sql
OPTIMIZE helix_silver.orders.fct_orders
ZORDER BY (customer_id, order_ts);
```

This both compacts files AND reorders data for faster customer + date range queries.

### Step 3 — Measure the improvement

```sql
-- Before OPTIMIZE: run this and note the query time
SELECT COUNT(*) FROM helix_silver.orders.fct_orders WHERE customer_id = 'C001';

-- Run OPTIMIZE WITH ZORDER, then run the same query again
-- The second run should be faster
```

### Step 4 — Schedule OPTIMIZE regularly

For tables with many daily writes, run OPTIMIZE once per day or week. Add it to your DAB job as a final task after the main pipeline tasks.

---

## Lab DP-12 — Broadcast Join and AQE

| Field | Value |
|---|---|
| Duration | ~45 minutes |
| File to open | `data_platform/optimizations/broadcast_joins.py` |
| Databricks features | Broadcast join, Adaptive Query Execution (AQE) |
| Estimated cost | ~€0.05 |

### What is a Broadcast Join?

A regular Spark join shuffles both DataFrames across the network so rows with matching keys land on the same worker. Shuffling is expensive — it involves serialising, sending, and deserialising millions of rows.

A broadcast join avoids the shuffle for small DataFrames. Spark copies the entire small DataFrame to every worker. Each worker then joins its partition of the large DataFrame locally, with no network traffic.

ShopStream example: joining 50 million orders (`fct_orders`) to 80 regions (`dim_regions`). Without broadcast: shuffle 50 million rows. With broadcast: copy 80 rows to each of 8 workers. The broadcast is 1,000x more efficient here.

Rule of thumb: broadcast when one DataFrame is under ~10 MB (adjustable via `spark.sql.autoBroadcastJoinThreshold`).

### What is AQE?

Adaptive Query Execution (AQE) lets Spark change its execution plan at runtime based on what it actually sees in the data.

Without AQE: Spark plans the entire job upfront. If it guesses a partition has 1 million rows but actually has 10 million, the plan is suboptimal.

With AQE (enabled by default on Databricks): Spark re-evaluates the plan after each shuffle. It can:
- Merge small partitions (reduce number of output tasks)
- Convert a sort-merge join to a broadcast join if one side turns out to be small
- Handle data skew by splitting large partitions

### Step 1 — Open the file

Open `data_platform/optimizations/broadcast_joins.py` — it is empty right now.

### Step 2 — Write the optimisation examples

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

def demo_broadcast_join(spark: SparkSession) -> None:
    orders  = spark.read.table("helix_silver.orders.fct_orders")
    regions = spark.read.table("helix_silver.regions.dim_regions")
    customers = spark.read.table("helix_silver.customers.dim_customers") \
        .filter("is_current = true")

    result = (
        orders
        .join(broadcast(regions),   on="region_code",   how="left")
        .join(broadcast(customers), on="customer_id",   how="left")
        .select(
            "order_id", "line_total",
            "city", "country",
            "region_name", "dhl_zone",
        )
    )

    result.explain(mode="extended")

    result.write.format("delta").mode("overwrite") \
        .saveAsTable("helix_gold.orders.fct_orders_enriched")

def demo_aqe(spark: SparkSession) -> None:
    print("AQE settings:")
    print("  adaptive enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
    print("  advisory partition size:", spark.conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes"))

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    demo_aqe(spark)
    demo_broadcast_join(spark)
```

`result.explain(mode="extended")` — prints the physical query plan. Look for `BroadcastHashJoin` (good, no shuffle) vs `SortMergeJoin` (shuffle needed). After adding `broadcast(regions)`, you should see `BroadcastHashJoin`.

### Verify

In the Spark UI (linked from the job run in Databricks), go to the SQL tab and look at the DAG. A broadcast join shows a small "broadcast" node — no shuffle stage. A sort-merge join shows a larger shuffle stage.

---

## Lab DP-13 — Delta Sharing

| Field | Value |
|---|---|
| Duration | ~30 minutes |
| File to open | None — configure via the Databricks UI |
| Databricks features | Delta Sharing, Unity Catalog |
| Estimated cost | ~€0 (configuration only) |

### What is Delta Sharing?

Delta Sharing is an open protocol for sharing live Delta tables with recipients outside your Databricks workspace — other organisations, other cloud platforms, or BI tools — without copying data and without requiring the recipient to have Databricks.

ShopStream use case: the finance team uses Tableau connected to a local SQL Server. They need daily revenue data. Without Delta Sharing: export CSVs, FTP to the finance server, import into SQL Server. With Delta Sharing: finance team connects their Tableau directly to the shared Delta table and always sees live data.

Delta Sharing is read-only for recipients. They cannot write to your tables. The sharing is governed by Unity Catalog — you control exactly which tables and columns are shared.

### Step 1 — Create a share

In the Databricks UI:
1. Catalog Explorer → Delta Sharing → Shared by me → Create share
2. Name: `helix-finance-share`
3. Add table: `helix_gold.revenue.fct_revenue_daily`
4. Select columns to share (you can exclude sensitive columns)

### Step 2 — Create a recipient

1. Delta Sharing → Recipients → Create recipient
2. Name: `finance-tableau`
3. Authentication type: `Token` (for non-Databricks recipients)
4. Download the activation link — send this to the finance team

### Step 3 — What the recipient sees

The finance team activates their token and gets a connection profile JSON file. In Tableau, they use the Delta Sharing connector with this profile. They see `fct_revenue_daily` as a live, read-only table.

When you add new rows to `fct_revenue_daily` (daily Gold pipeline run), the finance team sees them automatically — no data copying needed.

### Step 4 — Verify sharing from your side

```sql
-- Show what is in your share
SHOW SHARES;

-- Show recipients and their access
SHOW GRANT ON SHARE helix-finance-share;
```

### Step 5 — Revoke access

If the finance team no longer needs access:
1. Delta Sharing → Recipients → `finance-tableau` → Revoke
2. Their token is immediately invalidated. No further data access.
