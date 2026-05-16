# Databricks notebook source
# One-time seeding notebook — bypasses Event Hub and ADLS upload steps.
#
# Run this in Databricks when:
#   - helix_bronze.orders.orders_raw is empty (streaming pipeline never ran)
#   - helix_silver.orders.fct_orders is empty (no bronze data to process)
#
# What this does:
#   1. Reads valid customer_id + product_id from silver (already populated by batch job)
#   2. Generates ~2000 fake orders with the correct bronze schema
#   3. Writes directly to helix_bronze.orders.orders_raw (overwrite)
#   4. Generates ~300 returns and uploads a CSV to ADLS /raw/returns/
#
# After running this notebook, re-run the helix_nightly_batch job.
# The fct_orders and fct_returns tasks will pick up the seeded bronze data.

# COMMAND ----------

import random
import uuid
from datetime import datetime, timezone, timedelta

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
)

STORAGE_ACCOUNT = "helixdatalfqrcq"
RETURNS_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/returns/"

# COMMAND ----------
# Step 1 — collect valid FK values from silver

valid_customers = [
    r.customer_id
    for r in spark.read.table("helix_silver.customers.dim_customers")
    .filter("is_current = true")
    .select("customer_id")
    .limit(5000)
    .collect()
]

valid_products = [
    r.product_id
    for r in spark.read.table("helix_silver.products.dim_products")
    .select("product_id")
    .limit(5000)
    .collect()
]

print(f"Valid customers: {len(valid_customers)}, valid products: {len(valid_products)}")

# COMMAND ----------
# Step 2 — generate fake orders using the bronze schema

ORDER_SCHEMA = StructType([
    StructField("order_id",        StringType(),   nullable=False),
    StructField("customer_id",     StringType(),   nullable=False),
    StructField("product_id",      StringType(),   nullable=False),
    StructField("quantity",        IntegerType(),  nullable=True),
    StructField("unit_price",      DoubleType(),   nullable=True),
    StructField("order_ts",        StringType(),   nullable=True),
    StructField("raw_payload",     StringType(),   nullable=True),
    StructField("kafka_partition", IntegerType(),  nullable=True),
    StructField("kafka_offset",    LongType(),     nullable=True),
    StructField("event_time",      TimestampType(), nullable=True),
    StructField("_ingested_at",    TimestampType(), nullable=True),
])

NUM_ORDERS = 2000
now = datetime.now(timezone.utc)

order_ids = []
rows = []
for i in range(NUM_ORDERS):
    order_id = f"ORD{uuid.uuid4().hex[:7].upper()}"
    customer_id = random.choice(valid_customers)
    product_id = random.choice(valid_products)
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(5.0, 500.0), 2)
    days_ago = random.randint(0, 180)
    order_ts = (now - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%S")

    order_ids.append((order_id, customer_id, product_id))
    rows.append(Row(
        order_id=order_id,
        customer_id=customer_id,
        product_id=product_id,
        quantity=quantity,
        unit_price=unit_price,
        order_ts=order_ts,
        raw_payload=None,
        kafka_partition=0,
        kafka_offset=i,
        event_time=now - timedelta(days=days_ago),
        _ingested_at=now,
    ))

orders_df = spark.createDataFrame(rows, schema=ORDER_SCHEMA)

spark.sql("CREATE SCHEMA IF NOT EXISTS helix_bronze.orders")
(
    orders_df
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("helix_bronze.orders.orders_raw")
)

print(f"Seeded {orders_df.count()} rows into helix_bronze.orders.orders_raw")

# COMMAND ----------
# Step 3 — generate returns CSV and write to ADLS /raw/returns/
# ingest_returns_autoloader reads from this path with schema: reason_code (not reason)

REASONS = ["damaged", "wrong_size", "wrong_item", "changed_mind", "not_as_described", "arrived_late"]
NUM_RETURNS = 300

returns_rows = []
for _ in range(NUM_RETURNS):
    order_id, customer_id, product_id = random.choice(order_ids)
    days_ago = random.randint(0, 90)
    return_date = (now - timedelta(days=days_ago)).strftime("%Y-%m-%d")
    returns_rows.append(Row(
        return_id=f"R{uuid.uuid4().hex[:8].upper()}",
        order_id=order_id,
        customer_id=customer_id,
        product_id=product_id,
        return_date=return_date,
        reason_code=random.choice(REASONS),
        refund_amount=round(random.uniform(5.0, 500.0), 2),
    ))

from pyspark.sql.types import DoubleType as DT
RETURNS_SCHEMA = StructType([
    StructField("return_id",     StringType(), nullable=False),
    StructField("order_id",      StringType(), nullable=False),
    StructField("customer_id",   StringType(), nullable=False),
    StructField("product_id",    StringType(), nullable=False),
    StructField("return_date",   StringType(), nullable=True),
    StructField("reason_code",   StringType(), nullable=True),
    StructField("refund_amount", DoubleType(), nullable=True),
])

returns_df = spark.createDataFrame(returns_rows, schema=RETURNS_SCHEMA)

today_str = now.strftime("%Y%m%d")
(
    returns_df
    .coalesce(1)
    .write.format("csv")
    .option("header", "true")
    .mode("overwrite")
    .save(f"{RETURNS_PATH}returns_{today_str}/")
)

print(f"Uploaded {returns_df.count()} returns to {RETURNS_PATH}returns_{today_str}/")
print("\nDone. Now re-run the helix_nightly_batch job.")
