# Databricks notebook source
# Silver fact table for orders — runs as a Databricks Job notebook task.
# spark is injected automatically — do not call SparkSession.builder here.
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit

spark.sql("CREATE SCHEMA IF NOT EXISTS helix_silver.orders")

raw = spark.read.table("helix_bronze.orders.orders_raw")
valid_customers = (
    spark.read.table("helix_silver.customers.dim_customers")
    .filter(col("is_current"))
    .select("customer_id")
)
valid_products = spark.read.table("helix_silver.products.dim_products").select("product_id")

parsed = raw.withColumn(
    "order_ts_parsed",
    to_timestamp(col("order_ts"), "yyyy-MM-dd'T'HH:mm:ss")
)

# Collect valid keys as broadcast sets for FK validation
valid_customer_ids = {r.customer_id for r in valid_customers.collect()}
valid_product_ids  = {r.product_id  for r in valid_products.collect()}

valid_flag = (
    col("order_id").isNotNull()
    & col("customer_id").isin(list(valid_customer_ids))
    & col("product_id").isin(list(valid_product_ids))
    & (col("unit_price") > 0)
    & col("order_ts_parsed").isNotNull()
)

good = parsed.filter(valid_flag)
bad  = parsed.filter(~valid_flag)

(
    good.select(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("quantity"),
        col("unit_price"),
        (col("quantity") * col("unit_price")).alias("line_total"),
        col("order_ts_parsed").alias("order_ts"),
        current_timestamp().alias("_loaded_at"),
    )
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("helix_silver.orders.fct_orders")
)

if bad.count() > 0:
    (
        bad.withColumn("_quarantine_reason", lit("validation_failed"))
           .withColumn("_loaded_at", current_timestamp())
           .write.format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable("helix_silver.orders.fct_orders_quarantine")
    )

print(f"fct_orders: {good.count()} good rows, {bad.count()} quarantined")