# Databricks notebook source
# Silver fact table for returns — runs as a Databricks Job notebook task.
# spark is injected automatically — do not call SparkSession.builder here.
from pyspark.sql.functions import col, to_date, current_timestamp, lit

spark.sql("CREATE SCHEMA IF NOT EXISTS helix_silver.returns")

raw    = spark.read.table("helix_bronze.returns.returns_raw")
orders = spark.read.table("helix_silver.orders.fct_orders").select("order_id")

parsed = raw.withColumn("return_date_parsed", to_date(col("return_date"), "yyyy-MM-dd"))

valid_order_ids = {r.order_id for r in orders.collect()}

valid_flag = (
    col("return_id").isNotNull()
    & col("order_id").isin(list(valid_order_ids))
    & (col("refund_amount") >= 0)
)

good = parsed.filter(valid_flag)
bad  = parsed.filter(~valid_flag)

(
    good.select(
        col("return_id"), col("order_id"), col("customer_id"),
        col("product_id"), col("return_date_parsed").alias("return_date"),
        col("reason_code"), col("refund_amount"),
        current_timestamp().alias("_loaded_at"),
    )
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("helix_silver.returns.fct_returns")
)

if bad.count() > 0:
    (
        bad.withColumn("_quarantine_reason", lit("validation_failed"))
           .withColumn("_loaded_at", current_timestamp())
           .write.format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable("helix_silver.returns.fct_returns_quarantine")
    )

print(f"fct_returns: {good.count()} good rows, {bad.count()} quarantined")