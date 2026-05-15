# Databricks notebook source
# SCD1 product dimension — runs as a Databricks Job notebook task.
# spark is injected automatically — do not call SparkSession.builder here.
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

spark.sql("CREATE SCHEMA IF NOT EXISTS helix_silver.products")

source = spark.read.table("helix_bronze.products.products")
target_table = "helix_silver.products.dim_products"

if not spark.catalog.tableExists(target_table):
    (
        source.select(
            col("product_id"),
            col("product_name"),
            col("category"),
            col("unit_price"),
            col("supplier_id"),
            current_timestamp().alias("_last_updated"),
        )
        .write.format("delta").saveAsTable(target_table)
    )
else:
    target = DeltaTable.forName(spark, target_table)
    target.alias("t").merge(
        source.alias("s"),
        "t.product_id = s.product_id"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

print(f"dim_products updated: {spark.table(target_table).count()} total rows")