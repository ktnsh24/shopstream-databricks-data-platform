# Databricks notebook source
# Gold product performance aggregate — runs as a Databricks Job notebook task.
# spark is injected automatically — do not call SparkSession.builder here.
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg,
    round as spark_round, current_timestamp, when
)

spark.sql("CREATE SCHEMA IF NOT EXISTS helix_gold.products")

orders   = spark.read.table("helix_silver.orders.fct_orders")
returns  = spark.read.table("helix_silver.returns.fct_returns")
products = (
    spark.read.table("helix_silver.products.dim_products")
    .select("product_id", "product_name", "category", "unit_price")
)

order_perf = (
    orders.groupBy("product_id")
    .agg(
        count("order_id").alias("total_units_sold"),
        spark_sum("line_total").alias("total_revenue"),
        avg("quantity").alias("avg_quantity_per_order"),
    )
)

return_rate = (
    returns.groupBy("product_id")
    .agg(count("return_id").alias("total_returns"))
)

perf = (
    products
    .join(order_perf,  on="product_id", how="left")
    .join(return_rate, on="product_id", how="left")
    .fillna(0)
    .withColumn("return_rate_pct",
          spark_round(
              when(col("total_units_sold") > 0,
                   col("total_returns") / col("total_units_sold") * 100
              ).otherwise(0.0),
          1))
)

(
    perf.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("helix_gold.products.fct_product_performance")
)

print(f"fct_product_performance: {perf.count()} products")