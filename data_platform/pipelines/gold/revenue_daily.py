# Databricks notebook source
# Gold daily revenue aggregate — runs as a Databricks Job notebook task.
# spark is injected automatically — do not call SparkSession.builder here.
from pyspark.sql.functions import (
    col, date_trunc, sum as spark_sum, count,
    round as spark_round, current_timestamp
)

spark.sql("CREATE SCHEMA IF NOT EXISTS helix_gold.revenue")

orders  = spark.read.table("helix_silver.orders.fct_orders")
returns = spark.read.table("helix_silver.returns.fct_returns")

daily_orders = (
    orders
    .withColumn("order_date", date_trunc("day", col("order_ts")).cast("date"))
    .groupBy("order_date")
    .agg(
        count("order_id").alias("num_orders"),
        spark_sum("line_total").alias("gross_revenue"),
    )
)

daily_returns = (
    returns
    .groupBy("return_date")
    .agg(spark_sum("refund_amount").alias("total_refunds"))
    .withColumnRenamed("return_date", "order_date")
)

revenue = (
    daily_orders
    .join(daily_returns, on="order_date", how="left")
    .fillna(0, subset=["total_refunds"])
    .withColumn("net_revenue",
        spark_round(col("gross_revenue") - col("total_refunds"), 2))
    .orderBy("order_date")
)

(
    revenue.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("helix_gold.revenue.fct_revenue_daily")
)

print(f"fct_revenue_daily: {revenue.count()} date rows")
    build_revenue_daily(spark)