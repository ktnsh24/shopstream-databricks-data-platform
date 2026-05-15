# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_trunc, sum as spark_sum, count,
    round as spark_round, current_timestamp
)

def build_revenue_daily(spark: SparkSession) -> None:
    orders  = spark.read.table("helix_silver.orders.fct_orders")
    returns = spark.read.table("helix_silver.returns.fct_returns")

    daily_orders = (
        orders
        .withColumn("order_date", date_trunc("day", col("order_ts")).cast("date")) # truncates the timestamp to midnight. 2026-05-04 14:32:17 becomes 2026-05-04 00:00:00
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

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    build_revenue_daily(spark)