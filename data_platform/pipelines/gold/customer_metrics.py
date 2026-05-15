# Databricks notebook source
# Gold customer metrics aggregate — runs as a Databricks Job notebook task.
# spark is injected automatically — do not call SparkSession.builder here.
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max,
    datediff, current_date, round as spark_round
)

spark.sql("CREATE SCHEMA IF NOT EXISTS helix_gold.customers")

orders  = spark.read.table("helix_silver.orders.fct_orders")
returns = spark.read.table("helix_silver.returns.fct_returns")
customers = (
    spark.read.table("helix_silver.customers.dim_customers")
    .filter(col("is_current"))
    .select("customer_id", "first_name", "last_name", "city", "country")
)

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

metrics = (
    customers
    .join(order_metrics,  on="customer_id", how="left")
    .join(return_metrics, on="customer_id", how="left")
    .fillna(0, subset=["total_orders", "total_spend", "avg_order_value", "total_returns", "total_refunded"])
)

(
    metrics.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("helix_gold.customers.fct_customer_metrics")
)

print(f"fct_customer_metrics: {metrics.count()} customers")
    build_customer_metrics(spark)