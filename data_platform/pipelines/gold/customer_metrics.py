# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max,
    datediff, current_date, round as spark_round
)

def build_customer_metrics(spark: SparkSession) -> None:
    orders  = spark.read.table("helix_silver.orders.fct_orders")
    returns = spark.read.table("helix_silver.returns.fct_returns")
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

    metrics = (
        customers
        .join(order_metrics,  on="customer_id", how="left")
        .join(return_metrics, on="customer_id", how="left")
        .fillna(0, subset=["total_orders","total_spend","avg_order_value","total_returns","total_refunded"])
    )

    (
        metrics.write
        .format("delta")
        .mode("overwrite")  #Gold tables use mode("overwrite") — they are rebuilt entirely each run from the current Silver state.
        .option("overwriteSchema", "true")
        .saveAsTable("helix_gold.customers.fct_customer_metrics")
    )

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    build_customer_metrics(spark)