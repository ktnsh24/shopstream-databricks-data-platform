from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, when, current_timestamp, lit
)

def build_fct_orders(spark: SparkSession) -> None:
    raw = spark.read.table("helix_bronze.orders.orders_raw")
    valid_customers = spark.read.table("helix_silver.customers.dim_customers") \
        .filter(col("is_current")).select("customer_id")  #valid_customers reads current customer dimension rows only (is_current = true)
    valid_products  = spark.read.table("helix_silver.products.dim_products") \
        .select("product_id")

    parsed = raw.withColumn(
        "order_ts_parsed",
        to_timestamp(col("order_ts"), "yyyy-MM-dd'T'HH:mm:ss")
    )

#Before writing to Silver, validate that: 
# Foreign keys exist in the dimension tables (no dangling customer_id)
# Amounts are positive,  Timestamps are parseable

    valid_flag = (
        col("order_id").isNotNull()
        & col("customer_id").isin(
            [r.customer_id for r in valid_customers.collect()]
        )
        & col("product_id").isin(
            [r.product_id for r in valid_products.collect()]
        )
        & (col("unit_price") > 0)
        & col("order_ts_parsed").isNotNull()
    )

    good   = parsed.filter(valid_flag)
    bad    = parsed.filter(~valid_flag)

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
        .mode("append")
        .saveAsTable("helix_silver.orders.fct_orders")
    )

#Rows that fail validation go to a quarantine table for investigation, not to the main fact table.

    if bad.count() > 0:
        (
            bad.withColumn("_quarantine_reason", lit("validation_failed"))
               .withColumn("_loaded_at", current_timestamp())
               .write.format("delta")
               .mode("append")
               .saveAsTable("helix_silver.orders.fct_orders_quarantine")
        )

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    build_fct_orders(spark)