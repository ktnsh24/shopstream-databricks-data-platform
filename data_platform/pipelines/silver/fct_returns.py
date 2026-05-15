from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_timestamp, lit

def build_fct_returns(spark: SparkSession) -> None:
    raw     = spark.read.table("helix_bronze.returns.returns_raw")
    orders  = spark.read.table("helix_silver.orders.fct_orders").select("order_id")

    parsed = raw.withColumn("return_date_parsed", to_date(col("return_date"), "yyyy-MM-dd"))

    valid_flag = (
        col("return_id").isNotNull()
        & col("order_id").isin([r.order_id for r in orders.collect()])
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
        .write.format("delta").mode("append")
        .saveAsTable("helix_silver.returns.fct_returns")
    )

    if bad.count() > 0:
        (
            bad.withColumn("_quarantine_reason", lit("validation_failed"))
               .withColumn("_loaded_at", current_timestamp())
               .write.format("delta").mode("append")
               .saveAsTable("helix_silver.returns.fct_returns_quarantine")
        )

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    build_fct_returns(spark)