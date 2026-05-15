# Databricks notebook source
# SCD2 customer dimension — runs as a Databricks Job notebook task.
# spark is injected automatically — do not call SparkSession.builder here.
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws
from delta.tables import DeltaTable

spark.sql("CREATE SCHEMA IF NOT EXISTS helix_silver.customers")

source = spark.read.table("helix_bronze.customers.customers")

source_with_hash = source.withColumn(
    "row_hash",
    sha2(concat_ws("|",
        col("email"),
        col("city"),
        col("country"),
        col("phone"),
    ), 256)
)

target_table = "helix_silver.customers.dim_customers"

if not spark.catalog.tableExists(target_table):
    (
        source_with_hash.select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            col("city"),
            col("country"),
            col("phone"),
            col("row_hash"),
            lit(True).alias("is_current"),
            current_timestamp().alias("valid_from"),
            lit("9999-12-31").cast("timestamp").alias("valid_to"),
        )
        .write.format("delta").saveAsTable(target_table)
    )
else:
    target = DeltaTable.forName(spark, target_table)

    # Expire changed rows
    target.alias("t").merge(
        source_with_hash.alias("s"),
        "t.customer_id = s.customer_id AND t.is_current = true"
    ).whenMatchedUpdate(
        condition="t.row_hash != s.row_hash",
        set={"is_current": "false", "valid_to": "current_timestamp()"},
    ).execute()

    # Insert new / changed rows
    new_rows = (
        source_with_hash
        .join(
            spark.read.table(target_table).filter(col("is_current")),
            on="customer_id",
            how="left_anti",
        )
        .select(
            col("customer_id"), col("first_name"), col("last_name"),
            col("email"), col("city"), col("country"), col("phone"),
            col("row_hash"),
            lit(True).alias("is_current"),
            current_timestamp().alias("valid_from"),
            lit("9999-12-31").cast("timestamp").alias("valid_to"),
        )
    )
    new_rows.write.format("delta").mode("append").saveAsTable(target_table)

print(f"dim_customers updated: {spark.table(target_table).count()} total rows")
