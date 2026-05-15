# Databricks notebook source
# Batch ingestion — reads return CSV files from ADLS using Auto Loader.
# Runs as a Databricks Job notebook task (not a DLT pipeline).
# Re-runnable: overwrites the bronze table each time to avoid duplicates.
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

STORAGE_ACCOUNT = "helixdatalfqrcq"
SOURCE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/returns/"

RETURNS_SCHEMA = StructType([
    StructField("return_id",     StringType(), nullable=False),
    StructField("order_id",      StringType(), nullable=False),
    StructField("customer_id",   StringType(), nullable=False),
    StructField("product_id",    StringType(), nullable=False),
    StructField("return_date",   StringType(), nullable=True),
    StructField("reason_code",   StringType(), nullable=True),
    StructField("refund_amount", DoubleType(), nullable=True),
])

# Create schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS helix_bronze.returns")

df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(RETURNS_SCHEMA)
    .load(SOURCE_PATH)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("helix_bronze.returns.returns_raw")
)

print(f"Loaded {df.count()} returns into helix_bronze.returns.returns_raw")
