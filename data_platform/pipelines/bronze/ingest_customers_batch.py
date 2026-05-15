# Databricks notebook source
# Batch ingestion — reads customer CSV files from ADLS.
# Runs as a Databricks Job notebook task (not a DLT pipeline).
# Re-runnable: overwrites the bronze table each time to avoid duplicates.
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

STORAGE_ACCOUNT = "helixdatalfqrcq"
SOURCE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/customers/"

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id",  StringType(), nullable=False),
    StructField("first_name",   StringType(), nullable=True),
    StructField("last_name",    StringType(), nullable=True),
    StructField("email",        StringType(), nullable=True),
    StructField("phone",        StringType(), nullable=True),
    StructField("city",         StringType(), nullable=True),
    StructField("country",      StringType(), nullable=True),
    StructField("region",       StringType(), nullable=True),
    StructField("segment",      StringType(), nullable=True),
    StructField("signup_date",  StringType(), nullable=True),
])

# Create schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS helix_bronze.customers")

df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(CUSTOMER_SCHEMA)
    .load(SOURCE_PATH)
    .withColumn("_ingested_at", current_timestamp())
)

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("helix_bronze.customers.customers")
)

print(f"Loaded {df.count()} customers into helix_bronze.customers.customers")