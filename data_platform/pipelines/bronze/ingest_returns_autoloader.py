from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
#Define the schema
RETURNS_SCHEMA = StructType([
    StructField("return_id",    StringType(),    nullable=False),
    StructField("order_id",     StringType(),    nullable=False),
    StructField("customer_id",  StringType(),    nullable=False),
    StructField("product_id",   StringType(),    nullable=False),
    StructField("return_date",  StringType(),    nullable=True),
    StructField("reason_code",  StringType(),    nullable=True),
    StructField("refund_amount",DoubleType(),    nullable=True),
])

#Define paths
STORAGE_ACCOUNT = "helixdata001"
SOURCE_PATH  = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/returns/"
TARGET_PATH  = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/returns/returns_raw/"
CHECKPOINT_PATH = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/returns/"

#ingestion function
def ingest_returns(spark: SparkSession) -> None:
    stream = (
        spark.readStream
        .format("cloudFiles")  #cloudFiles is a special Databricks source that does incremental file discovery.
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "_schema")
        .option("header", "true")
        .schema(RETURNS_SCHEMA)
        .load(SOURCE_PATH)
    )

    stream_with_meta = stream.select(
        "*",
        current_timestamp().alias("_ingested_at"),
        input_file_name().alias("_source_file"),
    )

    (
        stream_with_meta.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable("helix_bronze.returns.returns_raw")
    )

#Add the entry point
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate() # returns the existing session if one is running (Databricks always has one), or creates a new one if running locally.
    ingest_returns(spark)



    