# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
# dbutils is injected automatically in Databricks — do not import it

#Define schema and config 

ORDER_SCHEMA = StructType([
    StructField("order_id",     StringType(),  nullable=False),
    StructField("customer_id",  StringType(),  nullable=False),
    StructField("product_id",   StringType(),  nullable=False),
    StructField("quantity",     IntegerType(), nullable=True),
    StructField("unit_price",   DoubleType(),  nullable=True),
    StructField("order_ts",     StringType(),  nullable=True),
])

STORAGE_ACCOUNT  = "helixdatalfqrcq"
TARGET_PATH      = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/orders/orders_raw/"
CHECKPOINT_PATH  = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/orders/"
CONSUMER_GROUP   = "databricks-streaming"
NAMESPACE        = "helix-events-abc123"
EVENT_HUB        = "orders"

# Read the connection string from Databricks secrets
def get_kafka_options(spark: SparkSession) -> dict:
    connection_string = dbutils.secrets.get(scope="helix", key="event-hub-connection-string")
    sasl_config = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{connection_string}";'
    )
    return {
        "kafka.bootstrap.servers":        f"{NAMESPACE}.servicebus.windows.net:9093",   # the Kafka broker address, For Event Hubs
        "subscribe":                       EVENT_HUB,
        "kafka.group.id":                  CONSUMER_GROUP,
        "kafka.security.protocol":         "SASL_SSL",
        "kafka.sasl.mechanism":            "PLAIN",
        "kafka.sasl.jaas.config":          sasl_config,
        "startingOffsets":                 "latest",
        "failOnDataLoss":                  "false",
    }

#the streaming function

def ingest_orders(spark: SparkSession) -> None:
    kafka_options = get_kafka_options(spark)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )
    
    parsed = (
        raw_stream
        .select(
            col("value").cast("string").alias("raw_payload"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("event_time"),
        )
        .withWatermark("event_time", "10 minutes")
        .withColumn("order", from_json(col("raw_payload"), ORDER_SCHEMA))
        .select(
            "order.*",
            "raw_payload",
            "kafka_partition",
            "kafka_offset",
            "event_time",
            current_timestamp().alias("_ingested_at"),
        )
    )

    (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .toTable("helix_bronze.orders.orders_raw")
    )

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    query = ingest_orders(spark)
    query.awaitTermination()