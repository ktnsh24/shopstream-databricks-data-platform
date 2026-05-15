# Databricks notebook source
# DLT streaming table — reads order events from Azure Event Hub (Kafka protocol)
# DLT manages checkpoints automatically. Do NOT define checkpoint paths here.
import dlt
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

ORDER_SCHEMA = StructType([
    StructField("order_id",    StringType(),  nullable=False),
    StructField("customer_id", StringType(),  nullable=False),
    StructField("product_id",  StringType(),  nullable=False),
    StructField("quantity",    IntegerType(), nullable=True),
    StructField("unit_price",  DoubleType(),  nullable=True),
    StructField("order_ts",    StringType(),  nullable=True),
])

NAMESPACE     = "helix-events-lfqrcq"
EVENT_HUB     = "orders"
CONSUMER_GROUP = "databricks-streaming"


def _kafka_options() -> dict:
    # dbutils is injected automatically by Databricks — never import it
    conn = dbutils.secrets.get(scope="helix", key="event-hub-connection-string")
    sasl = (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{conn}";'
    )
    return {
        "kafka.bootstrap.servers": f"{NAMESPACE}.servicebus.windows.net:9093",
        "subscribe":               EVENT_HUB,
        "kafka.group.id":          CONSUMER_GROUP,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism":    "PLAIN",
        "kafka.sasl.jaas.config":  sasl,
        "startingOffsets":         "earliest",
        "failOnDataLoss":          "false",
    }


@dlt.table(
    name="orders_raw",
    comment="Raw order events from Azure Event Hub. One row per Kafka message.",
)
def orders_raw():
    # spark is injected by DLT — never call SparkSession.builder here
    return (
        spark.readStream
        .format("kafka")
        .options(**_kafka_options())
        .load()
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