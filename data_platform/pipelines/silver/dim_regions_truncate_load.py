# Databricks notebook source
# Regions reference dimension — runs as a Databricks Job notebook task.
# Static reference data is embedded here — no external source needed.
# spark is injected automatically — do not call SparkSession.builder here.
from pyspark.sql.functions import current_timestamp

spark.sql("CREATE SCHEMA IF NOT EXISTS helix_silver.regions")

# Static region reference data (matches ShopStream's delivery zones)
regions_data = [
    ("nl-north",    "Netherlands", "North",    "NL"),
    ("nl-south",    "Netherlands", "South",    "NL"),
    ("nl-east",     "Netherlands", "East",     "NL"),
    ("nl-west",     "Netherlands", "West",     "NL"),
    ("nl-central",  "Netherlands", "Central",  "NL"),
    ("be-flanders", "Belgium",     "Flanders", "BE"),
    ("be-wallonia", "Belgium",     "Wallonia", "BE"),
    ("de-north",    "Germany",     "North",    "DE"),
    ("de-south",    "Germany",     "South",    "DE"),
]

df = (
    spark.createDataFrame(
        regions_data,
        ["region_id", "country", "region_name", "country_code"],
    )
    .withColumn("_loaded_at", current_timestamp())
)

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("helix_silver.regions.dim_regions")
)

print(f"dim_regions loaded: {df.count()} regions")