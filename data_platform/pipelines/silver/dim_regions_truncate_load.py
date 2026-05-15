from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def load_dim_regions(spark: SparkSession) -> None:
    source = spark.read.table("helix_bronze.regions.regions")

    enriched = source.withColumn("_loaded_at", current_timestamp())

    (
        enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("helix_silver.regions.dim_regions")
    )

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    load_dim_regions(spark)