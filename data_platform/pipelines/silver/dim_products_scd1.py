from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

#SCD1

def build_dim_products(spark: SparkSession) -> None:
    source = spark.read.table("helix_bronze.products.products")

    target_table = "helix_silver.products.dim_products"

    if not spark.catalog.tableExists(target_table):
        (
            source.select(
                col("product_id"),
                col("product_name"),
                col("category"),
                col("unit_price"),
                col("supplier_id"),
                current_timestamp().alias("_last_updated"),
            )
            .write.format("delta").saveAsTable(target_table)
        )
        return

    target = DeltaTable.forName(spark, target_table)

    target.alias("t").merge(
        source.alias("s"),
        "t.product_id = s.product_id"
        ).whenMatchedUpdateAll(     #if a product with the same product_id exists, overwrite all its columns with the new values.
        ).whenNotMatchedInsertAll(  # if no matching product_id exists, insert the row.
        ).execute()

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    build_dim_products(spark)