from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, sha2, concat_ws
)
from delta.tables import DeltaTable

#define function

def build_dim_customers(spark: SparkSession) -> None:
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
        initial = source_with_hash.select(
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
        initial.write.format("delta").saveAsTable(target_table)
        return
    
    target = DeltaTable.forName(spark, target_table)

    target.alias("t").merge(
        source_with_hash.alias("s"),
        "t.customer_id = s.customer_id AND t.is_current = true"
    ).whenMatchedUpdate(
        condition = "t.row_hash != s.row_hash",
        set = {
            "is_current": "false",
            "valid_to":   "current_timestamp()",
        }
    ).execute()

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

    if __name__ == "__main__":
        spark = SparkSession.builder.getOrCreate()
        build_dim_customers(spark)
