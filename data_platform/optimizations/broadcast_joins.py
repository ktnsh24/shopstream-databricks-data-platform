"""Broadcast join utilities for Helix Gold pipelines.

Why broadcast joins?
--------------------
A regular Spark join shuffles BOTH sides of the join across the network —
all executors exchange data so every executor can match keys.
A broadcast join ships the SMALL side to EVERY executor once, then each
executor joins locally. No shuffle. No network bottleneck.

DE parallel: a broadcast join is like photocopying the products lookup table
and taping a copy to every sorting desk in the depot, so each desk can look
up product details without calling a central desk every time.

When to use:
- Small side < AUTO_BROADCAST_JOIN_THRESHOLD_MB (default 10 MB in .env)
- Databricks AQE (Adaptive Query Execution) will often do this automatically,
  but explicit F.broadcast() is safer for known-small reference tables.

Reference data files (static, versioned in git):
- data/reference/regions.csv           (~9 rows)
- data/reference/product_categories.csv (~22 rows)

These are the "small side" of joins in the Gold pipelines.
"""
from pathlib import Path

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

# Path to reference data — relative to the repo root.
# Databricks runtime: use DBFS or ADLS path; locally: resolve from __file__.
_REPO_ROOT = Path(__file__).parent.parent.parent
REFERENCE_DIR = _REPO_ROOT / "data" / "reference"

# ---------------------------------------------------------------------------
# Schemas for reference files
# ---------------------------------------------------------------------------
REGION_SCHEMA = StructType(
    [
        StructField("region_code", StringType(), nullable=False),
        StructField("region_name", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("timezone", StringType(), nullable=True),
    ]
)

CATEGORY_SCHEMA = StructType(
    [
        StructField("category", StringType(), nullable=False),
        StructField("sub_category", StringType(), nullable=False),
        StructField("display_name", StringType(), nullable=True),
        StructField("is_seasonal", StringType(), nullable=True),  # "true"/"false" in CSV
        StructField("margin_band", StringType(), nullable=True),  # high / medium / low
    ]
)


# ---------------------------------------------------------------------------
# Loaders — return broadcast DataFrames
# ---------------------------------------------------------------------------

def load_regions(spark: SparkSession, reference_path: str | None = None) -> DataFrame:
    """Load regions reference CSV and broadcast it.

    Returns a broadcast DataFrame — pass directly into .join() calls.
    The broadcast hint tells Spark to ship this to every executor instead
    of shuffling. Safe because regions.csv is always < 1 KB.

    Usage:
        regions = load_regions(spark)
        result = orders.join(regions, on="region", how="left")
    """
    path = reference_path or str(REFERENCE_DIR / "regions.csv")
    logger.info("Loading regions reference from %s", path)

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(REGION_SCHEMA)
        .load(path)
        .withColumn("is_seasonal", F.lit(None))  # not in regions schema, remove
        .drop("is_seasonal")
    )

    row_count = df.count()
    logger.info("Loaded %s region rows. Broadcasting.", row_count)

    return F.broadcast(df)


def load_product_categories(spark: SparkSession, reference_path: str | None = None) -> DataFrame:
    """Load product categories reference CSV and broadcast it.

    Returns a broadcast DataFrame ready for .join() on (category, sub_category).

    Usage:
        categories = load_product_categories(spark)
        result = products.join(
            categories,
            on=["category", "sub_category"],
            how="left",
        )
    """
    path = reference_path or str(REFERENCE_DIR / "product_categories.csv")
    logger.info("Loading product categories reference from %s", path)

    df = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(CATEGORY_SCHEMA)
        .load(path)
        .withColumn("is_seasonal", F.col("is_seasonal").cast(BooleanType()))
    )

    row_count = df.count()
    logger.info("Loaded %s category rows. Broadcasting.", row_count)

    return F.broadcast(df)


# ---------------------------------------------------------------------------
# AQE config helper
# ---------------------------------------------------------------------------

def configure_aqe(spark: SparkSession, broadcast_threshold_mb: int = 10) -> None:
    """Enable Adaptive Query Execution and set the broadcast threshold.

    AQE lets Databricks decide at runtime whether to broadcast a join partner
    based on actual partition sizes — not just the schema estimate.
    This is the safety net for joins where the small side isn't a static file.

    broadcast_threshold_mb: tables smaller than this are auto-broadcast.
    Controlled by AUTO_BROADCAST_JOIN_THRESHOLD_MB in .env.

    DE parallel: AQE is like a smart sorting desk that switches from
    manual lookups to photocopied sheets once it realises the lookup
    table is small enough to copy.
    """
    threshold_bytes = broadcast_threshold_mb * 1024 * 1024
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(threshold_bytes))

    logger.info(
        "AQE enabled. Auto-broadcast threshold=%s MB (%s bytes)",
        broadcast_threshold_mb,
        threshold_bytes,
    )


# ---------------------------------------------------------------------------
# Example: how to use in a Gold pipeline
# ---------------------------------------------------------------------------

def example_enriched_revenue(spark: SparkSession, revenue_df: DataFrame) -> DataFrame:
    """Example: join revenue_daily with regions reference using explicit broadcast.

    This is how Gold pipelines use these utilities:

        from data_platform.optimizations.broadcast_joins import (
            load_regions,
            load_product_categories,
            configure_aqe,
        )

        configure_aqe(spark, broadcast_threshold_mb=10)
        regions = load_regions(spark)
        result = revenue_df.join(regions, on="region", how="left")

    The F.broadcast() wrapper on regions tells the Spark query planner:
    "don't shuffle this — send a copy to every executor."
    """
    configure_aqe(spark)
    regions = load_regions(spark)

    return (
        revenue_df
        .join(regions, on=revenue_df["region"] == regions["region_code"], how="left")
        .drop("region_code")  # avoid duplicate column after join
    )
