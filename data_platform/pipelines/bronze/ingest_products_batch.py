# Databricks notebook source
# Products bronze ingestion — creates a static product catalogue.
# No external source needed: products are a known reference dataset.
# Runs as a Databricks Job notebook task (not a DLT pipeline).
from pyspark.sql.functions import current_timestamp

# Create schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS helix_bronze.products")

# Static product catalogue — 25 representative ShopStream products
products_data = [
    ("P00001", "Wireless Headphones Pro",    "electronics", 89.99,  "SUP001"),
    ("P00002", "USB-C Charging Cable 2m",    "electronics", 12.99,  "SUP001"),
    ("P00003", "Bluetooth Speaker Mini",     "electronics", 49.99,  "SUP001"),
    ("P00004", "Laptop Stand Aluminium",     "electronics", 34.99,  "SUP002"),
    ("P00005", "Mechanical Keyboard TKL",    "electronics", 129.99, "SUP002"),
    ("P00006", "Running Shoes Lite",         "sports",      79.99,  "SUP003"),
    ("P00007", "Yoga Mat Premium 6mm",       "sports",      29.99,  "SUP003"),
    ("P00008", "Water Bottle 1L Steel",      "sports",      24.99,  "SUP003"),
    ("P00009", "Men T-Shirt Cotton",         "clothing",    19.99,  "SUP004"),
    ("P00010", "Women Hoodie Fleece",        "clothing",    44.99,  "SUP004"),
    ("P00011", "Denim Jeans Slim Fit",       "clothing",    59.99,  "SUP004"),
    ("P00012", "Face Moisturiser SPF50",     "beauty",      18.99,  "SUP005"),
    ("P00013", "Shampoo Argan Oil 300ml",    "beauty",      12.99,  "SUP005"),
    ("P00014", "Electric Toothbrush S2",     "beauty",      39.99,  "SUP005"),
    ("P00015", "Coffee Mug Ceramic 350ml",   "home",        14.99,  "SUP006"),
    ("P00016", "Scented Candle Lavender",    "home",        11.99,  "SUP006"),
    ("P00017", "Cutting Board Bamboo",       "home",        22.99,  "SUP006"),
    ("P00018", "Python for Data Science",    "books",       39.99,  "SUP007"),
    ("P00019", "The Lean Startup",           "books",       16.99,  "SUP007"),
    ("P00020", "Atomic Habits",              "books",       14.99,  "SUP007"),
    ("P00021", "Organic Granola 500g",       "food",         8.99,  "SUP008"),
    ("P00022", "Dark Chocolate 85% 100g",    "food",         4.99,  "SUP008"),
    ("P00023", "Green Tea Matcha 100g",      "food",        12.99,  "SUP008"),
    ("P00024", "Noise-Cancelling Earbuds",   "electronics", 99.99,  "SUP001"),
    ("P00025", "Smart Watch Basic",          "electronics", 149.99, "SUP002"),
]

df = (
    spark.createDataFrame(
        products_data,
        ["product_id", "product_name", "category", "unit_price", "supplier_id"],
    )
    .withColumn("_ingested_at", current_timestamp())
)

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("helix_bronze.products.products")
)

print(f"Loaded {df.count()} products into helix_bronze.products.products")