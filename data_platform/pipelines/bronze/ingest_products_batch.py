# Databricks notebook source
from databricks.connect import DatabricksSession

def main():
    spark = DatabricksSession.builder.getOrCreate()
    print("Lakeflow Connect pipeline for products.")
    print("Configuration in databricks.bundle.yml under resources.pipelines.products_lakeflow")

if __name__ == "__main__":
    main()