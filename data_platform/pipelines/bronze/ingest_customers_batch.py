# Databricks notebook source
from databricks.connect import DatabricksSession


def main():
    spark = DatabricksSession.builder.getOrCreate()
    print("Lakeflow Connect pipeline — no application code needed here.")
    print("Configuration lives in databricks.bundle.yml under resources.pipelines.customers_lakeflow")


if __name__ == "__main__":
    main()