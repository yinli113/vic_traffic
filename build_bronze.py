# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS raw_traffic_2025_05_01

# COMMAND ----------

# File: build_bronze.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first
import datetime

spark = SparkSession.builder.getOrCreate()

class BuildBronze:
    """
    Handles ingestion of raw traffic data from CSV to Delta table.
    """
    def __init__(self, base_data_dir="/FileStore/raw_traffic"):
        self.base_data_dir = base_data_dir

    def get_raw_data(self, filepath):
        """
        Reads raw CSV data from the given filepath.
        """
        data = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(filepath)
        )
        print(f"Data loaded from: {filepath}")
        return data
    
    def save_raw_data(self, data):
        """
        Saves DataFrame to Delta table using the date in 'QT_INTERVAL_COUNT'.
        """
        if data is None or data.head(1) == []:
            print("No data to save.")
            return

        date_value = data.select(first(col("QT_INTERVAL_COUNT"))).take(1)[0][0]
        date_obj = datetime.datetime.strptime(date_value, "%d/%m/%Y")
        table_name = f"raw_traffic_{date_obj.strftime('%Y_%m_%d')}"

        # Append instead of overwrite
        data.write.format("delta").mode("overwrite").option("replaceWhere", f"Date='{date_value}'").saveAsTable(table_name)
        
        print(f"Data successfully write to Delta table: {table_name}")
        return f"{table_name}"

    def build(self, filepath):
        """
        Orchestrates the Bronze build process: read and save.
        """
        data = self.get_raw_data(filepath)
        self.save_raw_data(data)

if __name__ == "__main__":
    filepath = "/FileStore/raw_traffic/VSDATA_20250501.csv"  # Example file
    builder = BuildBronze()
    builder.build(filepath)
