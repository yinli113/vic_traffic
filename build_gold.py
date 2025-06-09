# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists traffic_gold_region_hourly;
# MAGIC drop table if exists traffic_gold_detector_hourly;
# MAGIC drop table if exists traffic_gold_region_monthly;
# MAGIC drop table if exists traffic_gold_congestion_flags

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as sum_, year, month, col, when
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()

class BuildGold:
    """
    Handles building the Gold Layer from the Silver Fact table.
    """
    def __init__(self, fact_table_name="traffic_silver_fact"):
        self.fact_table_name = fact_table_name

    def load_fact_table(self):
        """
        Loads the Silver Fact Table as a DataFrame.
        """
        try:
            fact_df = spark.read.format("delta").table(self.fact_table_name)
            print(f"Successfully loaded fact table: {self.fact_table_name}")
            return fact_df
        except AnalysisException as e:
            print(f"Error loading fact table '{self.fact_table_name}': {e}")
            return None

    def create_region_hourly_volume(self, fact_df):
        """
        Creates Region-Level Hourly Volume Gold Table.
        """
        try:
            region_hourly = (
                fact_df
                .groupBy("NM_REGION", "Date", "Time_Label")
                .agg(sum_("Volume").alias("Hourly_Volume"))
            )
            (region_hourly.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .saveAsTable("traffic_gold_region_hourly"))
            print("Region-Level Hourly Volume table created.")
        except AnalysisException as e:
            print(f"Error creating Region-Level Hourly Volume table: {e}")

    def create_detector_hourly_volume(self, fact_df):
        """
        Creates Detector-Level Hourly Volume Gold Table with NM_REGION included.
        """
        try:
            detector_hourly = (
                fact_df
                .groupBy("NM_REGION", "NB_SCATS_SITE", "NB_DETECTOR", "Date", "Time_Label")
                .agg(sum_("Volume").alias("Hourly_Volume"))
            )
            (detector_hourly.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .saveAsTable("traffic_gold_detector_hourly"))
            print("Detector-Level Hourly Volume table created.")
        except AnalysisException as e:
            print(f"Error creating Detector-Level Hourly Volume table: {e}")

    def create_region_monthly_volume(self, fact_df):
        """
        Creates Region-Level Monthly Volume Gold Table.
        """
        try:
            region_monthly = (
                fact_df
                .withColumn("Year", year(col("Date")))
                .withColumn("Month", month(col("Date")))
                .groupBy("NM_REGION", "Year", "Month")
                .agg(sum_("Volume").alias("Monthly_Volume"))
            )
            (region_monthly.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .saveAsTable("traffic_gold_region_monthly"))
            print("Region-Level Monthly Volume table created.")
        except AnalysisException as e:
            print(f"Error creating Region-Level Monthly Volume table: {e}")

    def create_congestion_flags(self, fact_df):
        """
        Creates Congestion Flags Gold Table at Detector-Level with NM_REGION included.
        """
        try:
            detector_hourly = (
                fact_df
                .groupBy("NM_REGION", "NB_SCATS_SITE", "NB_DETECTOR", "Date", "Time_Label")
                .agg(sum_("Volume").alias("Hourly_Volume"))
            )
            congestion_flags = (
                detector_hourly
                .withColumn("Congestion_Flag", when(col("Hourly_Volume") > 50, True).otherwise(False))
                .withColumn(
                    "Congestion_Level",
                    when(col("Hourly_Volume") > 100, "High")
                    .when(col("Hourly_Volume") > 75, "Medium")
                    .when(col("Hourly_Volume") > 50, "Low")
                    .otherwise("Normal")
                )
            )
            (congestion_flags.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .saveAsTable("traffic_gold_congestion_flags"))
            print("Congestion Flags table created.")
        except AnalysisException as e:
            print(f"Error creating Congestion Flags table: {e}")

    def build_all(self):
        """
        Orchestrates the creation of all Gold tables.
        """
        fact_df = self.load_fact_table()
        if fact_df is None:
            print("Aborting build process: Fact table could not be loaded.")
            return

        self.create_region_hourly_volume(fact_df)
        self.create_detector_hourly_volume(fact_df)
        self.create_region_monthly_volume(fact_df)
        self.create_congestion_flags(fact_df)

        print("✅ Gold tables created successfully!")

if __name__ == "__main__":
    builder = BuildGold()
    builder.build_all()
