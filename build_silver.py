# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists dim_detector;
# MAGIC drop table if exists dim_time;
# MAGIC drop table if exists dim_site;
# MAGIC drop table if exists dim_region;
# MAGIC drop table if exists traffic_silver_fact

# COMMAND ----------

# File: build_silver.py
from pyspark.sql.functions import when, col, floor, format_string, lit

from pyspark.sql.functions import floor, col, format_string
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    expr, sha2, concat_ws, col, year, month, date_format, 
    dayofweek, when, monotonically_increasing_id, lit, floor, format_string
)
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()

class BuildSilver:
    """
    Handles transformation from Bronze to Silver, including Fact and Dimension tables.
    """
    def __init__(self, base_silver_dir="dbfs:/FileStore/traffic/silver"):
        self.base_silver_dir = base_silver_dir

    def get_bronze_data(self, table_name):
        try:
            df = spark.read.format("delta").table(table_name)
            print(f"Successfully loaded Bronze table: {table_name}")
            return df
        except AnalysisException as e:
            print(f"Error reading Bronze table '{table_name}': {e}")
            return None
        
    def clean_negative_values(self, df):
        """
        Sets all negative V00-V95 values to 0.
        """
        for i in range(96):
            col_name = f"V{i:02d}"
            df = df.withColumn(
                col_name,
                when(col(col_name) < 0, lit(0)).otherwise(col(col_name))
            )
        return df

    def add_time_label(self, df):
        """
        Adds a Time_Label column in 24-hour format (e.g. 00:15).
        """
        df = df.withColumn("Hour24", floor(col("Hour") / 4))
        df = df.withColumn("Minute", (col("Hour") % 4) * 15)
        df = df.withColumn(
            "Time_Label",
            format_string("%02d:%02d", col("Hour24"), col("Minute"))
        )
        return df.drop("Hour24", "Minute")
    
    def get_unpivoted_df(self, bronze_df):
        """
        Unpivots 96 interval columns and adds Time_Label.
        """
        from pyspark.sql.functions import to_date

        try:
            # Clean negative values in the Bronze V00-V95 columns
            bronze_df = self.clean_negative_values(bronze_df)

            stack_expr = ", ".join([f"'V{i:02d}', V{i:02d}" for i in range(96)])
            unpivoted_df = (
                bronze_df.selectExpr(
                    "NB_SCATS_SITE",
                    "QT_INTERVAL_COUNT as Date",
                    "NB_DETECTOR",
                    "NM_REGION",
                    f"stack(96, {stack_expr}) as (Hour_Label, Volume)"
                )
                .withColumn("Hour", expr("int(substring(Hour_Label, 2, 2))"))
            )

            # Clean negative values in the unpivoted 'Volume' column
            unpivoted_df = unpivoted_df.withColumn("Volume", when(col("Volume") < 0, lit(0)).otherwise(col("Volume")))

            unpivoted_df = self.add_time_label(unpivoted_df)
            unpivoted_df = unpivoted_df.withColumn("Date", to_date(col("Date"), "d/M/yyyy"))
            unpivoted_df = unpivoted_df.drop("Hour_Label")

            print("Successfully unpivoted Bronze DataFrame with Time_Label.")
            return unpivoted_df
        except AnalysisException as e:
            print(f"Error during unpivot: {e}")
            return None

    def create_silver_fact_table(self, unpivoted_df):
        """
        Creates the Silver Fact table, overwriting existing data and allowing schema changes.
        """
        try:
            fact_df = unpivoted_df.withColumn(
                "Fact_ID",
                sha2(concat_ws("-", "NB_SCATS_SITE", "Date", "NB_DETECTOR", "Hour"), 256)
            )
            (fact_df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable("traffic_silver_fact"))
            print("Silver Fact table created.")
            return "traffic_silver_fact"
        except AnalysisException as e:
            print(f"Error writing Silver Fact table: {e}")
            return None


    def create_dim_region(self, bronze_df):
        """
        Creates Region Dimension table.
        """
        try:
            dim_region_df = bronze_df.select("NM_REGION").distinct()
            dim_region_df.write.format("delta").mode("overwrite").saveAsTable("dim_region")
            print("Region Dimension table created.")
        except AnalysisException as e:
            print(f"Error creating Region Dimension: {e}")

    def create_dim_site(self, bronze_df):
        """
        Creates Site Dimension table.
        """
        try:
            dim_site_df = bronze_df.select("NB_SCATS_SITE", "NM_REGION").distinct()
            dim_site_df.write.format("delta").mode("overwrite").saveAsTable("dim_site")
            print("Site Dimension table created.")
        except AnalysisException as e:
            print(f"Error creating Site Dimension: {e}")

    def create_dim_time(self, fact_table_name):
        """
        Creates Time Dimension table from Fact table.
        """
        try:
            fact_df = spark.read.format("delta").table(fact_table_name)
            time_dim_df = (
                fact_df.select(col("Date"), col("Hour"))
                .distinct()
                .withColumn("Year", year(col("Date")))
                .withColumn("Month", month(col("Date")))
                .withColumn("DayOfWeek", date_format(col("Date"), "EEEE"))
                .withColumn("WeekdayFlag", when(dayofweek(col("Date")).between(2, 6), True).otherwise(False))
            )
            time_dim_df.write.format("delta").mode("overwrite").saveAsTable("dim_time")
            print("Time Dimension table created.")
        except AnalysisException as e:
            print(f"Error creating Time Dimension: {e}")

    def create_dim_detector(self, fact_table_name):
        """
        Creates Detector Dimension table from Fact table.
        """
        try:
            fact_df = spark.read.format("delta").table(fact_table_name)
            detector_dim_df = (
                fact_df.select("NB_SCATS_SITE", "NB_DETECTOR")
                .distinct()
                .withColumn("Detector_ID", monotonically_increasing_id())
                .withColumn("Lane_Type", lit(None).cast("string"))
                .withColumn("Movement_Direction", lit(None).cast("string"))
                .withColumn("Description", lit(None).cast("string"))
            )
            detector_dim_df.write.format("delta").mode("overwrite").saveAsTable("dim_detector")
            print("Detector Dimension table created.")
        except AnalysisException as e:
            print(f"Error creating Detector Dimension: {e}")

    def build_all(self, bronze_table_name):
        bronze_df = self.get_bronze_data(bronze_table_name)
        if bronze_df is None:
            print("Aborting build process: Bronze data not loaded.")
            return

        self.create_dim_region(bronze_df)
        self.create_dim_site(bronze_df)

        unpivoted_df = self.get_unpivoted_df(bronze_df)
        if unpivoted_df is None:
            print("Aborting build process: Unpivot failed.")
            return

        fact_table_name = self.create_silver_fact_table(unpivoted_df)
        if fact_table_name is None:
            print("Aborting build process: Fact table creation failed.")
            return

        self.create_dim_time(fact_table_name)
        self.create_dim_detector(fact_table_name)

        print("✅ Silver build process completed successfully.")
if __name__ == "__main__":
    builder = BuildSilver()
    bronze_table_name = "raw_traffic_2025_05_01"
    fact_table_name = "traffic_silver_fact"  # Example table name
    builder.build_all(bronze_table_name)