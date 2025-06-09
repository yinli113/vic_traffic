# Databricks notebook source
# MAGIC
# MAGIC %run ./build_silver
# MAGIC
# MAGIC

# COMMAND ----------

# File: build_silver_test.py

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()

class BuildSilverTest:
    """
    Tests the BuildSilver module.
    """
    def __init__(self):
        self.builder = BuildSilver()
        self.tables_to_check = [
            "traffic_silver_fact",
            "dim_region",
            "dim_site",
            "dim_time",
            "dim_detector"
        ]

    def drop_existing_tables(self):
        """
        Drops all Silver tables to ensure a clean test environment.
        """
        for table_name in self.tables_to_check:
            try:
                print(f"Dropping table '{table_name}' if exists...", end='')
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                print("Done")
            except Exception as e:
                print(f"Error dropping table '{table_name}': {e}")

    def run_build_process(self, bronze_table_name):
        """
        Calls the build_all method of BuildSilver.
        """
        try:
            print("Running BuildSilver process...")
            self.builder.build_all(bronze_table_name)
        except Exception as e:
            print(f"Error during build process: {e}")

    def validate_tables_created(self):
        """
        Validates that all expected Silver tables exist.
        """
        print("Validating that all expected tables are created...")
        for table_name in self.tables_to_check:
            try:
                _ = spark.table(table_name)
                print(f"Table '{table_name}' exists.")
            except AnalysisException:
                raise AssertionError(f"Table '{table_name}' was not created.")
        print("All expected tables exist.")

    def validate_non_empty_tables(self):
        """
        Validates that all tables are not empty.
        """
        print("Validating that tables contain data...")
        for table_name in self.tables_to_check:
            try:
                count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
                assert count > 0, f"Table '{table_name}' is empty."
                print(f"Table '{table_name}' has {count} rows.")
            except (AnalysisException, AssertionError) as e:
                raise AssertionError(f"Validation failed for table '{table_name}': {e}")
        print("All tables have data.")

    def run_all_tests(self, bronze_table_name):
        """
        Orchestrates all test steps.
        """
        self.drop_existing_tables()
        self.run_build_process(bronze_table_name)
        self.validate_tables_created()
        self.validate_non_empty_tables()
        print("All tests passed successfully.")

if __name__ == "__main__":
    tester = BuildSilverTest()
    bronze_table_name = "raw_traffic_2025_05_01"  # Example table name
    tester.run_all_tests(bronze_table_name)
