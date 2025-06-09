# Databricks notebook source
# MAGIC %run ./build_gold

# COMMAND ----------

# File: build_gold_test.py

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()

class BuildGoldTest:
    """
    Tests the BuildGold module.
    """
    def __init__(self):
        self.builder = BuildGold()
        self.tables_to_check = [
            "traffic_gold_region_hourly",
            "traffic_gold_detector_hourly",
            "traffic_gold_region_monthly",
            "traffic_gold_congestion_flags"
        ]

    def drop_existing_tables(self):
        """
        Drops all Gold tables to ensure a clean test environment.
        """
        for table_name in self.tables_to_check:
            try:
                print(f"Dropping table '{table_name}' if exists...", end='')
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")
                print("Done")
            except Exception as e:
                print(f"Error dropping table '{table_name}': {e}")

    def run_build_process(self):
        """
        Calls the build_all method of BuildGold.
        """
        try:
            print("Running BuildGold process...")
            self.builder.build_all()
        except Exception as e:
            print(f"Error during build process: {e}")

    def validate_table_created(self):
        """
        Validates that all expected Gold tables exist.
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

    def validate_expected_columns(self):
        """
        Validates that the required columns exist in each table.
        """
        print("Validating that tables contain expected columns...")
        expected_columns = {
            "traffic_gold_region_hourly": ["NM_REGION", "Date", "Time_Label", "Hourly_Volume"],
            "traffic_gold_detector_hourly": ["NM_REGION", "NB_SCATS_SITE", "NB_DETECTOR", "Date", "Time_Label", "Hourly_Volume"],
            "traffic_gold_region_monthly": ["NM_REGION", "Year", "Month", "Monthly_Volume"],
            "traffic_gold_congestion_flags": ["NM_REGION", "NB_SCATS_SITE", "NB_DETECTOR", "Date", "Time_Label", "Hourly_Volume", "Congestion_Flag", "Congestion_Level"]
        }

        for table_name, columns in expected_columns.items():
            try:
                df = spark.table(table_name)
                table_columns = set(df.columns)
                missing_cols = [col for col in columns if col not in table_columns]
                assert not missing_cols, f"Table '{table_name}' is missing columns: {missing_cols}"
                print(f"Table '{table_name}' has all expected columns.")
            except (AnalysisException, AssertionError) as e:
                raise AssertionError(f"Validation failed for table '{table_name}': {e}")
        print("All tables contain the expected columns.")

    def run_all_tests(self):
        """
        Orchestrates all test steps.
        """
        self.drop_existing_tables()
        self.run_build_process()
        self.validate_table_created()
        self.validate_non_empty_tables()
        self.validate_expected_columns()
        print("All Gold layer tests passed successfully!")

if __name__ == "__main__":
    tester = BuildGoldTest()
    tester.run_all_tests()
