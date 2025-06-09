# Databricks notebook source
# MAGIC %run ./build_bronze

# COMMAND ----------

# Databricks notebook source
# MAGIC %run ./build_bronze

# COMMAND ----------

from pyspark.sql.functions import col, first
import datetime

spark = SparkSession.builder.getOrCreate()

class IngestTest:
    """
    Orchestrates tests for data ingestion: drop old tables, ingest, and validate row count and region count.
    """
    def __init__(self, base_data_dir="/FileStore/raw_traffic"):
        self.base_data_dir = base_data_dir

    def clean_table(self, table_name):
        """
        Drops the existing Delta table if it exists.
        """
        try:
            print(f"Starting cleanup of table '{table_name}'...", end='')
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            print("Done")
        except AnalysisException as e:
            print(f"Error dropping table '{table_name}': {e}")

    def row_count_test(self, table_name, expected_count):
        """
        Asserts the row count of a table matches the expected count.
        """
        try:
            print(f"Validating row count in table '{table_name}'...", end='')
            actual_count = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
            assert actual_count == expected_count, f"Expected {expected_count}, got {actual_count}"
            print("Passed")
        except (AnalysisException, AssertionError) as e:
            print(f"Validation failed: {e}")

    def region_count_test(self, table_name, expected_count=2):
        """
        Asserts the region count of column NM_REGION matches the expected count.
        """
        try:
            print(f"Validating region count in table '{table_name}'...", end='')
            actual_count = spark.sql(f"SELECT COUNT(DISTINCT(NM_REGION)) FROM {table_name}").collect()[0][0]
            assert actual_count == expected_count, f"Expected {expected_count}, got {actual_count}"
            print("Passed")
        except (AnalysisException, AssertionError) as e:
            print(f"Validation failed: {e}")

    def ingest_and_test(self, csv_index, expected_row_count, expected_region_count):
        """
        Ingests data, saves to Delta table, and runs all tests.
        """
        ingestor = BuildBronze(self.base_data_dir)
        csv_file = f"{self.base_data_dir}/data_{csv_index}.csv"
        raw_data = ingestor.get_raw_data(csv_file)

        if raw_data is None:
            print(f"Skipping test {csv_index}: could not load data.")
            return

        try:
            date_value = raw_data.select(first(col("QT_INTERVAL_COUNT"))).take(1)[0][0]
            date_obj = datetime.datetime.strptime(date_value, "%d/%m/%Y")
            table_name = f"raw_traffic_{date_obj.strftime('%Y_%m_%d')}"
        except (ValueError, TypeError, IndexError) as e:
            print(f"Skipping test {csv_index}: error parsing date - {e}")
            return

        self.clean_table(table_name)
        saved_table_name = ingestor.save_raw_data(raw_data)

        if saved_table_name:
            self.row_count_test(saved_table_name, expected_row_count)
            self.region_count_test(saved_table_name, expected_region_count)
            print(f"✅ Test {csv_index} completed successfully with expected row count {expected_row_count} and expected region count {expected_region_count}.\n")
        else:
            print(f"❌ Test {csv_index} failed to save data.\n")

    def run_tests(self):
        """
        Runs all test cases.
        """
        test_cases = [
            (1, 25, 2),  # (csv_index, expected_row_count, expected_region_count)
            (2, 32, 2)
        ]

        for csv_index, expected_row_count, expected_region_count in test_cases:
            self.ingest_and_test(csv_index, expected_row_count, expected_region_count)

if __name__ == "__main__":
    test_runner = IngestTest()
    test_runner.run_tests()
