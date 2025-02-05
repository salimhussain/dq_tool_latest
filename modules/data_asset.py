from modules.expectation_manager import ExpectationManager
from pyspark.sql import SparkSession

import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
spark = SparkSession.builder.appName("DQ-Tool").getOrCreate()

# Maximum number of rows to load from a table
MAX_TABLE_ROWS = 1000

class DataAsset:

    __slots__ = [
        "catalog",
        "schema",
        "table",
        "data_source",
        "dataframe",
        "element_count",
        "expectation_manager",
        "num_rules",
    ]

    def __init__(self, catalog: str, schema: str, table: str,data_source: str) -> None:

        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.data_source = data_source

        self.dataframe = self._load_dataframe(data_source)
        self.element_count = self.dataframe.count()
        self.expectation_manager = ExpectationManager(self.data_source)

        self.num_rules = 0

    def get_expectation(self, data) -> None:
        expectation, enabled = self.expectation_manager.get_expectation(data)
        if enabled:
            self.num_rules += 1
        return expectation, enabled
    
    def add_expectation(self, expectation) -> None:
        self.expectation_manager.add_expectation(expectation)
    
    def save_expectation_suite(self) -> None:
        self.expectation_manager.save_expectation_suite()

    def parse_results(
        self, results: dict, vaLidation_key: int, execution_key: int
    ) -> None:
        self.expectation_manager.parse_results(
            results, self.element_count, vaLidation_key, execution_key
        )

    def _load_dataframe(self, data_source):
        #debug print
        print(f"Data source: {data_source}")
        try:
            if data_source == "Hive":
                df = spark.read.table(f"{self.schema}.{self.table}")
            elif data_source == "DeltaTable":
                df = spark.read.table(f"{self.catalog}.{self.schema}.{self.table}")
            else:
                raise ValueError(f"Unsupported data source: {data_source}")
            return df
        except Exception as e:
            logger.error(f"Error loading DataFrame: {e}")
            return None
