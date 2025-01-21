from pyspark.sql import SparkSession
from pyspark.sql import Row

from typing import List
from json import loads, JSONDecodeError

from modules.rule_validation import RuleValidation
from modules.rule_exception import RuleException

from modules.utils.utils import *

import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
spark = SparkSession.builder.appName("DQ-Tool").getOrCreate()


def preprocess_json(raw_json: str) -> dict:
    # Check for improperly escaped backslashes
    try:
        # Attempt to load the JSON as-is
        return loads(raw_json)
    except JSONDecodeError:
        # Fix invalid backslashes by escaping them
        sanitized_json = raw_json.encode('unicode_escape').decode('utf-8')
        return loads(sanitized_json)
    

class BaseExpectation:
    """Base class for Expectation objects"""

    __slots__ = [
        "arguments",
        "table_key",
        "column",
        "rule_key",
        "rule_no",
        "gx_expectation_name",
        "gx_conditions",
        "gx_expectation",
        "validation",
        "store_exceptions",
        "element_count",
        "execution_key",
    ]

    def __init__(self, data: Row) -> None:
        """Initialize the BaseExpectation object
        :param data: Row object with the data from the Expectation table
        """
        raw_json = data.gx_arguments
        if data.gx_arguments.startswith('"') and data.gx_arguments.endswith('"'):
            raw_json = data.gx_arguments[1:-1]
        raw_json = raw_json.replace('""', '"')
        
        self.arguments: dict = preprocess_json(raw_json)

        self.table_key: List[str] = [key.strip() for key in data["table_key"].split(",")]
        self.column: str = data["column"]

        self.rule_key: int = data["rule_key"]
        self.rule_no: int = data["rule_no"]
        self.gx_expectation_name: str = data["gx_expectation"]
        self.gx_conditions: str = data["gx_conditions"]
        self.store_exceptions = True
        
        self.prepare_arguments()
        self.gx_expectation = my_import(data["gx_expectation"], self.arguments)

    def exception_raised(self, result: dict) -> bool:
        """Check if an exception was raised during the validation"""
        if not result["success"]:
            # Extract the exception message
            exception_info = result["exception_info"]
            key = next(iter(result["exception_info"]))  # Get the first (and only) key
            
            if key == "raised_exception":
                raised_exception = result["exception_info"][key]
            else:
                raised_exception = result["exception_info"][key].get("raised_exception")
                
            if raised_exception:
                exception_message = exception_info.get(key, {}).get("exception_message", "Unknown exception")
                # Truncate the exception message up to 'JVM stacktrace'
                truncate_index = exception_message.find('JVM stacktrace')
                if truncate_index != -1:
                    truncated_message = exception_message[:truncate_index]
                else:
                    truncated_message = exception_message
                print(f"\t- Exception raised: {truncated_message}")
                return True
        return False

    def prepare_arguments(self) -> None:
        """Prepare the arguments for the Expectation object"""
        if isinstance(self.gx_conditions, str):
            condition = {
                "condition_parser": "spark",
                "row_condition": self.gx_conditions,
            }
            print(f'[*] Expectation {self.gx_expectation_name} with condition: {condition["row_condition"]}')
            self.arguments.update(condition)

    def parse_validation(
        self, result: dict, element_count: int, validation_key: int, execution_key: int
    ) -> bool:
        """Parse the validation results and return the validation data and failed rows

        :param results: dictionary with the validation results
        :param element_count: integer with the number of elements validated (df.count())
        :param validation_key: integer with the validation ID
        :param execution_key: integer with the last execution key
        :return: if an error was raised or not
        """
        
        print(f"\t- Parsing validation for {self.gx_expectation_name} ({result['success']}) rule_key: {self.rule_key} - rule_no: {self.rule_no}")
        
        if self.gx_expectation_name != format_type(
            result["expectation_config"]["type"]
        ):
            logger.error(f"Expectation type mismatch, {self.gx_expectation_name} != {result['expectation_config']['type']}")
            raise ValueError("Expectation type mismatch")

        # Parse the validation result and return a dictionary with the validation data
        self.validation = RuleValidation(result, validation_key, self.rule_key, execution_key)
        
        self.element_count = element_count
        self.execution_key = execution_key

        return self.exception_raised(result)
            
    def _get_failed_rows(self, results: dict) -> List[dict]:
        """Get the failed rows from the validation results

        :param results: dictionary with the validation results
        :return: list of dictionaries with the failed rows
        """
        failed_rows = []
        if not results["success"]:
            for item in results["result"]["unexpected_index_list"]:
                exception_row_pk = "_".join(
                    str(item[key]) for key in self.table_key if key in item
                )
                unexpected_value = str(item.get(self.column, None))
                failed_rows.append(
                    RuleException.get(
                        self.validation.validation_key,
                        self.rule_key,
                        self.execution_key,
                        exception_row_pk,
                        unexpected_value,
                    )
                )
            print(f"\t- Failed rows: {len(failed_rows)}")
        return failed_rows


class UnexpectedRowsExpectation(BaseExpectation):

    __slots__ = [
        "arguments",
        "table_key",
        "column",
        "rule_key",
        "rule_no",
        "gx_expectation_name",
        "gx_conditions",
        "gx_expectation",
        "validation",
        "store_exceptions",
        "id",
        "element_count",
        "execution_key",
    ]

    def __init__(self, data) -> None:
        super().__init__(data)

    def parse_validation(
        self, results: dict, element_count: int, validation_key: int, execution_key: int
    ) -> None:
        exception_raised = super().parse_validation(
            results, element_count, validation_key, execution_key
        )

        if exception_raised: return
        
        null_count = 0
        failed_rows = []

        if not results["success"]:
            # If the number of failed rows is less than 200, get the failed rows
            if len(results["result"]["details"]["unexpected_rows"]) < 200:
                for row in results["result"]["details"]["unexpected_rows"]:
                    exception_row_pk = "_".join(
                        str(row[key]) for key in self.table_key if key in row
                    )
                    unexpected_value = str(row.get(self.column, None))

                    null_count = (
                        null_count + 1 if unexpected_value == "None" else null_count
                    )

                    failed_rows.append(
                        RuleException.get(
                            self.validation.validation_key,
                            self.rule_key,
                            execution_key,
                            exception_row_pk,
                            unexpected_value,
                        )
                    )
            # If number is >=200, get manually all the failed rows
            else:

                query = results["expectation_config"]["kwargs"]["unexpected_rows_query"]
                batch_id = results["expectation_config"]["kwargs"]["batch_id"].split("-")
                
                schema = batch_id[2]
                table = batch_id[4]
                
                query = query.replace("{batch}", f"{schema}.{table}")
                # Remove the * from the query and replace by table_key columns and column
                query = query.replace("*", ", ".join(self.table_key + [self.column]))

                # Execute the query and get the failed rows as a list of dictionaries
                unexpected_rows = spark.sql(query).collect()
                for row in unexpected_rows:
                    failed_rows.append(
                        RuleException.get(
                            self.validation.validation_key,
                            self.rule_key,
                            execution_key,
                            "_".join(str(row[key]) for key in self.table_key),
                            str(row[self.column]),
                        )
                    )
                
            print(f"\t- Failed rows: {len(failed_rows)}")
      
        exception_count = results["result"]["observed_value"]
        element_count = max(element_count, 1)  # Ensure element_count is at least 1 to prevent division by zero

        # Calculate exception_percent safely
        exception_percent = (exception_count / element_count) * 100 if element_count > 0 else 0.0
        # Calculate missing_percent safely
        missing_percent = (null_count / element_count) * 100 if element_count > 0 else 0.0

        # Update the validation data
        self.validation.add_exceptions(failed_rows)
        self.validation.update(
            {
                "element_count": element_count,
                "exception_count": exception_count,
                "exception_percent": exception_percent,
                "observed_value": results["result"]["observed_value"],
                "missing_count": null_count,
                "missing_percent": missing_percent,
                "exception_percent_total": 100 - exception_percent,
                "exception_percent_nonmissing": 100 - missing_percent,
            }
        )


class ColumnExpectation(BaseExpectation):

    __slots__ = [
        "arguments",
        "table_key",
        "column",
        "rule_key",
        "rule_no",
        "gx_expectation_name",
        "gx_conditions",
        "gx_expectation",
        "validation",
        "store_exceptions",
        "id",
        "element_count",
        "execution_key",
    ]

    def __init__(self, data) -> None:
        super().__init__(data)

    def prepare_arguments(self) -> None:
        super().prepare_arguments()

        if self.gx_expectation_name == "ExpectColumnValuesToBeInSet":
            # If value_set is a string (schema.table.column), query the database and get list of values
            if isinstance(self.arguments["value_set"], str):
                print(f"\t- Expectation {self.gx_expectation_name} with value_set: {self.arguments['value_set']}")
                schema, table, column = self.arguments["value_set"].split(".")
                values = spark.sql(
                    f"SELECT DISTINCT {column} FROM {schema}.{table}"
                ).collect()
                values_list = [row[column] for row in values]
                print(f"\t- Expectation {self.gx_expectation_name} with value_set count {len(values_list)}")
                self.arguments["value_set"] = values_list

    def parse_validation(
        self, results: dict, element_count: int, validation_key: int, execution_key: int
    ) -> None:
        exception_raised = super().parse_validation(
            results, element_count, validation_key, execution_key
        )

        if exception_raised: return
        self.validation.add_exceptions(self._get_failed_rows(results))


class ExpectColumnValuesToNotBeNull(BaseExpectation):

    __slots__ = [
        "arguments",
        "table_key",
        "column",
        "rule_key",
        "rule_no",
        "gx_expectation_name",
        "gx_conditions",
        "gx_expectation",
        "validation",
        "store_exceptions",
        "id",
        "element_count",
        "execution_key",
    ]

    def __init__(self, data) -> None:
        super().__init__(data)

    def prepare_arguments(self) -> None:
        super().prepare_arguments()

        if self.gx_expectation_name == "ExpectColumnValuesToNotBeNull":
            if "store_exceptions" in self.arguments:
                self.store_exceptions = self.arguments.pop("store_exceptions")

    def parse_validation(
        self, results: dict, element_count: int, validation_key: int, execution_key: int
    ) -> None:
        exception_raised = super().parse_validation(
            results, element_count, validation_key, execution_key
        )

        if exception_raised: return
        self.validation.update(
            {
                "missing_count": results["result"]["unexpected_count"],
                "missing_percent": results["result"]["unexpected_percent"],
                "exception_percent_total": results["result"]["unexpected_percent"],
                "exception_percent_nonmissing": 0,
            }
        )
        # If store_exceptions is True, get the failed rows
        failed_rows = []
        if self.store_exceptions:
            failed_rows = self._get_failed_rows(results)
        self.validation.add_exceptions(failed_rows)
