from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit, coalesce, concat_ws, row_number
from pyspark.sql.window import Window

from typing import Dict, List

from schemas.data_model.dq_exceptions_schema import DQ_EXCEPTIONS_SCHEMA
from schemas.data_model.dq_validations_schema import DQ_VALIDATIONS_SCHEMA

import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
spark = SparkSession.builder.appName("DQ-Tool").getOrCreate()


def align_to_schema(
    df: DataFrame, schema: StructType = DQ_EXCEPTIONS_SCHEMA
) -> DataFrame:
    """Align a DataFrame to the specified schema by adding missing columns with null values and,
    reordering the columns to match the schema.

    :param df: The DataFrame to align.
    :param schema: The schema to align to.
    """
    columns = df.columns
    for field in schema.fields:
        if field.name not in columns:
            print(f"\t- Adding missing column {field.name} to DataFrame")
            df = df.withColumn(
                field.name, lit(None).cast(field.dataType)
            )  # Add missing column
        # Cast the column to the correct type if doesn't match the schema
        if field.dataType != df.select(col(field.name)).schema[field.name].dataType:
            print(f"\t- Casting column {field.name} to {field.dataType}")
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df.select([field.name for field in schema.fields])  # Reorder columns


class DataModelManager:
    """A class to manage the integration of rules, validations, and exception into the data model tables."""

    DQ_RULE = "dqops.dq_rule"
    DQ_RULE_TYPE = "dqops.dq_rule_type"
    DQ_EXCEPTIONS = "dqops.dq_exceptions"
    DQ_VALIDATIONS = "dqops.dq_validations"

    def __init__(self, data_source: str):
        self.data_source = data_source
        if self.data_source == "DeltaTable":
            from delta.tables import DeltaTable
            self.DeltaTable = DeltaTable

    def merge_in_table(
        self,
        df: DataFrame,
        columns: list,
        primary_key: str = "integration_id",
        destination_table: str = "dqops.dq_rule",
    ) -> None:
        """Generic method to store data into a Hive or Delta table with upsert or insert-only logic.

        :param df: The DataFrame to be stored.
        :param columns: List of columns to be upserted.
        :param primary_key: The primary key column for upsert logic.
        :param destination_table: The destination table to upsert into.
        """
        #debug print
        print(f"Data source in merge_in_table: {self.data_source}")
        try:
            if self.data_source == "Hive":
                temp_view_name = "temp_view"

                # Create a temporary view from the DataFrame
                df.createOrReplaceTempView(temp_view_name)

                # Perform the upsert operation using SQL
                spark.sql(f"""
                    INSERT OVERWRITE TABLE {destination_table}
                    SELECT * FROM (
                        SELECT
                            COALESCE(source.{primary_key}, target.{primary_key}) AS {primary_key},
                            {', '.join([f'COALESCE(source.{col}, target.{col}) AS {col}' for col in columns if col != primary_key])}
                        FROM {destination_table} target
                        FULL OUTER JOIN {temp_view_name} source
                        ON target.{primary_key} = source.{primary_key}
                    ) merged
                """)
            elif self.data_source == "DeltaTable":
                delta_table = self.DeltaTable.forName(spark, destination_table)
                (
                    delta_table.alias("target")
                    .merge(
                        df.alias("source"),
                        f"target.{primary_key} = source.{primary_key}",
                    )
                    .whenMatchedUpdate(set={col: f"source.{col}" for col in columns})
                    .whenNotMatchedInsert(values={col: f"source.{col}" for col in columns})
                    .execute()
                )
            else:
                raise ValueError(f"Unsupported data source: {self.data_source}")
        except Exception as e:
            raise ValueError(
                f"Error upserting data into table {destination_table}: {e}"
            )

    def _store_in_table(self, df: DataFrame, table_path: str) -> None:
        """Store data into a Hive table by appending new rows.

        :param df: Spark DataFrame containing the data to store.
        :param table_path: Path to the Hive table.
        """
        #debug print
        print(f"Data source in store_in_table: {self.data_source}")
        try:
            if self.data_source == "Hive":
                df.write.format("hive").mode("append").saveAsTable(table_path)
            elif self.data_source == "DeltaTable":
                df.write.format("delta").mode("append").saveAsTable(table_path)
            else:
                raise ValueError(f"Unsupported data source: {self.data_source}")
        except Exception as e:
            raise ValueError(f"Error storing data in table {table_path}: {e}")

    def save_expectation_suite(self, expectations: list) -> None:
        """Save the expectation suite to the data model.
        :param expectations: List of expectations.
        """
        validations = []
        for expectation in expectations:
            if hasattr(expectation, 'validation'):
                if not expectation.validation:
                    print(f"\t[!] Skipping expectation {expectation.rule_key} with no validation.")
                    continue
            else:
                print(f"\t[!] Attribute 'validation' does not exist for expectation with rule_key: {expectation.rule_key} - RULE # {expectation.rule_no}.")
                continue
            
            validation = expectation.validation
            exceptions = validation.exceptions

            current_exception_count = 0
            fixed_exception_count = 0
            new_exception_count = 0

            if self.data_source == "Hive":
                # Get the maximum value of 'exception_key' from the existing table
                max_exception_key_df = spark.sql("SELECT MAX(exception_key) as max_exception_key FROM dqops.dq_exceptions")
                max_exception_key = max_exception_key_df.collect()[0]['max_exception_key']

                # If there is no existing value, start with 1
                if max_exception_key is None:
                    max_exception_key = 0

            if exceptions:
                print(f"\t[*] Processing {len(exceptions)} exceptions for rule", validation.rule_key, "-", expectation.rule_no)
                
                exception_comparison = self.get_exception_comparison(
                    failed_rows=exceptions,
                    rule_key=validation.rule_key,
                    val_key=validation.validation_key,
                    execution_key=expectation.execution_key,
                )

                current_exception_df = exception_comparison["current_exception"]
                fixed_exception_df = exception_comparison["fixed_exception"]
                new_exception_df = exception_comparison["new_exception"]

                # Combine current and fixed exception into one DataFrame
                all_exception_df = current_exception_df.unionByName(
                    fixed_exception_df, allowMissingColumns=False
                ).unionByName(new_exception_df, allowMissingColumns=False)

                if self.data_source == "Hive":
                    # Add the 'integration_id' column
                    all_exception_df = all_exception_df.withColumn(
                        'integration_id',
                        concat_ws('~', all_exception_df['exception_row_pk'], all_exception_df['exception_row_pk'].cast('string'))
                    )

                    # Add the 'exception_key' column, starting from max_exception_key and increment by 1
                    window_spec = Window.orderBy("integration_id")
                    all_exception_df = all_exception_df.withColumn(
                        'exception_key',
                        row_number().over(window_spec) + max_exception_key
                    )

                current_exception_count = current_exception_df.count()
                fixed_exception_count = fixed_exception_df.count()
                new_exception_count = new_exception_df.count()
                
                self._store_in_table(all_exception_df, self.DQ_EXCEPTIONS)
                
                print(
                    f"\tStored {current_exception_count + fixed_exception_count + new_exception_count} exceptions: "
                    f"{new_exception_count} new, {fixed_exception_count} fixed, "
                    f"{current_exception_count} known"
                )

            total_element_count = (
                expectation.element_count or 1
            )  # Avoid division by zero

            validation.update(
                {
                    "known_exception_count": current_exception_count,
                    "new_exception_count": new_exception_count,
                    "fixed_exception_count": fixed_exception_count,
                    "known_exception_percent": current_exception_count
                    / total_element_count,
                    "new_exception_percent": new_exception_count / total_element_count,
                    "fixed_exception_percent": fixed_exception_count
                    / total_element_count,
                }
            )
            # Add the updated validation to the list
            validations.append(validation.to_dict())

        if validations:
            print(f"[*] Storing {len(validations)} validations in the data model.")
            
            if self.data_source == "Hive":
                # Convert integer values to float for the field 'exception_percent_nonmissing'
                for validation in validations:
                    if 'exception_percent_nonmissing' in validation:
                        validation['exception_percent_nonmissing'] = float(validation['exception_percent_nonmissing'])
            
            validations_df = spark.createDataFrame(
                validations, DQ_VALIDATIONS_SCHEMA
            )

            if self.data_source == "Hive":
                # Add the 'integration_id' column
                validations_df = validations_df.withColumn(
                    'integration_id',
                    concat_ws('~', validations_df['rule_key'], validations_df['execution_key'])
                )
            
            self._store_in_table(validations_df, self.DQ_VALIDATIONS)

    def get_exception_comparison(
        self, failed_rows: List[dict], rule_key: int, val_key: int, execution_key: int
    ) -> Dict[str, DataFrame]:
        """Compare the current failed rows with the latest exception from dq_exception to identify fixed and new exception.

        :param failed_rows: List of dictionaries representing the current failed rows.
        :param expectation_keys: List of expectation keys to filter the dq_exception table.
        :param table_path: Path to the dq_exception table.
        :return: A dictionary with two lists: 'fixed_exception' and 'new_exception'.
        """
        # Create a DataFrame from the current failed rows
        current_failed_df = spark.createDataFrame(failed_rows, DQ_EXCEPTIONS_SCHEMA)
        
        # Get the latest exception from dq_exception
        if self.data_source == "Hive":
            latest_exception_df = (
                spark.read.format("hive")
                .table(self.DQ_EXCEPTIONS)
                .filter(
                    (col("execution_key") == (execution_key - 1))  # Current execution key minus 1
                    & (col("rule_key") == rule_key)
                    & (col("fixed_exception_flag") == False)
                )
            )
        elif self.data_source == "DeltaTable":
            latest_exception_df = (
                spark.read.format("delta")
                .table(self.DQ_EXCEPTIONS)
                .filter(
                    (col("execution_key") == (execution_key - 1))  # Current execution key minus 1
                    & (col("rule_key") == rule_key)
                    & (col("fixed_exception_flag") == False)
                )
            )

        # Skip further processing if there is no latest exception
        if latest_exception_df.isEmpty():
            print("\tNo latest exception found for rule", rule_key)
            # Return the current failed rows as new exception as there is no latest exception
            return {
                "fixed_exception": spark.createDataFrame([], DQ_EXCEPTIONS_SCHEMA),
                "current_exception": spark.createDataFrame([], DQ_EXCEPTIONS_SCHEMA),
                "new_exception": align_to_schema(current_failed_df.withColumn("new_exception_flag", lit(True)), DQ_EXCEPTIONS_SCHEMA),
            }

        # Update the validation_key for latest exception
        latest_exception_df = latest_exception_df.join(
            current_failed_df.select("rule_key", "validation_key").distinct(),
            on="rule_key",
            how="left",
        ).drop(
            latest_exception_df["validation_key"]
        )  # Drop old validation_key to replace it

        # # Identify fixed exceptions: in latest exception but not in current failed rows
        fixed_exception_df = latest_exception_df.join(
            current_failed_df,
            on=["rule_key", "exception_row_pk"],
            how="left_anti",  # Anti-join to find exception not in current failed rows
        ).withColumn("fixed_exception_flag", lit(True))
        
        # Identify new exception: in current failed rows but not in latest exception
        new_exception_df = current_failed_df.join(
            latest_exception_df.select("rule_key", "exception_row_pk"),  # Match keys
            on=["rule_key", "exception_row_pk"],
            how="left_anti",  # Exclude rows in latest_exception_df
        ).withColumn("new_exception_flag", lit(True))

        # Remove new exception from current exception to avoid duplicates
        # Update the field CREATED_AT_TS for known exception with the ts from the latest exception
        current_exception_df = (
            current_failed_df.join(
                new_exception_df.select("rule_key", "exception_row_pk"),  # Match keys
                on=["rule_key", "exception_row_pk"],
                how="left_anti",  # Exclude rows in new_exception_df
            )
            .join(
                latest_exception_df.select(
                    "rule_key",
                    "exception_row_pk",
                    col("created_at_ts").alias("latest_created_at_ts"),
                ),
                on=["rule_key", "exception_row_pk"],  # Join on key fields
                how="left",
            )
            .withColumn(
                "created_at_ts",
                coalesce(col("latest_created_at_ts"), col("created_at_ts")),
            )
            .drop("latest_created_at_ts")
        )
        
        fixed_exception_df = align_to_schema(fixed_exception_df, DQ_EXCEPTIONS_SCHEMA)
        new_exception_df = align_to_schema(new_exception_df, DQ_EXCEPTIONS_SCHEMA)
        current_exception_df = align_to_schema(current_exception_df, DQ_EXCEPTIONS_SCHEMA)

        return {
            "fixed_exception": fixed_exception_df,
            "current_exception": current_exception_df,
            "new_exception": new_exception_df,
        }
