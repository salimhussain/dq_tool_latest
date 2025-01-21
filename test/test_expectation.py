# Databricks notebook source
!pip install great_expectations==1.3.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
os.environ["GX_ANALYTICS_ENABLED"] = "false"

from json import loads
import great_expectations as ge
import importlib

# COMMAND ----------

catalog = "dbw_gt_data_dev_lake_catalog"
schema  = "gold"
table_name = "gld_dim_employee"
column = "date_of_birth"
table_key_list = ["integration_id"] # List of PK columns

# COMMAND ----------

# e.g., ExpectColumnValuesToBeBetween, ExpectColumnValuesToBeInSet, UnexpectedRowsExpectation
# ExpectColumnValuesToMatchRegex, ExpectColumnValuesToNotBeNull
expectation_name = "ExpectColumnValuesToNotBeNull"
arguments = '{"column": "date_of_birth"}'
args = loads(arguments)
args

# COMMAND ----------

def my_import(name: str, arguments: dict) -> object:
    """Import a specific Expectation Class from GX Expectation module
    name -- name of the Expectation Class to be imported
    arguments -- dictionary with the arguments to be passed to the Expectation Class
    """
    module = importlib.import_module("great_expectations.expectations")
    return getattr(module, name)(**arguments)

def test_expectation(df = None):
    context = ge.get_context()
    data_name = f"{catalog}.{schema}.{table_name}"
    
    # Set up context
    data_source = context.data_sources.add_spark(name=data_name)
    data_asset = data_source.add_dataframe_asset(name=table_name)
    
    if not df:
        batch_parameters = {"dataframe": spark.read.table(f"{catalog}.{schema}.{table_name}")}
    else:
        batch_parameters = {"dataframe": df}
    
    expectation_suite = context.suites.add(ge.core.expectation_suite.ExpectationSuite(name=f"s-{data_name}"))
    
    # Get the expectation class
    gx_expectation = my_import(expectation_name, args)
    # Add the expectation to the suite
    expectation_suite.add_expectation(gx_expectation)
        
    batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_definition")
    validation_definition = context.validation_definitions.add(
            ge.core.validation_definition.ValidationDefinition(
                name=f"vd-{data_name}",
                data=batch_definition,
                suite=expectation_suite,
            )
    )
    checkpoint = context.checkpoints.add(
        ge.Checkpoint(
            name=f"cp-{data_name}",
            validation_definitions=[validation_definition],
            result_format={
                "result_format": "COMPLETE",
                "unexpected_index_column_names": table_key_list,
                "partial_unexpected_count": 0,
                "exclude_unexpected_values": False,
                "include_unexpected_rows": True,
                "return_unexpected_index_query": False,
            },
        )
    )
    result = checkpoint.run(batch_parameters=batch_parameters)
    return result

# COMMAND ----------

result = test_expectation()

# COMMAND ----------

result

# COMMAND ----------


