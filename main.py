import os
from pyspark.sql import SparkSession
import warnings

# Suppress all DeprecationWarnings (not recommended unless you've reviewed them)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Set the data source type
#data_source = "Hive"  
data_source = "DeltaTable"


if data_source == "DeltaTable":
    # Case for DeltaTable
    # Databricks notebook source
    #!pip install great_expectations==1.3.0
    
    # COMMAND ----------

    dbutils.library.restartPython()

    # COMMAND ----------

    def get_tables_matching_pattern(schema_name="dqops", pattern="dq_rules"):
        """Get all table names in the specified schema that match the given pattern.

        :param schema_name: The schema (database) name.
        :param pattern: The pattern to match table names (e.g., 'data_quality_rules%').
        :return matching_tables: List of table names matching the pattern.
        """
        # Set the schema
        spark.sql(f"USE SCHEMA {schema_name}")
        # List all tables in the schema
        tables = spark.catalog.listTables(schema_name)
        # Filter tables that match the naming convention
        return [table.name for table in tables if pattern in table.name]

    # COMMAND ----------

    os.environ["GX_ANALYTICS_ENABLED"] = "false"
    PATTERN = "brnz_sharepoint_dq_rules_jobseekers_gld"
    SCHEMA = "dqops"
    tables = get_tables_matching_pattern(SCHEMA, PATTERN)

    # COMMAND ----------

    from modules.rule_manager import RuleManager
    from modules.gx_context_manager import GXContextManager
    from modules.validation_manager import ValidationManager

    context_manager = GXContextManager()
    rule_manager = RuleManager(data_source)
    validation_manager = ValidationManager()

    # COMMAND ----------

    # Load the rules from the specified table
    #rule_manager.update_rules(tables)

    # COMMAND ----------

    # Iterate over the rules and update the context
    df = rule_manager.parse_rules(context_manager)

    # COMMAND ----------

    # Run the rules
    results = validation_manager.run(context_manager)

    # COMMAND ----------

    # Save the results
    context_manager.save_results()

elif data_source == "Hive":
    # Case for Hive
    
    # Stop the existing Spark session if it exists
    if 'spark' in globals():
        spark.stop()
    
    # Initialize a new SparkSession 
    spark = SparkSession.builder.appName("DQ-Tool").getOrCreate()

    def get_tables_matching_pattern(schema_name="dqops", pattern="dq_rule"):
        """Get all table names in the specified schema that match the given pattern.
        :param schema_name: The schema (database) name.
        :param pattern: The pattern to match table names (e.g., 'data_quality_rules%').
        :return matching_tables: List of table names matching the pattern.
        """
        # Set the schema
        spark.sql(f"USE SCHEMA {schema_name}")

        # List all tables in the schema
        tables = spark.catalog.listTables(schema_name)
        
        # Filter tables that match the naming convention
        return [table.name for table in tables if pattern in table.name]

    os.environ["GX_ANALYTICS_ENABLED"] = "false"
    PATTERN = "dq_rule"
    SCHEMA = "dqops"
    tables = get_tables_matching_pattern(SCHEMA, PATTERN)

    from modules.rule_manager import RuleManager
    from modules.gx_context_manager import GXContextManager
    from modules.validation_manager import ValidationManager

    context_manager = GXContextManager()
    rule_manager = RuleManager(data_source)
    validation_manager = ValidationManager()

    # We are not updating the rules table from code in Hive case. The rule tables should be udpated manually in Hive.
    # That's why keeping this next line commented.
    # Load the rules from the specified table
    # rule_manager.update_rules(tables)

    # Iterate over the rules and update the context
    rule_manager.parse_rules(context_manager)

    # Run the rules
    results = validation_manager.run(context_manager)

    # Save the results
    context_manager.save_results()

    # Stop SparkSession
    spark.stop()




