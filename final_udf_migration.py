# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog hive_metastore

# COMMAND ----------

database_functions = []
databases = spark.sql("SHOW DATABASES IN hive_metastore").collect()
database_names = [row["databaseName"] for row in databases]
for database_name in database_names:
    try:
        functions = spark.sql(f"SHOW user FUNCTIONS IN hive_metastore.{database_name}").collect()
        function_names = [row["function"] for row in functions]
        for func in function_names:
            database_functions.append({"database": database_name, "function": func})
        print(f"\nDatabase: {database_name}")
        for func in function_names:
            print(f"  Function: {func}")
    except Exception as e:
        print(f"Error fetching functions for database '{database_name}': {e}")
# Optional: Convert to DataFrame for further processing
database_functions_df = spark.createDataFrame(database_functions)
display(database_functions_df)

# COMMAND ----------

import re
from pyspark.sql import SparkSession
databases = spark.sql("SHOW DATABASES IN hive_metastore")
database_names = [row["databaseName"] for row in databases.collect()]
for database_name in database_names:
    try:
        # Fetch all user-defined functions in the current database
        functions = spark.sql(f"SHOW user FUNCTIONS IN hive_metastore.{database_name}")
        function_names = [row["function"].split(".")[-1] for row in functions.collect()]  # Extract only the function name
        # Process each function
        for function_name in function_names:
            # Skip the `getargument` function
            if function_name.lower() == "getargument":
                print(f"Skipping function '{function_name}' in database '{database_name}'")
                continue
            try:
                function_details = spark.sql(f"DESCRIBE FUNCTION EXTENDED hive_metastore.{database_name}.{function_name}")
                udf_info = function_details.collect()
                function_name_val = function_name  # Now only the function name
                function_type = None
                return_type = None
                body = None
                input_params = []
                # Flags to identify sections
                input_section = False               
                # Parse the function details
                for row in udf_info:
                    line = row["function_desc"].strip()
                    
                    # Capture the function type
                    if "Type:" in line:
                        function_type = line.split("Type:")[1].strip()
                    
                    # Capture input parameters
                    if line.startswith("Input:"):
                        input_section = True
                        line = line.replace("Input:", "").strip()
                    
                    # Stop capturing inputs when reaching another section
                    elif input_section and (line.startswith("Returns:") or line.startswith("Body:")):
                        input_section = False
                    
                    if input_section:
                        input_params.append(line.strip())
                    
                    # Capture the return type
                    if "Returns:" in line:
                        return_type = line.split("Returns:")[1].strip()
                    
                    # Capture the body (if available)
                    if "Body:" in line:
                        body = line.split("Body:")[1].strip()
                # Parsing each input parameter's details
                parsed_inputs = []
                for input_line in input_params:
                    # Regex to capture input parameter name and type
                    match = re.match(r"(\w+)\s+(\w+)", input_line)
                    if match:
                        param_name = match.group(1)
                        param_type = match.group(2)
                        parsed_inputs.append(f"{param_name} {param_type}")
                # Create the function definition statement
                input_params_str = ", ".join(parsed_inputs)
                function_definition = f"CREATE OR REPLACE FUNCTION ucx_vidya.{database_name}.{function_name_val}({input_params_str})\nRETURNS {return_type}\nRETURN {body};"
                print(f"Attempting to recreate function:\n{function_definition}")
                # Execute the function creation in Unity Catalog
                spark.sql(function_definition)
                print(f"Function '{function_name_val}' successfully recreated in Unity Catalog under 'ucx_vidya.{database_name}'.") 
            except Exception as e:
                print(f"Error recreating function '{function_name}' in database '{database_name}': {e}")
    except Exception as e:
        print(f"Error processing database '{database_name}': {e}")


# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ucx_vidya

# COMMAND ----------

database_functions = []
databases = spark.sql("SHOW DATABASES IN ucx_vidya").collect()
database_names = [row["databaseName"] for row in databases]
for database_name in database_names:
    try:
        functions = spark.sql(f"SHOW user FUNCTIONS IN ucx_vidya.{database_name}").collect()
        function_names = [row["function"] for row in functions]
        for func in function_names:
            database_functions.append({"database": database_name, "function": func})
        print(f"\nDatabase: {database_name}")
        for func in function_names:
            print(f"  Function: {func}")
    except Exception as e:
        print(f"Error fetching functions for database '{database_name}': {e}")
# Optional: Convert to DataFrame for further processing
database_functions_df = spark.createDataFrame(database_functions)
display(database_functions_df)

# COMMAND ----------


