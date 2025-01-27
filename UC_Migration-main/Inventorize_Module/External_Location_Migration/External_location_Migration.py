# Databricks notebook source
# create_external_locations.py

import os
from pyspark.sql import SparkSession

# Import configuration
import config

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ExternalLocationSetup") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Set AWS credentials for accessing S3
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", config.AWS_ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config.AWS_SECRET_KEY)

def create_external_location(database, table):
    """
    Creates an external location in Unity Catalog for a given database and table.
    """
    # Define the S3 location for this table
    table_s3_location = f"{config.TABLE_LOCATION_PREFIX}/{database}/{table}"

    # Define the external location name in Unity Catalog
    external_location_name = f"{database}_{table}_location"

    # SQL commands to create the external location and assign permissions
    create_location_sql = f"""
    CREATE EXTERNAL LOCATION IF NOT EXISTS `{external_location_name}`
    URL '{table_s3_location}'
    WITH STORAGE CREDENTIAL `{config.EXTERNAL_ROLE}`;
    """
    
    grant_permissions_sql = f"""
    GRANT USAGE ON EXTERNAL LOCATION `{external_location_name}` TO CATALOG `{config.TARGET_CATALOG}`;
    GRANT READ FILES ON EXTERNAL LOCATION `{external_location_name}` TO CATALOG `{config.TARGET_CATALOG}`;
    """

    # Execute SQL commands
    try:
        print(f"Creating external location for {database}.{table} at {table_s3_location}")
        spark.sql(create_location_sql)
        spark.sql(grant_permissions_sql)
        print(f"External location `{external_location_name}` created and permissions granted.")
    except Exception as e:
        print(f"Error creating external location for {database}.{table}: {e}")

def setup_external_locations():
    """
    Retrieves the list of tables from the source database and creates external locations in Unity Catalog.
    """
    # Switch to source Hive database
    spark.catalog.setCurrentDatabase(config.SOURCE_DATABASE)

    # Retrieve tables in the source Hive database
    tables = [table.name for table in spark.catalog.listTables(config.SOURCE_DATABASE)]
    print(f"Found {len(tables)} tables in {config.SOURCE_DATABASE}.")

    # Create external location for each table
    for table in tables:
        create_external_location(config.SOURCE_DATABASE, table)

if __name__ == "__main__":
    setup_external_locations()
    print("External location setup complete.")


# COMMAND ----------

# Set the Unity Catalog and source Hive Metastore catalog
unity_catalog = 'dvltest01'
source_catalog = 'zen_sharecat'
 
# Get all databases from the source Hive Metastore catalog
hive_databases = spark.sql(f"SHOW DATABASES IN {source_catalog}").collect()
 
# Loop through each database
for db in hive_databases:
    database_name = db['databaseName']
   
    # Get all tables from the current Hive database
    tables = spark.sql(f"SHOW TABLES IN {source_catalog}.{database_name}").collect()
   
    # Loop through each table in the current database
    for table in tables:
        table_name = table['tableName']
       
        # Get table details (including type and location)
        table_details = spark.sql(f"DESCRIBE FORMATTED {source_catalog}.{database_name}.{table_name}").collect()
       
        table_type = None
        table_location = None
       
        for detail in table_details:
            if detail['col_name'] == 'Type':
                table_type = detail['data_type']
            elif detail['col_name'] == 'Location':
                table_location = detail['data_type']
       
        # Create the corresponding Unity Catalog database if it doesn't exist
        uc_db = database_name
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {unity_catalog}.{uc_db}")
       
        print(f"Processing table: {table_name}")
        print(f"Table type: {table_type}")
       
        # Create the table in Unity Catalog based on its type
        if table_type == 'MANAGED':
            spark.sql(f"CREATE TABLE IF NOT EXISTS {unity_catalog}.{uc_db}.{table_name} CLONE {source_catalog}.{database_name}.{table_name}")
        elif table_type == 'EXTERNAL':
            spark.sql(f"CREATE TABLE IF NOT EXISTS {unity_catalog}.{uc_db}.{table_name} USING DELTA LOCATION '{table_location}'")
        else:
            print(f"Table {table_name} in database {database_name} is of an unrecognized type: {table_type}")
 
print("Migration completed.")
