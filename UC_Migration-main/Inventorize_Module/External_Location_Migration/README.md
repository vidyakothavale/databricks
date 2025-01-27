# Hive to Unity Catalog Migration - External Location Setup

This project contains a set of scripts to automate the creation of external locations in Databricks Unity Catalog. It is designed for migrating tables from a Hive metastore to Unity Catalog, specifically to the `ucx_migration_priyansh` catalog. The script creates external locations in AWS S3 for each table, assigns permissions, and prepares them for migration.

## Features

- Creates external locations in Unity Catalog for each table in a Hive metastore database.
- Assigns required permissions for Unity Catalog to access data in external locations.
- Supports AWS S3 as the storage backend for external locations.
- Automates setup, reducing manual intervention for large-scale migrations.

## Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled.
- AWS S3 bucket where tables will be stored.
- Hive metastore with the tables to be migrated.
- Python with `pyspark` installed and configured for Databricks.

### Project Structure

- `config.py`: Configuration file with AWS credentials, database information, and external location settings.
- `create_external_locations.py`: Main script to create external locations and assign permissions.

## Configuration

Before running the script, set up your AWS credentials, target catalog, and bucket information in `config.py`.

Example `config.py`:

```python
# config.py

# AWS S3 credentials and bucket for external locations
AWS_ACCESS_KEY = "your_aws_access_key"
AWS_SECRET_KEY = "your_aws_secret_key"
S3_BUCKET = "your_s3_bucket_name"          # The main S3 bucket for tables

# Hive and Unity Catalog details
SOURCE_DATABASE = "default"                 # Hive metastore source catalog
TARGET_CATALOG = "ucx_migration_priyansh"   # Unity Catalog target catalog

# External location settings
TABLE_LOCATION_PREFIX = f"s3://{S3_BUCKET}/tables"  # Base S3 path for table locations
EXTERNAL_ROLE = "your_databricks_iam_role"          # Databricks IAM role for S3 access
