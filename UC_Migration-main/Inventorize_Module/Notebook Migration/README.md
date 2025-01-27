

---

# Databricks Notebook Migration from Hive Metastore to Unity Catalog

This Directory contains a Python script to automate the migration of Databricks notebooks from Hive Metastore to Unity Catalog. The script performs path updates within notebooks, replacing `hive_metastore` references with specified Unity Catalog (UC) paths. Additionally, it includes testing functions to verify the correctness of the migration by checking updated paths in notebooks.

## Table of Contents

1. [Overview](#overview)
2. [Features](#features)
3. [Prerequisites](#prerequisites)
4. [Setup Instructions](#setup-instructions)
5. [Parameters](#parameters)
6. [Usage](#usage)
7. [Testing Migration](#testing-migration)
8. [Troubleshooting](#troubleshooting)
9. [License](#license)

---

## Overview

As organizations adopt Unity Catalog for data management in Databricks, migrating resources from Hive Metastore becomes necessary. This script automates notebook migration by updating `hive_metastore` references to Unity Catalog paths, ensuring a seamless transition and compatibility within Databricks workspaces.

## Features

- **Automated Path Update**: Replaces `hive_metastore` paths with Unity Catalog paths in notebooks.
- **Recursive Directory Search**: Checks all notebooks within a specified directory, including subdirectories.
- **Parameterization**: Accepts configuration parameters via a YAML file.
- **Path Verification**: Verifies if notebooks have been correctly updated to use Unity Catalog paths.
- **Error Handling and Logging**: Provides detailed error messages for failed operations, aiding in troubleshooting.

## Prerequisites

1. **Databricks Environment**: Access to a Databricks workspace with Unity Catalog enabled.
2. **Databricks CLI**: Install and configure the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html).
3. **Permissions**: Ensure adequate permissions to read notebooks and update content in the specified directories.
4. **Python 3.x**: This script requires Python 3.x and the following dependencies:
   - `pyyaml`
   - `requests`

   You can install `pyyaml` using:
   ```bash
   pip install pyyaml
   ```

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-repo/notebook-migration.git
   cd notebook-migration
   ```

2. **Create Configuration File**:
   Create a `config.yaml` file in the root directory with the following structure:
   ```yaml
   DATABRICKS_Host: "https://<your-databricks-workspace-url>"
   DATABRICKS_TOKEN: "<your-databricks-api-token>"
   UC_CATALOG_NAME: "<your-unity-catalog-name>"
   UPDATED_DIRECTORY: "<path-to-directory-containing-notebooks>"
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Parameters

The following parameters are loaded from `config.yaml`:

- **`DATABRICKS_Host`**: URL for your Databricks workspace.
- **`DATABRICKS_TOKEN`**: Databricks API token for authentication.
- **`UC_CATALOG_NAME`**: The name of the target Unity Catalog.
- **`UPDATED_DIRECTORY`**: Directory path containing notebooks for migration.

## Usage

1. **Run the Migration Script**:
   Execute the migration script to update paths in notebooks:
   ```python
   update_notebooks_with_hive_metastore(config['UPDATED_DIRECTORY'])
   ```

   - **Path Update Function**: The `update_notebook_paths` function replaces instances of `hive_metastore` with Unity Catalog paths in notebooks.
   - **Recursive Notebook Check**: The script will iterate through all notebooks in the specified directory, updating paths as needed.

2. **Notebook Update**:
   The script will update each notebook’s content and overwrite it in the specified Databricks directory.

## Testing Migration

After running the migration, verify that notebooks have been updated correctly using the testing script:

```python
check_notebook_paths_in_directory(config['UPDATED_DIRECTORY'])
```

- **Notebook Path Check**: This function reads each notebook and checks for remaining `hive_metastore` references.
- **UC Catalog Verification**: Confirms that Unity Catalog paths have been correctly replaced in all notebooks.

## Troubleshooting

### Common Issues

1. **Access Denied Errors**:
   Ensure your Databricks token has sufficient permissions to read and update notebooks.

2. **Invalid YAML Configuration**:
   Double-check the `config.yaml` structure. Ensure all required parameters are present and correctly formatted.

3. **Notebook Format Mismatch**:
   If the notebook language is not Python, adjust the `language` field in the `update_notebook_in_databricks` function to match the correct language.

4. **Databricks CLI Authentication Errors**:
   Reconfigure the Databricks CLI if you encounter authentication issues.

### Additional Help

For further assistance, refer to the [Databricks Documentation](https://docs.databricks.com) or contact your Databricks support team.



---

