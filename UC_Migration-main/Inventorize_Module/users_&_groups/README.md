# Databricks User Data Retrieval Script

This script retrieves user data from a Databricks workspace via the Databricks REST API, processes the data into a DataFrame for further analysis . Additionally, the script loads configuration details from a YAML file to interact with the Databricks API. Any required configration changes are to be made directly over the YAML file for further user.

## Prerequisites

To use this script, you need to have the following installed:

- **Python 3.x**: Make sure you are using Python 3.x.
- **Databricks Workspace Access**: You need access to a Databricks workspace and the appropriate API permissions.
- **Python Libraries**:
  - `requests`: To make API calls to Databricks.
  - `pandas`: For working with data in a DataFrame format.
  - `pyyaml`: For reading configuration files in YAML format.
  
You can install the necessary Python libraries using pip:

```bash
pip install requests pandas pyyaml pyspark
```

## Configuration

This script requires a configuration YAML file (`config.yaml`) that includes details about your Databricks workspace. Example configuration file (`config.yaml`):

```yaml
workspace_url: "https://<databricks-instance>.databricks.com"
token: "<your-databricks-api-token>"
```

Make sure the `config.yaml` file is stored in your Databricks workspace at `/Workspace/Users/<your-email>/config.yaml`.

## Script Workflow

1. **Load Configuration**: The script first loads the workspace URL and API token from the `config.yaml` file.
2. **Retrieve User Data**: The script makes an API request to the Databricks REST API to fetch user information such as `User ID`, `Display Name`, and `Email`.
3. **Process Data**: The retrieved data is converted into a Pandas DataFrame for easier manipulation.
4. **Convert to Spark DataFrame**: The Pandas DataFrame is then converted into a PySpark DataFrame to leverage Spark's distributed processing capabilities.
5. **Display Data**: The resulting Spark DataFrame is displayed in the notebook.

## Output

After executing the script, a table containing the user data is displayed.

## Troubleshooting

- **Failed to retrieve users**: If the API request fails, ensure that the API token in the `config.yaml` file is valid and has the necessary permissions to access user data.
- **No Data Returned**: Check that the Databricks workspace has active users and the correct API endpoint is being used.

