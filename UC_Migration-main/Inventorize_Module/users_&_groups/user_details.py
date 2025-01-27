# Databricks notebook source
import requests
import pandas as pd
import yaml

# Load configuration from the YAML file
with open("/Workspace/Users/imaks2597@gmail.com/config.yaml", 'r') as file:
    config = yaml.safe_load(file)

# Set up your Databricks workspace information
workspace_url = config['workspace_url']
token = config['token'] 

print(workspace_url )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Retrieving user data Using Databricks API

# COMMAND ----------

import requests
import pandas as pd
import yaml

# Load configuration from the YAML file
with open("/Workspace/Users/imaks2597@gmail.com/config.yaml", 'r') as file:
    config = yaml.safe_load(file)

# Set up your Databricks workspace information
workspace_url = config['workspace_url']
token = config['token']

# Define the endpoint for listing users
endpoint = f"{workspace_url}/api/2.0/preview/scim/v2/Users"

# Set up headers for the API request
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Make the request to get users
response = requests.get(endpoint, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    users = response.json().get("Resources", [])
    user_data = [
        (user['id'], user['displayName'], user['emails'][0]['value']) 
        for user in users
    ]
    df_users = pd.DataFrame(user_data, columns=['User ID', 'Display Name', 'Email'])
    spark_df_users = spark.createDataFrame(df_users)
    display(spark_df_users)
else:
    raise Exception(f"Failed to retrieve users: {response.status_code} - {response.text}")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Retrieving user details along with entitlement

# COMMAND ----------

import requests
import pandas as pd
import yaml

# Load configuration from the YAML file
with open("/Workspace/Users/imaks2597@gmail.com/config.yaml", 'r') as file:
    config = yaml.safe_load(file)

# Set up your Databricks workspace information
workspace_url = config['workspace_url']
token = config['token'] 

# Define the endpoint for listing users
endpoint = f"{workspace_url}/api/2.0/preview/scim/v2/Users"

# Set up headers for the API request
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Make the request to get users
response = requests.get(endpoint, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    users = response.json().get("Resources", [])
    user_data = [
        (
            user['id'], 
            user['displayName'], 
            user['emails'][0]['value'], 
            user.get('entitlements', []),
            user.get('roles', []),
            user.get('groups', []),
            user.get('active', False)
        ) 
        for user in users
    ]
    df_users = pd.DataFrame(user_data, columns=['User ID', 'Display Name', 'Email', 'Entitlements', 'Roles', 'Groups', 'Active'])
    spark_df_users = spark.createDataFrame(df_users)
    display(spark_df_users)
else:
    raise Exception(f"Failed to retrieve users: {response.status_code} - {response.text}")
