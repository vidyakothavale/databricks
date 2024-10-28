# Databricks notebook source

AWS_ACCESS_KEY_ID = '<AWS_ACCESS_KEY_ID>'
AWS_SECRET_ACCESS_KEY = '<AWS_SECRET_ACCESS_KEY>'
AWS_REGION = '<AWS REGION>'
ROLE_NAME = '<ROLE NAME>'
POLICY_NAME = '<POLICY NAME>'
EVENT_POLICY_NAME='<EVENT POLICY NAME>'
AWS_ACCOUNT_ID = '<AWS_ACCOUNT_ID>'
BUCKET_NAME = '<BUCKET NAME>'

DATABRICKS_INSTANCE = "<DATABRICKS_INSTANCE>"
DATABRICKS_TOKEN = "<DATABRICKS_TOKEN>"
EXTERNAL_ID = '<EXTERNAL_ID>'
storage_credential_name = '<credential name>'
external_location_name='<External location name>'
user_mail='<user_mail>' #write username who wants to gave access of storage credential

# COMMAND ----------



# COMMAND ----------

import boto3
import requests
import json
import time


# COMMAND ----------

# MAGIC %md
# MAGIC create iam role

# COMMAND ----------

def create_iam_role(role_name):
    boto3.setup_default_session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    # Initialize a session using Amazon IAM
    iam_client = boto3.client('iam')
    trust_policy = {
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "AWS": [
        "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
      ]
    },
    "Action": "sts:AssumeRole",
    "Condition": {
      "StringEquals": {
        "sts:ExternalId": "0000"
      }
    }
  }]
}

    # Create the IAM role if it doesn't already exist
    try:
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Role to allow Databricks access to S3 bucket'
        )
        msg = "Role successfully created"
    except iam_client.exceptions.EntityAlreadyExistsException:
        response = iam_client.get_role(RoleName=role_name)
        msg = "Role already exists"
    # Display the message
    display(msg)

# COMMAND ----------

create_iam_role(ROLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC Create the following IAM policy in the same account as the S3 bucket

# COMMAND ----------

def create_and_attach_policy(
    aws_access_key_id, aws_secret_access_key, aws_region,
    BUCKET_NAME, role_name, aws_account_id, policy_name
):
    boto3.setup_default_session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    iam_client = boto3.client('iam')
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}/*",
                    f"arn:aws:s3:::{BUCKET_NAME}"
                ],
                "Effect": "Allow"
            },
            {
                "Action": ["sts:AssumeRole"],
                "Resource": [f"arn:aws:iam::{aws_account_id}:role/{role_name}"],
                "Effect": "Allow"
            }
        ]
    }
    #create the IAM policy or retrieve existing policy
    try:
        response = iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description='Policy to allow Databricks access to S3 bucket'
        )
        print("Policy successfully created")
        policy_arn = response['Policy']['Arn']
    except iam_client.exceptions.EntityAlreadyExistsException:
        policy_arn = f"arn:aws:iam::{aws_account_id}:policy/{policy_name}"
        response = iam_client.get_policy(PolicyArn=policy_arn)
        print("Policy already exists")
        
        # Get the current policy version document
        version_id = response['Policy']['DefaultVersionId']
        policy_version = iam_client.get_policy_version(
            PolicyArn=policy_arn,
            VersionId=version_id
        )
        current_policy_document = policy_version['PolicyVersion']['Document']

        # Check if the current bucket is in the policy
        bucket_in_policy = any(
            f"arn:aws:s3:::{BUCKET_NAME}" in resource
            for statement in current_policy_document.get("Statement", [])
            if "Resource" in statement
            for resource in (statement["Resource"] if isinstance(statement["Resource"], list) else [statement["Resource"]])
        )
        
        if not bucket_in_policy:
            # Update policy document to add the current bucket
            current_policy_document['Statement'][0]['Resource'].extend([
                f"arn:aws:s3:::{BUCKET_NAME}/*",
                f"arn:aws:s3:::{BUCKET_NAME}"
            ])
            # Create a new version of the policy with the updated document
            iam_client.create_policy_version(
                PolicyArn=policy_arn,
                PolicyDocument=json.dumps(current_policy_document),
                SetAsDefault=True
            )
            print("Policy updated to include the new bucket")
    # Attach the IAM policy to the IAM role
    try:
        iam_client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
        print("Policy successfully attached to the role")
    except Exception as e:
        print(f"Failed to attach policy: {str(e)}")


# COMMAND ----------

create_and_attach_policy(
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    BUCKET_NAME, ROLE_NAME, AWS_ACCOUNT_ID, POLICY_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the DatabricksFileEventsPolicy IAM policy in the same account as the S3 bucket.Create an IAM policy for file events in the same account as the S3 bucket.

# COMMAND ----------

def create_and_attach_event_policy(
    aws_access_key_id, aws_secret_access_key, aws_region,
    bucket_name, role_name, aws_account_id, event_policy_name
):
    boto3.setup_default_session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    iam_client = boto3.client('iam')
    # Define the IAM policy for file events
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ManagedFileEventsSetupStatement",
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketNotification",
                    "s3:PutBucketNotification",
                    "sns:ListSubscriptionsByTopic",
                    "sns:GetTopicAttributes",
                    "sns:SetTopicAttributes",
                    "sns:CreateTopic",
                    "sns:TagResource",
                    "sns:Publish",
                    "sns:Subscribe",
                    "sqs:CreateQueue",
                    "sqs:DeleteMessage",
                    "sqs:ReceiveMessage",
                    "sqs:SendMessage",
                    "sqs:GetQueueUrl",
                    "sqs:GetQueueAttributes",
                    "sqs:SetQueueAttributes",
                    "sqs:TagQueue",
                    "sqs:ChangeMessageVisibility"
                ],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    "arn:aws:sqs:*:*:*",
                    "arn:aws:sns:*:*:*"
                ]
            },
            {
                "Sid": "ManagedFileEventsListStatement",
                "Effect": "Allow",
                "Action": [
                    "sqs:ListQueues",
                    "sqs:ListQueueTags",
                    "sns:ListTopics"
                ],
                "Resource": "*"
            },
            {
                "Sid": "ManagedFileEventsTeardownStatement",
                "Effect": "Allow",
                "Action": [
                    "sns:Unsubscribe",
                    "sns:DeleteTopic",
                    "sqs:DeleteQueue"
                ],
                "Resource": [
                    "arn:aws:sqs:*:*:*",
                    "arn:aws:sns:*:*:*"
                ]
            }
        ]
    }
    # Create the IAM policy
    try:
        response = iam_client.create_policy(
            PolicyName=event_policy_name,
            PolicyDocument=json.dumps(policy_document),
            Description='Policy to allow Databricks to manage file events'
        )
        msg = "Policy successfully created"
    except iam_client.exceptions.EntityAlreadyExistsException:
        response = iam_client.get_policy(PolicyArn=f"arn:aws:iam::{aws_account_id}:policy/{event_policy_name}")
        msg = "Policy already exists"
    # Display the response and message
    display(msg)
    # Attach the IAM policy to the IAM role
    policy_arn = response['Policy']['Arn']
    try:
        attach_response = iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        attach_msg = "Policy successfully attached to the role"
    except Exception as e:
        attach_msg = f"Failed to attach policy: {str(e)}"
    # Display the attach response and message
    display(attach_msg)

# COMMAND ----------

create_and_attach_event_policy(
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    BUCKET_NAME, ROLE_NAME, AWS_ACCOUNT_ID, EVENT_POLICY_NAME
)


# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Give Databricks the IAM role details

# COMMAND ----------

# MAGIC %md
# MAGIC create credential

# COMMAND ----------

def create_storage_credential(storage_credential_name, AWS_ACCOUNT_ID, ROLE_NAME, DATABRICKS_INSTANCE, DATABRICKS_TOKEN, read_only=False):
    role_arn = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/{ROLE_NAME}"
    payload = {
        "name": storage_credential_name,
        "aws_iam_role": {
            "role_arn": role_arn
        },
        "read_only": read_only
    }
    url = f"{DATABRICKS_INSTANCE}/api/2.1/unity-catalog/storage-credentials"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        display(f"Storage credential created successfully: {storage_credential_name}")
    else:
        display(f"Failed to create storage credential. Status Code: {response.status_code}, Response: {response.text}, Storage credential name: {storage_credential_name}")

# COMMAND ----------

create_storage_credential(storage_credential_name, AWS_ACCOUNT_ID, ROLE_NAME, DATABRICKS_INSTANCE, DATABRICKS_TOKEN, read_only=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Update the IAM role trust relationship policy

# COMMAND ----------

def update_trust_relationship(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ACCOUNT_ID, ROLE_NAME, EXTERNAL_ID):
    boto3.setup_default_session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    iam_client = boto3.client('iam')
    # Define the IAM role name and external ID
    trust_relationship = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": [
                        "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL",
                        f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/{ROLE_NAME}"
                    ]
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                    "StringEquals": {
                        "sts:ExternalId": EXTERNAL_ID
                    }
                }
            }
        ]
    }
    for attempt in range(3):
        try:
            response = iam_client.update_assume_role_policy(
                RoleName=ROLE_NAME,
                PolicyDocument=json.dumps(trust_relationship)
            )
            display(response)
            break  # Exit loop if successful
        except Exception as e:
            if attempt < 2:  # Retry logic, do not wait after the last attempt
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                display(f"Failed to update trust relationship policy after {attempt + 1} attempts: {e}")

# COMMAND ----------

update_trust_relationship(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ACCOUNT_ID, ROLE_NAME, EXTERNAL_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC Create External Location

# COMMAND ----------

# MAGIC %md
# MAGIC _Path of aws bucket_

# COMMAND ----------

def create_s3_client_and_display_bucket_info(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BUCKET_NAME):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    # Store the provided bucket name and path in a variable
    bucket_path = f"s3://{BUCKET_NAME}/"
    bucket_info = [(BUCKET_NAME, bucket_path)]
    display(bucket_info)
    print(bucket_info)

# COMMAND ----------

create_s3_client_and_display_bucket_info(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, BUCKET_NAME)

# COMMAND ----------

BUCKET_PATH=f"s3://{BUCKET_NAME}/"


# COMMAND ----------

# MAGIC %md
# MAGIC Grant on storage credential and create external location
# MAGIC

# COMMAND ----------

def manage_external_location(external_location_name, storage_credential_name, user_mail, BUCKET_PATH):
    # Check if the external location already exists
    check_query = "SHOW EXTERNAL LOCATIONS;"
    existing_locations = spark.sql(check_query)

    # Check if the external location exists in the results
    location_exists = any(loc['name'] == external_location_name for loc in existing_locations.collect())

    if not location_exists:
        spark.sql(f"""
        GRANT CREATE EXTERNAL LOCATION ON STORAGE CREDENTIAL `{storage_credential_name}` TO `{user_mail}`;
        """)
        
        spark.sql(f"""
        CREATE EXTERNAL LOCATION `{external_location_name}`
        URL '{BUCKET_PATH}'
        WITH (STORAGE CREDENTIAL `{storage_credential_name}`)
        COMMENT 'Default source for AWS external data';
        """)
        
        spark.sql(f"""
        GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `{external_location_name}` TO `{user_mail}`;
        """)
        
        print(f"External location '{external_location_name}' created and all privileges granted successfully.")
    else:
        print(f"External location '{external_location_name}' already exists.")

# COMMAND ----------

manage_external_location(external_location_name, storage_credential_name, user_mail, BUCKET_PATH)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create a location accessed using the s3_remote_cred credential
# MAGIC --CREATE EXTERNAL LOCATION `s3-remote` URL 's3://mount-point'
# MAGIC     -- WITH (STORAGE CREDENTIAL `storagecre112`)
# MAGIC      --COMMENT 'Default source for AWS exernal data';
# MAGIC

# COMMAND ----------


