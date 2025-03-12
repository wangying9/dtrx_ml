# Databricks notebook source
# MAGIC %md
# MAGIC https://www.youtube.com/watch?v=x1mXZlllQmw

# COMMAND ----------

# MAGIC %pip install --upgrade boto3

# COMMAND ----------

!pip install requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

## Configure AWS credentials

# COMMAND ----------

import boto3
import requests

# COMMAND ----------

import os
cr=open('credentials.txt','r')
n=0
for i in cr.readlines():
  if 'KEY_ID' in i:
    os.environ['AWS_ACCESS_KEY_ID']=i.split('"')[1].strip()
    # print(i.split('"')[1].strip())
  if 'SECRET_ACCESS_KEY' in i:
    os.environ['AWS_SECRET_ACCESS_KEY']=i.split('"')[1].strip()
    # print(i.split('"')[1].strip())
  if 'TOKEN' in i:
    os.environ['AWS_SESSION_TOKEN']=i.split('"')[1].strip()
    # print(i.split('"')[1].strip())
cr.close()

# COMMAND ----------

os.environ['AWS_ACCESS_KEY_ID']

# COMMAND ----------

os.environ['AWS_SECRET_ACCESS_KEY']

# COMMAND ----------

os.environ['AWS_SESSION_TOKEN']

# COMMAND ----------

os.environ['AWS_DEFAULT_REGION'] = 'us-west-2' 

# COMMAND ----------

# MAGIC %md
# MAGIC ## AWS STS client

# COMMAND ----------

# AWS Configuration - Use IAM Role (No need for explicit keys if using IAM role)
aws_session = boto3.Session()
#create an AWS STS (Security Token Service) client in order to interact with AWS STS APIs
sts_client = aws_session.client('sts')

# COMMAND ----------

# Get the caller identity
# identity = sts_client.get_caller_identity()
# print("AWS Account ID:", identity['Account'])
# print("IAM Role ARN:", identity['Arn'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## S3

# COMMAND ----------

#Create S3 client
s3 = aws_session.client('s3')

# COMMAND ----------

# list s3 buckets
response = s3.list_buckets()

# COMMAND ----------

print([bucket['Name'] for bucket in response['Buckets']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lamda client

# COMMAND ----------

# Create a Lambda client
lambda_client = boto3.client('lambda')

# COMMAND ----------

# MAGIC %md
# MAGIC List all lambda functions

# COMMAND ----------

#list existing lambda functions
lambda_client.list_functions()

# COMMAND ----------

response.keys()

# COMMAND ----------

# List all Lambda functions
response = lambda_client.list_functions()

# Iterate through the functions and print the image URI (if available)
for function in response['Functions']:
    function_name = function['FunctionName']
    image_uri = function.get('ImageUri', 'No image URI set')
    print(f"Function: {function_name}, Image URI: {image_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC Retrieve details for a specific lambda function

# COMMAND ----------

function_name='bhd1-wuw2-genai-docuploadchat001'
# Get details of the Lambda function
response = lambda_client.get_function(
    FunctionName=function_name
)
# Output the response
print("Function ARN:", response['Configuration']['FunctionArn'])
# if it is a image, it will have imageuri, if it is traditional lambda package, it show no image URI set
print("Container Image URI:", response['Configuration'].get('ImageUri', 'No image URI set'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## BedRock client

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Guardrails

# COMMAND ----------

#  Initialize the Bedrock client
bedrock_client = boto3.client('bedrock', region_name='us-west-2')  # specify your region

# COMMAND ----------

#fetch a guardrails
respnse_guardrail=bedrock_client.get_guardrail(guardrailIdentifier='arn:aws:bedrock:us-west-2:619071353516:guardrail/9e67r10ri0d9')

# COMMAND ----------

type(respnse_guardrail)

# COMMAND ----------

respnse_guardrail.keys()

# COMMAND ----------

respnse_guardrail

# COMMAND ----------

respnse_guardrail['sensitiveInformationPolicy']

# COMMAND ----------

respnse_guardrail['contentPolicy']

# COMMAND ----------

pd.DataFrame.from_dict(respnse_guardrail['contentPolicy'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### LLM

# COMMAND ----------

# list all models in bedrock
bedrock_client.list_foundation_models()

# COMMAND ----------

# get a specific model
model=bedrock_client.get_foundation_model(modelIdentifier='anthropic.claude-3-5-sonnet-20240620-v1:0')

# COMMAND ----------

model.keys()

# COMMAND ----------

model['modelDetails']

# COMMAND ----------

import json

# COMMAND ----------

# Create a client for Bedrock Runtime
bedrock_runtime = boto3.client('bedrock-runtime', region_name='us-west-2')
# Define the input message
messages = [
    {
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": "Can you explain the concept of quantum computing in simple terms?"
            }
        ]
    }
]


# Prepare the request payload with the correct structure
payload = {
    "messages": messages,  # Messages as per the required schema
    "anthropic_version": "bedrock-2023-05-31",
    "max_tokens": 200,
    "top_k": 250,
    "temperature": 1,
    "top_p": 0.999
}

# Model ID will be included as part of the request headers or as part of the API call itself, not as a top-level key in the payload.

try:
    # Invoke the model (you may need to pass modelId in a different way, like via headers or in the function call itself)
    response = bedrock_runtime.invoke_model(
        modelId="anthropic.claude-3-5-haiku-20241022-v1:0",  # Specify the model directly in the API call
        body=json.dumps(payload),  # Send the body with correctly structured payload
        contentType="application/json",  # 'contentType' should be lowercase
        accept="application/json"  # 'accept' should be lowercase
    )
    # Print the entire response for debugging
    print("Full Response from Bedrock:", response)

    # Now let's check how the 'response' is structured to access the correct key
    if 'body' in response:
        result = response["body"].read().decode("utf-8")  # 'Body' is the correct field in response
        print("Response Body:", result)
    else:
        print("No 'Body' found in the response. Response is:", response)

except Exception as e:
    print("Error invoking model:", e)

# COMMAND ----------

# parse a valid JSON string and convert it into a Python Dictionary
result = json.loads(result)

# COMMAND ----------

result

# COMMAND ----------

for i in result['content']:
  if 'text' in i:
    print(i['text'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## SageMaker

# COMMAND ----------

# Create a SageMaker runtime client
sagemaker_runtime = boto3.client('sagemaker-runtime')

# COMMAND ----------

input_data={"inputText":"Can you explain the concept of quantum computing in simple terms?"}

# COMMAND ----------

# Specify the endpoint name, content type, and payload
endpoint_name = "Claude 3.5 Sonnet"
content_type = "application/json"
payload = input_data["inputText"]
# Convert the payload to JSON (or other formats as required by your model)

payload = json.dumps(payload)

# Invoke the model endpoint
response = sagemaker_runtime.invoke_endpoint(# is not authorized to perform: sagemaker
    EndpointName=endpoint_name,
    ContentType=content_type,
    Body=payload
)

# COMMAND ----------

# Create an ECR client
ecr_client = boto3.client('ecr', region_name='us-west-2')  # Use your region

# Get authorization token
response = ecr_client.get_authorization_token()

# COMMAND ----------

response

# COMMAND ----------

# List repositories
response = ecr_client.describe_repositories()

# Print out repository names
for repo in response['repositories']:
    print(repo['repositoryName'])

# COMMAND ----------

# Describe a specific repository
response = ecr_client.describe_repositories(
    repositoryNames=['bhd1-wuw2-ecr-genai004']  # Replace with the repository name
)

# Print out the details of the repository
print(response['repositories'][0])

# COMMAND ----------

# List images in a repository
response = ecr_client.list_images(
    repositoryName='bhd1-wuw2-ecr-genai004'  # Replace with your repository name
)

# Print out image tags and digests
for image in response['imageIds']:
    print(f"Image Tag: {image.get('imageTag')}, Image Digest: {image['imageDigest']}")
