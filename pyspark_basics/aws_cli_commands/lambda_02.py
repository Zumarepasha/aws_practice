# Write a Python program to invoke a specific Lambda function by its name and print the response.

import boto3
import json
 
lambda_client = boto3.client('lambda', region_name='ap-south-1')  # Replace with your region
 
function_name = 'zp-lambda'  # Replace with your Lambda function's name
 
payload = {
    "key1": "value1",
    "key2": "value2"
}
 
try:
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='RequestResponse',  # RequestResponse waits for a response
        Payload=json.dumps(payload)  # Convert the payload to JSON format
    )
 
    response_payload = response['Payload'].read()
    print("Lambda Response:")
    print(json.loads(response_payload))
 
except Exception as e:
    print("Error invoking Lambda function:", e)