# Write a Python program using Boto3 to list all EventBridge event buses in your AWS account.

import boto3

eventbridge_client = boto3.client('events', region_name='ap-south-1')
try:
    response = eventbridge_client.list_event_buses()
    print("EventBridge Event Buses:")
    for bus in response['EventBuses']:
        print("Name:", bus['Name'])
except Exception as e:
    print("Error listing EventBridge event buses:", e)