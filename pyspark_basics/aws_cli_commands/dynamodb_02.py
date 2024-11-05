# Write a Python script to add a new item to a specific DynamoDB table by providing a dictionary of attributes.

import boto3
 
dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')  # Replace with your region
 
table_name = 'Employees'  # Replace with your DynamoDB table name
table = dynamodb.Table(table_name)
 
item = {
    'Emp_id': '001',  # Replace with your actual primary key
    'Attribute1': 'Value1',
    'Attribute2': 'Value2'
}
 
try:
    table.put_item(Item=item)
    print(f"Item added to table '{table_name}':")
    print(item)
except Exception as e:
    print("Error adding item:", e)