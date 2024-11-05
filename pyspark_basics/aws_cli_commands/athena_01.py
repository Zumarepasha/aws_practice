# Write a Python program using Boto3 to start a query in Athena and return the query execution ID.

import boto3
athena_client = boto3.client('athena', region_name='us-east-1')
database = 'your-database-name'
query_string = 'SELECT * FROM your_table LIMIT 10;'
try:
    response = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': 's3://your-output-bucket/'}
    )
    query_execution_id = response['QueryExecutionId']
    print("Started Athena query with execution ID:", query_execution_id)
except Exception as e:
    print("Error starting Athena query:", e)