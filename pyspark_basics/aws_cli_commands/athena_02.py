# Write a python program to create table in athena

import boto3, time

client = boto3.client('athena', region_name = 'ap-south-1')

query_string = """ 
CREATE EXTERNAL TABLE IF NOT EXISTS zp_db.employee (
    id INT,
    name STRING,
    age INT,
    country STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '\"'
)
LOCATION 's3://zp-demo-buck/source/'
TBLPROPERTIES ('has_encrypted_data'='false');
"""
 
query_id = client.start_query_execution(QueryString=query_string, ResultConfiguration={'OutputLocation': 's3://zp-demo-buck/athena-results/'} )['QueryExecutionId']


while True:
    if client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State'] == 'RUNNING': 
        print("sleeping")
        time.sleep(1)
    else:
        print("Table Created")
        break
