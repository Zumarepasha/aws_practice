# RetailMart wants to identify inactive customers who haven’t made recent purchases. 
# Load customer data from DynamoDB and transaction data from S3, and calculate the last 
# purchase date for each customer using PySpark. Flag customers as inactive if their last 
# purchase was more than six months ago, then update this status in DynamoDB.

import boto3
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, current_date, datediff
from datetime import datetime, timedelta
spark = SparkSession.builder.appName("CustomerAnalysis")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.access.key", "your_access_key_id")\
    .config("spark.hadoop.fs.s3a.secret.key", "your_secret_access_key")\
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.metastore.metrics.enabled", "false") \
    .config("spark.hadoop.io.native.lib.available", "false")\
    .config("spark.executor.memory", "4g")\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()

os.environ['AWS_ACCESS_KEY_ID'] = 'your_access_key_id' 
os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret_access_key' 
os.environ['AWS_DEFAULT_REGION'] = 'your_region' 
 
# Create a session with your credentials 
session = boto3.Session(aws_access_key_id='your_access_key_id', aws_secret_access_key='your_secret_access_key', region_name='your_region')  
dynamodb = session.resource('dynamodb') 
table = dynamodb.Table('Customers_Data')

# Load data from s3
trans_input = "s3a://zp-demo-buck/source/transactions.csv"
trans_df = spark.read.csv(trans_input, header=True, inferSchema=True)

# Load customer data from DynamoDB
response = table.scan()
cust_data = response['Items']

# Convert the DynamoDB items to DataFrame
cust_df = spark.createDataFrame(cust_data)

# Display the initial rows 
print("Customer Data from DynamoDB:")
cust_df.show()
print("Transaction Data from S3:")
trans_df.show()

# Calculate last purchase date for each customer
last_purchase_df = (
    trans_df.groupBy("customer_id").agg(max("transaction_date").alias("last_purchase_date"))
)
print("Last purchase date for each customer")
last_purchase_df.show()

# Join with customer data to get a complete view
cust_act_df = cust_df.join(last_purchase_df, on="customer_id", how="inner")

# Flag customers as inactive if last purchase was over six months ago
six_months_ago = datetime.now() - timedelta(days=180)
six_months_ago_str = six_months_ago.strftime("%Y-%m-%d")

inactive_cust_df = cust_act_df.withColumn(
    "inactive",(datediff(current_date(), col("last_purchase_date")) > 180)
).select("customer_id", "inactive")

print("customers status")
inactive_cust_df.show()

# Filter out inactive customers
inactive_customers = inactive_cust_df.filter(col("inactive") == True).collect()
active_customers = inactive_cust_df.filter(col("inactive") == False).collect()

# Update status in DynamoDB
for row in inactive_customers:
    table.update_item(
        Key={"customer_id": row["customer_id"]},
        UpdateExpression="SET inactive = :inactive",
        ExpressionAttributeValues={":inactive": {"BOOL": True}} 
    )

# For customers who are active, you can update status as 'active' similarly
for row in active_customers:
    table.update_item(
        Key={"customer_id": row["customer_id"]},
        UpdateExpression="SET status = :status",  
        ExpressionAttributeValues={":status": {"S": "active"}}  
    )

print("Customer status updated in DynamoDB.")
spark.stop()