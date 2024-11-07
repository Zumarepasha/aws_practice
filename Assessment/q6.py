# RetailMart wants to monitor unusual spending patterns as part of fraud detection. 
# Load sales transactions from S3 and join with customer data from DynamoDB. Calculate the 
# average spending per transaction and flag any transactions that exceed a certain threshold 
# as anomalies, then log these flags in DynamoDB.

import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when

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
anomaly_table = dynamodb.Table('AnomalyTable')

response = table.scan()
cust_data = response['Items']

# Convert the DynamoDB items to DataFrame
cust_df = spark.createDataFrame(cust_data)

# Load data from s3
sales_input = "s3a://zp-demo-buck/source/sales.csv"
sales_df = spark.read.csv(sales_input, header=True, inferSchema=True)

print("Customer Data from DynamoDB:")
cust_df.show()
print("Transaction Data from S3:")
sales_df.show()

# Calculate average spending per transaction for each customer
avg_spending_df = (
    sales_df.groupBy("customer_id").agg(avg("purchase_amount").alias("avg_spending"))
)

# Join sales data with average spending per customer
sales_with_avg_df = sales_df.join(avg_spending_df, on="customer_id", how="left")

# Define a threshold (e.g., 1.5x the average spending) to flag anomalies
threshold_multiplier = 1.5
anomalies_df = sales_with_avg_df.withColumn(
    "is_anomaly",
    when(col("purchase_amount") > (col("avg_spending") * threshold_multiplier), True).otherwise(False)
).filter(col("is_anomaly") == True)

print("Anamolies DF:")
anomalies_df.show()

# Collect anomalies and log them in DynamoDB
for row in anomalies_df.collect():
    anomaly_table.put_item(
        Item={
            "customer_id": row["customer_id"],
            "transaction_id": row["transaction_id"],
            "purchase_amount": row["purchase_amount"],
            "avg_spending": row["avg_spending"],
            "is_anomaly": row["is_anomaly"]
        }
    )

print("Anomalies flagged and logged in DynamoDB.")
spark.stop()