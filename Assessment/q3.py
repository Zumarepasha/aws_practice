# The company wants to categorize customers into tiers (e.g., "Bronze," "Silver," "Gold") based on 
# total purchase value. Load transaction data from S3 and customer information from DynamoDB, 
# join them, then use PySpark to calculate total spending per customer. Assign categories based 
# on thresholds and save results back to DynamoDB

import boto3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when

spark = SparkSession.builder.appName("CustomerAnalysis")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .config("spark.hadoop.fs.s3a.", "your_access_key_id")\
    .config("spark.hadoop.fs.s3a.", "your_secret_access_key")\
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

# Join DataFrames on customer_id
joined_df = trans_df.join(cust_df, on="customer_id", how="inner")

# Calculate Total Spending 
total_spending_df = joined_df.groupBy("customer_id").agg(sum("amount").alias("total_spent"))

# Categorize Customers based on total spending
tiered_df = total_spending_df.withColumn(
    "Tier",
    when(col("total_spent") >= 5000, "Gold")
    .when((col("total_spent") >= 3000) & (col("total_spent") < 5000), "Silver")
    .otherwise("Bronze")
)

# Save the Results back to DynamoDB
def save_to_dynamodb(row):
    # Pass credentials directly instead of relying on environment variables
    session = boto3.Session(
        aws_access_key_id='your_access_key_id', 
        aws_secret_access_key='your_secret_access_key', 
        region_name='your_region'
    )
    dynamodb = session.resource('dynamodb')

    customer_id_str = str(row['customer_id'])
    
    dynamodb.Table('Tier_table').put_item(
        Item={
            'customer_id': customer_id_str,
            'total_spent': {'N': str(row['total_spent'])},
            'tier': {'S': row['Tier']}
        }
    )

# Write data to DynamoDB
tiered_df.foreach(save_to_dynamodb)

print("Customer categorization completed and saved to DynamoDB")
spark.stop()