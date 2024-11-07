# RetailMart wants to add insights like "favorite product category" to each customer profile. 
# Load transactions from S3 and customer data from DynamoDB, use PySpark to analyze purchases 
# per category, determine the most frequently bought category for each customer, and write this 
# back to DynamoDB.

import boto3
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, row_number
from pyspark.sql.window import Window

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

# Calculate purchase frequency per category for each customer
category_count_df = (
    trans_df.groupBy("customer_id", "product_category").agg(count("product_category").alias("purchase_count"))
)
print("purchase frequency per category for each customer")
category_count_df.show()

# Determine the most frequently bought category for each customer
favorite_category_df = (
    category_count_df
    .withColumn("rank", row_number().over(Window.partitionBy("customer_id").orderBy(desc("purchase_count"))))
    .filter(col("rank") == 1)
    .select("customer_id", "product_category")
)
print("most frequently bought category for each customer")
favorite_category_df.show()

# Collect and update each customer's favorite category in DynamoDB
for row in favorite_category_df.collect():
    table.update_item(
        Key={"customer_id": row["customer_id"]},
        UpdateExpression="SET favorite_category = :category",
        ExpressionAttributeValues={":category": row["product_category"]}
    )

print("Favorite product category updated for each customer in DynamoDB.")
spark.stop()