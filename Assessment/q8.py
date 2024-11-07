# RetailMart wants to analyze churn by monitoring inactive accounts. Load customer profiles 
# from DynamoDB and transaction history from S3, then use PySpark to identify customers 
# with no transactions in the last year and calculate the monthly churn rate, helping inform 
# customer retention strategies.

import boto3
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, year, month, lit, count
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

# Get today's date and calculate the cutoff date (1 year ago)
one_year_ago = datetime.now() - timedelta(days=365)

# Calculate the last transaction date for each customer
last_trans_df = (
    trans_df
    .groupBy("customer_id")
    .agg(max("transaction_date").alias("last_transaction_date"))
)
print("last transaction date for each customer")
last_trans_df.show()

# Join customer data with last transaction data
cust_act_df = cust_df.join(last_trans_df, on="customer_id", how="left")

# Flag customers as inactive if they have no transactions in the last year
inactive_cust_df = cust_act_df.withColumn(
    "inactive", (col("last_transaction_date") < lit(one_year_ago))
)

# Filter for inactive customers
inactive_cust_df = inactive_cust_df.filter(col("inactive") == True)
print("inactive customers")
inactive_cust_df.show()

# Calculate churn rate by month
churn_by_month_df = (
    inactive_cust_df
    .withColumn("churn_year", year(col("last_transaction_date")))
    .withColumn("churn_month", month(col("last_transaction_date")))
    .groupBy("churn_year", "churn_month")
    .agg(count("customer_id").alias("churn_count"))
    .orderBy("churn_year", "churn_month")
)

churn_by_month_df.show()
spark.stop()