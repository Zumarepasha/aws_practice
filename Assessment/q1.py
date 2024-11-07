# As a Data Engineer at RetailMart, you’re tasked with performing a comprehensive Customer Sales Analysis 
# to provide insights into purchasing patterns and product performance. First, you initialize a 
# Spark session named "CustomerAnalysis" to handle large datasets. Then, you load order data 
# from a CSV file stored in S3 and display the initial rows to verify the data. Focusing on 
# high-value purchases, you filter orders with an amount over ₹1,000, then add a discounted_price column 
# reflecting a 10% discount on the original price. Next, you group the sales data by product_category 
# to calculate total sales per category, offering insights into the top-selling products. 
# To build a complete customer view, you join the customer and order DataFrames on customer_id. 
# Additionally, you analyze employee tenure by adding a years_of_experience column based on joining_date 
# in the employee DataFrame. Finally, you save the cleaned and aggregated sales data back to S3 in 
# Parquet format for efficient storage and future analysis, equipping RetailMart with actionable 
# insights for strategic decision-making.

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year
from pyspark.sql.types import IntegerType

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

s3_input_path = "s3a://zp-demo-buck/source/Order_data.csv"

df = spark.read.csv(s3_input_path, header=True, inferSchema=True)
df.show()

# Filtering Data
filtered_df = df.filter(col("amount") > 1000)
filtered_df.show()

# Adding extra column
filtered_df = filtered_df.withColumn("discounted_price", col("amount") * 0.9)
filtered_df.show()

# Grouping data for total sales
Grouped_sales_df = filtered_df.groupBy("product_category") \
    .agg({"discounted_price": "sum"}) \
    .withColumnRenamed("sum(discounted_price)", "total_sales")

Grouped_sales_df.show()

# join customers and orders df
customers_input = "s3a://zp-demo-buck/source/Customers.csv"
customers_df = spark.read.csv(customers_input, header=True, inferSchema=True)

joined_df = customers_df.join(filtered_df, on='customer_id')
joined_df.show(10)

# calculating Employee experience
emp_input = "s3a://zp-demo-buck/source/Employees.csv"
emp_df = spark.read.csv(emp_input, header=True, inferSchema=True)

cur_year = datetime.now().year
emp_df = emp_df.withColumn("years_of_experience", (cur_year - year(col("joining_date"))).cast(IntegerType()))

emp_df.show()

# Save data back to S3 in Parquet format
output_path = "s3a://zp-demo-buck/target/"
customers_df.write.parquet(output_path + "customer_sales", mode="overwrite")
customers_df.write.parquet(output_path + "category_sales", mode="overwrite")

print("Data processing complete. Output saved to S3.")
spark.stop()