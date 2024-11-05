from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("PySpark S3 Example")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
.config("spark.hadoop.fs.s3a.", "YOUR_KEY")\
.config("spark.hadoop.fs.s3a.", "YOUR_KEY")\
.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")\
.config("spark.hadoop.fs.s3a.path.style.access", "true")\
.config("spark.hadoop.fs.s3a.metastore.metrics.enabled", "false") \
.config("spark.hadoop.io.native.lib.available", "false")\
.config("spark.executor.memory", "4g").config("spark.driver.memory", "4g").getOrCreate()

# Corrected S3 path
s3_input_path = "s3a://scriptbkt/2015Q1.csv"
s3_output_path = "s3a://out-script-bkt/path/to/output-file.csv"

# Read the input data from S3
df = spark.read.csv(s3_input_path, header=True, inferSchema=True)
df.show()

# Filter data for 'Casual' subscription type
filtered_df = df.filter(col("Subscription Type") == "Casual")
filtered_df.show()

# Write the filtered data back to S3
filtered_df.write.csv(s3_output_path, header=True, mode="overwrite")
spark.stop()

