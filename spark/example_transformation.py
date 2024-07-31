from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Initialize Spark session
spark = SparkSession.builder.appName("S3 Data Aggregation").getOrCreate()

# Define S3 input and output paths
input_path = "s3://your-input-bucket/sales_data.csv"
output_path = "s3://your-output-bucket/aggregated_sales_data/"

# Read data from S3
sales_data = spark.read.csv(input_path, header=True, inferSchema=True)

# Perform aggregation: calculate total sales per category using DataFrame API
aggregated_data = (
    sales_data.withColumn("total_sales", col("quantity") * col("price"))
    .groupBy("category")
    .agg(sum("total_sales").alias("total_sales"))
)

#or 
# Create a temporary view of the data
sales_data.createOrReplaceTempView("sales")

# Perform aggregation using SQL query
aggregated_data = spark.sql("""
    SELECT category, SUM(quantity * price) AS total_sales
    FROM sales
    GROUP BY category
""")

# Write aggregated data back to S3
aggregated_data.write.csv(output_path, header=True)

# Stop the Spark session
spark.stop()
  