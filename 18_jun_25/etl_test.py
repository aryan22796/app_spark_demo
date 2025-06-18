from pyspark.sql import SparkSession
import pandas as pd

# JDBC connection properties
pg_url = "jdbc:postgresql://localhost:5432/demo"
pg_properties = {
    "user": "postgres",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# Initialize Spark
spark = SparkSession.builder \
    .appName("Basic ETL") \
    .config("spark.jars", "file:///E:/demo/postgresql-42.7.3.jar") \
    .getOrCreate()

# Extract: Read data from PostgreSQL
df = spark.read.jdbc(url=pg_url, table="employees", properties=pg_properties)

# Transform: Example - filter employees with salary > 55000
transformed_df = df.filter(df.salary > 55000)

# Load: Save to CSV and Parquet
transformed_df.write.mode("overwrite").csv("file:///E:/demo/output/employees_csv", header=True)
transformed_df.write.mode("overwrite").parquet("file:///E:/demo/output/employees_parquet")

spark.stop()
