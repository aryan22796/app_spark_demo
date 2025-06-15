# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# # Set Java path if required
# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

# # Set dynamic paths
# base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# input_file = os.path.join(base_dir, 'data', 'input', 'sales.csv')
# output_dir = os.path.join(base_dir, 'data', 'output', 'sales_etl_output')

# # Start Spark session
# spark = SparkSession.builder.appName("ETL_Sales_Local").getOrCreate()

# # Read CSV
# df = spark.read.option("header", True).csv(input_file)

# # Simple ETL: filter Electronics and add total_price = quantity * unit_price
# df_filtered = df.filter(col("category") == "Electronics") \
#     .withColumn("total_price", col("quantity").cast("int") * col("unit_price").cast("float"))

# # Save to Parquet
# df_filtered.write.mode("overwrite").parquet(output_dir)

# spark.stop()
