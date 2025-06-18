from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MiniProject") \
    .master("local[*]") \
    .getOrCreate()

df = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
    ["id", "name"]
)

df.show()

spark.stop()
