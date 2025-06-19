# # Topic 4: PySpark Driver and Executor Configuration - Code Examples

from pyspark.sql import SparkSession

# # ------------------------------------------
# # ✅ Example 1: Static Resource Allocation
# # ------------------------------------------
# spark_static = (
#     SparkSession.builder
#     .appName("StaticConfigExample")
#     .config("spark.executor.cores", "4")
#     .config("spark.executor.memory", "8g")
#     .config("spark.executor.memoryOverhead", "2g")
#     .getOrCreate()
# )

# print("\n===> Static Spark Session Started")
# spark_static.range(100000000).selectExpr("id * 2 as value").show(5)
# spark_static.stop()

# # ------------------------------------------
# # ✅ Example 2: Dynamic Resource Allocation with Shuffle Service
# # ------------------------------------------
# spark_dynamic = (
#     SparkSession.builder
#     .appName("DynamicAllocationExample")
#     .config("spark.dynamicAllocation.enabled", "true")
#     .config("spark.dynamicAllocation.minExecutors", "2")
#     .config("spark.dynamicAllocation.maxExecutors", "10")
#     .config("spark.dynamicAllocation.initialExecutors", "4")
#     .config("spark.shuffle.service.enabled", "true")
#     .getOrCreate()
# )

# print("\n===> Dynamic Spark Session Started")
# spark_dynamic.range(50000000).repartition(200).groupBy().count().show()
# spark_dynamic.stop()

# # # ------------------------------------------
# # # ✅ Example 3: Realistic Use Case - Streaming Load Handling
# # # ------------------------------------------
# # # Scenario: A streaming job that receives variable input load. Dynamic resource allocation helps manage resources efficiently.

# # spark_streaming_example = (
# #     SparkSession.builder
# #     .appName("StreamingResourceAdaptiveJob")
# #     .config("spark.dynamicAllocation.enabled", "true")
# #     .config("spark.dynamicAllocation.minExecutors", "1")
# #     .config("spark.dynamicAllocation.maxExecutors", "15")
# #     .config("spark.dynamicAllocation.initialExecutors", "3")
# #     .config("spark.shuffle.service.enabled", "true")
# #     .getOrCreate()
# # )

# # print("\n===> Streaming Use Case Session Started")
# # spark_streaming_example.range(1000000).repartition(100).groupBy().count().show()
# # spark_streaming_example.stop()

# # # ------------------------------------------
# # # 🔍 Notes:
# # # - Dynamic allocation automatically scales executors based on workload.
# # # - Shuffle service is required to preserve shuffle files when executors are deallocated.
# # # - These configurations are especially useful in streaming or multi-stage jobs.
# # # - Monitor executors in Spark UI > Executors tab.

# # # End of Topic 4 Code Examples


#tunning performamce , cuostom log , enviroment variable
from pyspark.sql.functions import col
spark = (
    SparkSession.builder
    .appName("log propr")
    .config("spark.executor.cores", "2")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.memoryOverhead", "1g")
    .config("spark.sql.shuffle.partitions","10")
    .getOrCreate()
)
print("all partition ", spark.conf.get("spark.sql.shuffle.partitions"))
spark.sparkContext.setLogLevel("Warn")#pep8, pyspark, python,github(git config --global user.name "",user.email)

data = [(1, "Alice",20000), (2, "Bob",30000), (3, "Cathy",45000), (4, "David",50000)]
df = spark.createDataFrame(data, ["id", "name","Salary"])
#df = spark.read.parquet("file/PAth")

b_df = df.withColumn("bonus",col("Salary")*.10)
b_df = df.filter("Salary >2500")
b_df.show()

