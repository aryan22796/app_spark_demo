from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("log propr")
    .config("spark.executor.cores", "2")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.memoryOverhead", "512m")
    .config("spark.sql.shuffle.partitions","10")
    .config("spark.driver.memory","1g")
    .getOrCreate()
)
print("all partition ", spark.conf.get("spark.sql.shuffle.partitions"))
spark.sparkContext.setLogLevel("Warn")#pep8, pyspark, python,github(git config --global user.name "",user.email)

# data = [(1, "Alice",20000), (2, "Bob",30000), (3, "Cathy",45000), (4, "David",50000)]
# df = spark.createDataFrame(data, ["id", "name","Salary"])
# #df = spark.read.parquet("file/PAth")

# b_df = df.withColumn("bonus",col("Salary")*.10)
# b_df = df.filter("Salary >2500")
# b_df.show()

df = spark.range(0,10_000_000)
df_t = df.withColumn("squared",df["id"]*df["id"])
print("total value : ", df_t.count())
spark.stop()