from pyspark.sql import SparkSession
import os

# ✅ Set JAVA_HOME here
os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.15.6-hotspot"

# ✅ Optional: Update PATH too if needed (uncommon, but safe)
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]

# 🚀 Initialize Spark
spark = SparkSession.builder \
    .appName("myfirstapp") \
    .getOrCreate()

# 🧾 Sample data
data = [("nagendra", 30), ("Aryan", 28)]
columns = ["Name", "Age"]

# 🔍 Create DataFrame and show
df = spark.createDataFrame(data, columns)
df.show()

# 🧹 Stop Spark
spark.stop()
