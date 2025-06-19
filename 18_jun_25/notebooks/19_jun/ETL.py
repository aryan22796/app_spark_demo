from pyspark.sql import SparkSession
from pyspark.sql.functions import col ,sum,avg,pandas_udf
import pandas as pd


spark = (
    SparkSession.builder
    .appName("ETL_basic_project")
    .config("spark.executor.cores", "2")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.memoryOverhead", "512m")
    .config("spark.sql.shuffle.partitions","8")
    .config("spark.driver.memory","2g")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
     
    
    .getOrCreate()
)
data = [(i,"Product"+str(i %5 ) , "region"+ str(i%3),i *10.5)  for i in range(1,1000001)]
df= spark.createDataFrame(data , ["sale_id","Product","region","amount"])
# print("all partition ", spark.conf.get("spark.sql.shuffle.partitions"))
df_fil = df.filter(col("amount")>200).select("sale_id","product","region")
spark.sparkContext.setLogLevel("Warn")
print(df_fil.show(10))
print(df_fil.count())