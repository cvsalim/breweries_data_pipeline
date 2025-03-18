# Silver Layer: Transform the bronze layer to improve scalability and performance
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder.appName("SilverLayer").getOrCreate()

# ETL Paths
bronze_path = "medallion_data/bronze_layer.json"
silver_path = "medallion_data/silver_layer"

# Reading and treating raw data
df = spark.read.option("multiline", "true").json(bronze_path)
df = df.na.fill({"city": "Unknown"})
#Check if there are Duplicates
df.count()
df1= df.dropDuplicates()
df1.count()
df1.printSchema()

# Save data partitioned by city
df1.write.mode("overwrite").partitionBy("city").parquet(silver_path)
print(f"Parquet has been saved: {silver_path}")
