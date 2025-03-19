""" Silver Layer: Transform the bronze layer json format to parquet, check the data consistency by using DropDuplicates and na fill, improving scalability and performance"""
from pyspark.sql import SparkSession
import os
spark = SparkSession.builder.appName("SilverLayer").master("local[*]").getOrCreate()

"""ETL Paths """
bronze_path = "/opt/airflow/dags/medallion_data/bronze_layer.json"
silver_path = "/opt/airflow/dags/medallion_data/silver_layer"

""" Reading and treating raw data"""
df = spark.read.option("multiline", "true").json(bronze_path)
df = df.na.fill({"city": "Unknown"})

"""Check if there are Duplicates"""
df.count()
df1= df.dropDuplicates()
df1.count()
df1.printSchema()

""" Save data partitioned by city"""
df1.write.mode("overwrite").partitionBy("city").parquet(silver_path)
print(f"Parquet has been saved: {silver_path}")
