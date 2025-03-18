# Gold Layer: Get the silver data and connect with the Business needs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder.appName("GoldLayer").getOrCreate()

# ETL Paths
silver_path = "medallion_data/silver_layer"
gold_path = "medallion_data/gold_layer"
os.makedirs(gold_path, exist_ok=True)

# Load silver data
silver_df = spark.read.parquet(silver_path)

# Aggregate breweries by state
agg_df = silver_df.groupBy("brewery_type", "state").count()

# Salvar the gold data
agg_df.write.mode("overwrite").parquet(gold_path)

print("Gold data has been stored")

#agg_df.show() #Run this command if you want to see the Dataframe result
