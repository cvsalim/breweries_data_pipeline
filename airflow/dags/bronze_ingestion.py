""" Bronze Layer: Extracting data from the API and storing it in JSON"""
from pyspark.sql import SparkSession
import requests
import json
import os

spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()
API_URL = "https://api.openbrewerydb.org/breweries"
bronze_path = "medallion_data"
os.makedirs(bronze_path, exist_ok=True)

""" Extract data"""
response = requests.get(API_URL)
data = response.json()

# print(data)  # Activate this command to consult the output

""" Save the raw layer as is"""
with open(f"{bronze_path}/bronze_layer.json", "w") as f:
    json.dump(data, f, indent=4)

print(f"Bronze data has been stored at: {bronze_path}/bronze_layer.json")
