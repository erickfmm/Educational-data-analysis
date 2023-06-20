#Import SparkSession
from pyspark.sql import SparkSession
#Create Session
#spark = SparkSession.builder.getOrCreate()
spark = SparkSession.builder.master("local[*]").getOrCreate()
#spark = SparkSession.builder.remote("sc://0.0.0.0:7077").getOrCreate()

print("to create some stuff")
spark.range(1).createTempView("test_view")
print("Tables:")
print(spark.catalog.listTables())
print("end")