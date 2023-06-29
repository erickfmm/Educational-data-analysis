#Import SparkSession
from pyspark.sql import SparkSession
#Create Session

def connect(args):
    spark = SparkSession.builder.master("local[*]").enableHiveSupport().getOrCreate()
    #spark = SparkSession.builder.remote("sc://0.0.0.0:7077").getOrCreate()
    return spark

if __name__ == "__main__":
    spark = connect()
    print("to create some stuff")
    spark.range(1).createTempView("test_view")
    print("Tables:")
    print(spark.catalog.listTables())
    print("end")