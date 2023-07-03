#Import SparkSession
from pyspark.sql import SparkSession
#Create Session
import logging


def connect(args):
    logger = logging.getLogger('root')
    logger.debug("To connect to spark")
    spark = SparkSession.builder.master("local[*]")\
    .config("spark.executor.memory", "70g")\
     .config("spark.driver.memory", "50g")\
     .config("spark.memory.offHeap.enabled",True)\
     .config("spark.memory.offHeap.size","16g").enableHiveSupport().getOrCreate()
    #spark = SparkSession.builder.remote("sc://0.0.0.0:7077").getOrCreate()

    logger.debug("Spark connected")
    return spark

if __name__ == "__main__":
    spark = connect()
    print("to create some stuff")
    spark.range(1).createTempView("test_view")
    print("Tables:")
    print(spark.catalog.listTables())
    print("end")