docker run -d -p 7077:7077 -p 80:4040 apache/spark-py /opt/spark/bin/pyspark

python ./connect_to_spark.py