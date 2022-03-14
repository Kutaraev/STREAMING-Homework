import os
import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from kafka import KafkaConsumer

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3 pyspark-shell'

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# consumer = KafkaConsumer('stream')


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667") \
  .option("subscribe", "stream") \
  .load()


df_query = df.selectExpr("CAST(value AS STRING)")
cleaned_df = df_query.withColumn('value', regexp_replace('value', '"', ''))


df1 = cleaned_df \
    .writeStream \
    .format("csv")\
    .option("escape", "")\
    .option("quote", "")\
    .option("format", "append")\
    .trigger(processingTime = "60 seconds")\
    .option("path", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/stream_to_csv/")\
    .option("checkpointLocation", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/stream_to_csv/checkpoint/")\
    .outputMode("append")\
    .start()

df1.awaitTermination()
