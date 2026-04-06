from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from config.settings import *
from schema import schema
from processing import process

spark = SparkSession.builder \
    .appName("RealtimePipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# đọc Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC_RAW) \
    .load()

# parse JSON
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# xử lý
result = process(parsed)

# output -> Kafka
query = result.selectExpr("CAST(temperature AS STRING) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("topic", TOPIC_PROCESSED) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start()

query.awaitTermination()