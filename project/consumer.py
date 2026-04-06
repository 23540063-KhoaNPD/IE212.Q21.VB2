from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
import time
import socket
import time

def wait_for_kafka(host, port):
    print("⏳ Waiting for Kafka...")
    while True:
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            print("✅ Kafka ready")
            break
        except:
            time.sleep(2)
            print("Wait Kafka")

TOPIC_NAME = "my_dataset_topic"
BOOTSTRAP_SERVERS = "kafka:9092"

# 🔥 WAIT HERE
wait_for_kafka("kafka", 9092)

spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Define schema
schema = StructType() \
    .add("movieId", IntegerType()) \
    .add("title", StringType()) \
    .add("genres", StringType())

# 🔹 Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

# 🔹 Parse JSON
json_df = df.selectExpr("CAST(value AS STRING) as json")

# json_df.writeStream \
#     .format("console") \
#     .option("truncate", False) \
#     .start() \
#     .awaitTermination()

parsed_df = json_df.select(
    from_json(col("json"), schema).alias("data")
).select("data.*")

# 🔹 Start stream
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 50) \
    .start()

query.awaitTermination()