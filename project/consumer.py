import socket
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import MapType, StringType

# 🔹 Config
TOPIC_NAME = "my_dataset_topic"
BOOTSTRAP_SERVERS = "kafka:9092"

# 🔹 Wait Kafka broker ready
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
            print("Waiting for Kafka...")

wait_for_kafka("kafka", 9092)

# 🔹 Spark session
spark = SparkSession.builder \
    .appName("KafkaDynamicConsumer") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 🔹 Read stream from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

# 🔹 Convert Kafka value to string
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

# 🔹 Parse JSON dynamically using MapType
parsed_df = json_df.select(
    from_json(col("json_str"), MapType(StringType(), StringType())).alias("data")
)

# Ghi trực tiếp MapType
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()