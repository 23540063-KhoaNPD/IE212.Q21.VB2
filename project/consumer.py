from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType

TOPIC_NAME = "my_dataset_topic"
BOOTSTRAP_SERVERS = "kafka:9092"

spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

# Define schema based on your CSV
schema = StructType() \
    .add("column1", StringType()) \
    .add("column2", FloatType()) \
    .add("column3", StringType())  # ví dụ

# 🔹 Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

# 🔹 Parse JSON value
json_df = df.selectExpr("CAST(value AS STRING) as json")
parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")

# 🔹 Show stream
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()