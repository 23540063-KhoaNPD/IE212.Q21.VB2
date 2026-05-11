from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("LogMonitoring") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("level", StringType()),
    StructField("service", StringType()),
    StructField("message", StringType()),
    StructField("user_id", IntegerType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs") \
    .load()

# Parse JSON
logs = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp"))

# Watermark
logs = logs.withWatermark("timestamp", "2 minutes")

# Aggregation
agg = logs.groupBy(
    window(col("timestamp"), "1 minute"),
    col("level")
).count()

# Detect ERROR spike
error_df = agg.filter((col("level") == "ERROR") & (col("count") > 20))

# Output to console (debug)
query1 = agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Output alert
query2 = error_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

spark.streams.awaitAnyTermination()