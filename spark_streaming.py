from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# schema dữ liệu
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("sensor_id", IntegerType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType())

spark = SparkSession.builder \
    .appName("RealtimeSensorProcessing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# đọc stream từ socket
df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# parse JSON
json_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# xử lý realtime
processed_df = json_df.filter(col("temperature") > 30)

# output ra console
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
