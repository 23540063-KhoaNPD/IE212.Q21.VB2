from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import MapType, StringType

spark = SparkSession.builder \
    .appName("KafkaSparkPipeline") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# đọc từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "my_dataset_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# parse JSON dynamic
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), MapType(StringType(), StringType())).alias("data"))

# flatten
result = parsed.select("data.*")

# ví dụ xử lý (nếu có cột temperature)
# result = result.withColumn("temperature", col("temperature").cast("double"))

# output console
query = result.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()