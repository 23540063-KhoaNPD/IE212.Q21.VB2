from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import MapType, StringType

spark = SparkSession.builder \
    .appName("KafkaDynamicConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# đọc Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my_dataset_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# parse JSON dynamic
parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), MapType(StringType(), StringType())).alias("data"))

# flatten
result = parsed.select("data.*")

query = result.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()