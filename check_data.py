from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckKafkaData") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Tắt bớt log rác để dễ quan sát dữ liệu
spark.sparkContext.setLogLevel("ERROR")

print("\n🚀 Đang kết nối tới Kafka để lấy dữ liệu...")

try:
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "my_dataset_topic") \
        .option("startingOffsets", "earliest") \
        .load()

    # Chuyển dữ liệu từ Binary sang String để hiển thị nội dung JSON
    print("✅ Đã kết nối thành công! Dữ liệu mới nhất:")
    df.selectExpr("CAST(value AS STRING) as data").show(10, truncate=False)

except Exception as e:
    print(f"❌ Lỗi: {e}")

spark.stop()