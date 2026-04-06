from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import socket
import time

# --- Cấu hình ---
TOPIC_NAME = "my_dataset_topic"
BOOTSTRAP_SERVERS = "kafka:9092"

# Danh sách các cột tiêu biểu muốn giữ lại để quan sát
SELECTED_COLUMNS = [
    "Timestamp",
    "Src IP", 
    "Src Port", 
    "Dst IP", 
    "Dst Port", 
    "Protocol", 
    "Flow Duration",
    "Tot Fwd Pkts",
    "Tot Bwd Pkts",
    "Label"  # Cột quan trọng nhất để xem kết quả (ddos, normal, etc.)
]

def wait_for_kafka(host, port):
    print("⏳ Đang kiểm tra kết nối Kafka...")
    while True:
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            break
        except:
            time.sleep(2)

wait_for_kafka("kafka", 9092)

spark = SparkSession.builder \
    .appName("FilteredConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. Đọc stream từ Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .load()

# 2. Lấy Schema động từ tin nhắn đầu tiên (như đã làm ở bước trước)
# Ở đây tôi giả định bạn dùng schema_of_json hoặc đã có schema
# Để đơn giản và chính xác, ta sẽ parse toàn bộ rồi select sau
from pyspark.sql.functions import schema_of_json

# Lấy sample để tạo schema
sample_data = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME).load() \
    .selectExpr("CAST(value AS STRING)").limit(1).collect()

if sample_data:
    dynamic_schema = schema_of_json(sample_data[0][0])
    
    # 3. Parse và Lọc cột
    parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_data") \
        .select(from_json(col("json_data"), dynamic_schema).alias("data")) \
        .select("data.*")

    # Xử lý khoảng trắng thừa trong tên cột (nếu có) và lọc
    # Cách này giúp tránh lỗi nếu tên cột trong CSV là " Label" thay vì "Label"
    available_cols = parsed_df.columns
    final_selection = []
    for c in SELECTED_COLUMNS:
        # Tìm cột khớp (không phân biệt khoảng trắng đầu cuối)
        matched = [real_col for real_col in available_cols if real_col.strip() == c]
        if matched:
            final_selection.append(col(f"`{matched[0]}`").alias(c))

    display_df = parsed_df.select(*final_selection)

    # 4. Xuất ra Console
    query = display_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 20) \
        .start()

    print("🚀 Đang hiển thị các cột tiêu biểu...")
    query.awaitTermination()
else:
    print("❌ Chưa có dữ liệu trong Kafka để phân tích Schema.")