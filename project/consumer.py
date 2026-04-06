from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, schema_of_json
from kafka import KafkaAdminClient
import socket
import time
from pyspark.sql.functions import trim, lower

# --- Cấu hình ---
TOPIC_NAME = "my_dataset_topic"
BOOTSTRAP_SERVERS = "kafka:9092"
SELECTED_COLUMNS = [
    "Timestamp", "Src IP", "Src Port", "Dst IP", "Dst Port", 
    "Protocol", "Flow Duration", "Tot Fwd Pkts", "Tot Bwd Pkts", "Label"
]

def wait_for_kafka(host, port):
    print(f"⏳ 1. Kiểm tra kết nối TCP tới {host}:{port}...")
    while True:
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            print("✅ Kafka Broker đã mở cổng kết nối.")
            break
        except:
            time.sleep(2)

def wait_for_topic(topic_name, servers):
    print(f"⏳ 2. Kiểm tra sự tồn tại của topic '{topic_name}'...")
    while True:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=servers)
            topics = admin_client.list_topics()
            admin_client.close()
            if topic_name in topics:
                print(f"✅ Topic '{topic_name}' đã sẵn sàng!")
                break
            else:
                print(f"⚠️ Topic '{topic_name}' chưa được tạo. Đang đợi Producer...")
        except Exception as e:
            print(f"❌ Lỗi khi kết nối AdminClient: {e}")
        time.sleep(3)

# --- KHỞI CHẠY ---
wait_for_kafka("kafka", 9092)
wait_for_topic(TOPIC_NAME, BOOTSTRAP_SERVERS)

spark = SparkSession.builder \
    .appName("FilteredConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 3. Vòng lặp đợi dữ liệu mẫu để suy luận Schema
dynamic_schema = None
print("⏳ 3. Đợi dữ liệu mẫu đầu tiên từ Kafka để học Schema...")

while dynamic_schema is None:
    try:
        sample_data = spark.read.format("kafka") \
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
            .option("subscribe", TOPIC_NAME) \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .limit(1) \
            .collect()

        if len(sample_data) > 0:
            json_str = sample_data[0][0]
            dynamic_schema = schema_of_json(json_str)
            print("✅ Đã học được Schema từ dữ liệu thực tế.")
        else:
            print("ℹ️ Topic trống. Đang đợi Producer gửi tin nhắn đầu tiên...")
            time.sleep(5)
    except Exception as e:
        print(f"🔄 Đang khởi tạo lại bộ đọc mẫu: {e}")
        time.sleep(5)

# ... (Các phần import và wait_for giữ nguyên) ...

# 4. Bắt đầu Stream thực tế
print("🚀 4. Bắt đầu xử lý luồng dữ liệu...")
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

# BƯỚC A: Parse JSON
parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), dynamic_schema).alias("data")) \
    .select("data.*")

# BƯỚC B: Làm sạch tên cột (loại bỏ khoảng trắng trong tên cột 85 cột)
# Ví dụ: " Label" -> "Label"
cleaned_df = parsed_df.select([col(f"`{c}`").alias(c.strip()) for c in parsed_df.columns])

# BƯỚC C: Lọc dữ liệu (Sử dụng cleaned_df đã chuẩn hóa tên cột)
# Dùng trim và lower để loại bỏ mọi biến thể của " ddos ", "DDOS"
filtered_df = cleaned_df.filter(
    (trim(lower(col("Label"))) == "ddos")
)

# BƯỚC D: Chọn các cột tiêu biểu từ dữ liệu ĐÃ LỌC (filtered_df)
final_selection = []
available_cols = filtered_df.columns

for c in SELECTED_COLUMNS:
    # Vì đã strip() ở Bước B, ta so sánh trực tiếp
    if c in available_cols:
        final_selection.append(col(f"`{c}`"))

if not final_selection:
    print("❌ Không tìm thấy cột nào trong danh sách SELECTED_COLUMNS!")
    display_df = filtered_df 
else:
    display_df = filtered_df.select(*final_selection)

# 5. Xuất ra Console
# Dùng vertical=True nếu bạn vẫn thấy bảng bị vỡ do nhiều cột
query = display_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .start()

query.awaitTermination()