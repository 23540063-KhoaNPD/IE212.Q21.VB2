import pandas as pd
import json
import time
from kafka import KafkaProducer
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(BASE_DIR, '..', 'data','data.csv')
# 1. Cấu hình Kafka
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'my_dataset_topic'

# 2. Khởi tạo Producer (Đã tối ưu cho Kafka 3.x KRaft)
producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    # --- CÁC THAM SỐ QUAN TRỌNG CẦN THÊM ---
    api_version=(3, 7, 0),    # Phải khớp với bản apache/kafka:3.7.0 bạn đã pull
    acks='all',               # Broker phải xác nhận đã nhận dữ liệu
    retries=5,                # Thử lại nếu gặp lỗi mạng tạm thời
    max_block_ms=60000        # Chờ tối đa 60s nếu Kafka chưa sẵn sàng
)

def send_dataset(file_path):
    try:
        # Đọc dataset bằng pandas
        df = pd.read_csv(file_path)
        print(f"Bắt đầu gửi {len(df)} bản ghi...")

        for index, row in df.iterrows():
            # Chuyển dòng hiện tại thành dictionary
            data_row = row.to_dict()
            
            # Gửi vào Kafka
            producer.send(TOPIC_NAME, value=data_row)
            
            if index % 100 == 0:
                print(f"Đã gửi: {index} dòng")
            
            # (Tùy chọn) Nghỉ một chút để mô phỏng real-time
            # time.sleep(0.1) 

        producer.flush() # Đảm bảo toàn bộ dữ liệu đã được gửi đi
        print("Hoàn thành!")

    except Exception as e:
        print(f"Lỗi: {e}")

if __name__ == "__main__":
    send_dataset(file_path) # Thay bằng đường dẫn file của bạn