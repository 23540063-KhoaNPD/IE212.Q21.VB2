import pandas as pd
import json
import time
import random
import os
import numpy as np
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# Cấu hình đường dẫn
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(BASE_DIR, 'data', 'final_dataset.csv')

BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "my_dataset_topic"

print("--- Khởi động Kafka Producer ---")

def wait_for_kafka(delay=2):
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            print("✅ Kafka broker đã sẵn sàng!")
            return admin
        except NoBrokersAvailable:
            print(f"⚠️ Kafka chưa sẵn sàng, đang thử lại...")
            time.sleep(delay)

def create_topic(admin_client, topic_name):
    try:
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
        print(f"✅ Đã tạo Topic '{topic_name}'")
    except TopicAlreadyExistsError:
        print(f"ℹ️ Topic '{topic_name}' đã tồn tại")

def create_producer(retries=10, delay=5):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                # Nén dữ liệu vì 85 cột có thể tạo payload lớn
                compression_type='gzip',
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=5,
                # Tăng kích thước batch để tối ưu hiệu năng
                batch_size=16384, 
                linger_ms=10
            )
            print("✅ Kết nối Producer thành công!")
            return producer
        except NoBrokersAvailable:
            print(f"❌ Không thể kết nối Kafka, thử lại {i+1}/{retries} sau {delay}s...")
            time.sleep(delay)
    raise Exception("Không thể kết nối tới Kafka sau nhiều lần thử.")

def send_dataset(file_path, producer):
    if not os.path.exists(file_path):
        print(f"❌ Lỗi: Không tìm thấy file tại {file_path}")
        return

    print(f"🚀 Bắt đầu gửi dữ liệu từ: {file_path}")
    
    while True: # Vòng lặp vô hạn nếu bạn muốn giả lập streaming liên tục
        # Đọc theo chunk để tiết kiệm RAM (ví dụ mỗi lần 500 dòng)
        chunks = pd.read_csv(file_path, chunksize=500)
        
        total_sent = 0
        for chunk in chunks:
            # Xử lý giá trị NaN/NaT (JSON không hỗ trợ NaN chuẩn của Python)
            chunk = chunk.replace({np.nan: None})
            
            for _, row in chunk.iterrows():
                data_row = row.to_dict()
                
                try:
                    # Gửi dữ liệu không đồng bộ để đạt tốc độ cao hơn
                    producer.send(TOPIC_NAME, value=data_row).add_errback(
                        lambda e: print(f"❌ Lỗi Kafka: {e}")
                    )
                    total_sent += 1
                    
                    if total_sent % 100 == 0:
                        print(f"📊 Đã gửi: {total_sent} dòng")
                    
                    # Điều tiết tốc độ gửi (Gợi ý: 0.1s đến 0.5s cho 85 cột)
                    time.sleep(random.uniform(0.1, 0.5))
                    
                except Exception as e:
                    print(f"❌ Lỗi khi xử lý dòng {total_sent}: {e}")

        producer.flush()
        print("🔄 Đã gửi xong toàn bộ file. Khởi động lại vòng lặp...")
        time.sleep(5) 

if __name__ == "__main__":
    admin = wait_for_kafka()
    create_topic(admin, TOPIC_NAME)
    producer = create_producer()
    
    try:
        send_dataset(file_path, producer)
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng Producer.")
    finally:
        producer.close()