import pandas as pd
import json
import time
import random
import os
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# Path CSV
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(BASE_DIR, 'data', 'final_dataset.csv')

BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = "my_dataset_topic"

print("Start Kafka producer")

# 🔹 Wait Kafka broker ready
def wait_for_kafka(delay=2):
    while True:
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            print("✅ Kafka broker is ready!")
            return admin
        except NoBrokersAvailable:
            print("Kafka not ready, retrying ...")
            time.sleep(delay)

# 🔹 Create topic if not exist
def create_topic(admin_client, topic_name):
    try:
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
        print(f"✅ Topic '{topic_name}' created")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists")

# 🔹 Create producer with retry
def create_producer(retries=10, delay=5):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=5
            )
            print("✅ Kafka producer connected!")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready, retry in {delay}s... ({i+1}/{retries})")
            time.sleep(delay)
    raise Exception("Cannot connect to Kafka after retries")

# 🔹 Send CSV dataset
def send_dataset(file_path, producer):
    df = pd.read_csv(file_path)

    # 🔹 Lấy thông tin cột tự động
    columns = df.columns.tolist()
    num_columns = len(columns)
    print(f"Dataset has {num_columns} columns: {columns}")
    print(f"Sending {len(df)} rows...")

    while True:
        for index, row in df.iterrows():
            # Tạo dict tự động từ tên cột
            data_row = {col: row[col] for col in columns}
            print(json.dumps(data_row))
            
            future = producer.send(TOPIC_NAME, value=data_row)
            try:
                future.get(timeout=10)
            except Exception as e:
                print(f"❌ Failed to send row {index}: {e}")

            if index % 100 == 0 and index != 0:
                print(f"Sent: {index} rows")

            # Delay ngẫu nhiên giữa các row
            time.sleep(random.uniform(1, 3))

        producer.flush()
        
    print("✅ Done sending dataset!")

if __name__ == "__main__":
    admin = wait_for_kafka()
    create_topic(admin, TOPIC_NAME)
    producer = create_producer()
    send_dataset(file_path, producer)