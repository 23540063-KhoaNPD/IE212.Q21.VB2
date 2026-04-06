import pandas as pd
import json
import time
from kafka import KafkaProducer
import os

# path file CSV
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(BASE_DIR, 'data', 'data.csv')

BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC_NAME = 'my_dataset_topic'

producer = KafkaProducer(
    bootstrap_servers=[BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(3, 7, 0),
    acks='all',
    retries=5
)

def send_dataset(file_path):
    df = pd.read_csv(file_path)
    print(f"Gửi {len(df)} dòng...")

    for index, row in df.iterrows():
        data_row = row.to_dict()
        producer.send(TOPIC_NAME, value=data_row)

        if index % 100 == 0:
            print(f"Đã gửi: {index}")

        # giả lập realtime
        time.sleep(0.05)

    producer.flush()
    print("Xong!")

if __name__ == "__main__":
    send_dataset(file_path)