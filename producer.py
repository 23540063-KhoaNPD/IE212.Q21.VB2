from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_data():
    return {
        "timestamp": datetime.now().isoformat(),
        "temperature": random.uniform(20, 40)
    }

while True:
    data = generate_data()
    producer.send('sensor-data', data)
    print("Sent:", data)
    time.sleep(1)
