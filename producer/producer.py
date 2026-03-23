from kafka import KafkaProducer
import json, time, random
from datetime import datetime
from config.settings import *

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate():
    return {
        "timestamp": datetime.now().isoformat(),
        "temperature": random.uniform(20, 40)
    }

while True:
    data = generate()
    producer.send(TOPIC_RAW, data)
    print("Sent:", data)
    time.sleep(1)