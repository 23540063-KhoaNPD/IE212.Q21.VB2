import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

levels = ["INFO", "WARN", "ERROR"]
services = ["auth-service", "payment-service", "order-service"]

def generate_log():
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "level": random.choices(levels, weights=[0.7, 0.2, 0.1])[0],
        "service": random.choice(services),
        "message": "Sample log message",
        "user_id": random.randint(1, 1000)
    }

while True:
    log = generate_log()

    # 🔥 simulate spike ERROR
    if random.random() < 0.05:
        log["level"] = "ERROR"

    producer.send("logs", log)
    print(log)

    time.sleep(0.5)