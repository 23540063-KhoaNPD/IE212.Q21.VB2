import socket
import time
import json
import random
from datetime import datetime

HOST = "localhost"
PORT = 9999

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))
server.listen(1)

print(f"Listening on {HOST}:{PORT}...")
conn, addr = server.accept()
print("Connected by", addr)

def generate_data():
    return {
        "timestamp": datetime.now().isoformat(),
        "sensor_id": random.randint(1, 5),
        "temperature": round(random.uniform(20, 40), 2),
        "humidity": round(random.uniform(30, 90), 2)
    }

while True:
    data = generate_data()
    msg = json.dumps(data) + "\n"
    conn.send(msg.encode("utf-8"))
    print("Sent:", data)
    time.sleep(1)
