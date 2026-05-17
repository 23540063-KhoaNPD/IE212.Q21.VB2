from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, schema_of_json
from kafka import KafkaAdminClient
import socket
import time
import asyncio
import websockets
import json

# --- Cấu hình ---
TOPIC_NAME = "my_dataset_topic"
BOOTSTRAP_SERVERS = "kafka:9092"
SOCKET_URI = "ws://192.168.100.246:9999"

SELECTED_COLUMNS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "nameDest",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud"
]

# ================= SOCKET =================

async def send_batch_async(rows):
    try:
        async with websockets.connect(SOCKET_URI) as ws:
            print(f"📡 Sending {len(rows)} records to socket...")

            for row in rows:
                await ws.send(json.dumps(row))
                await asyncio.sleep(0.01)  # tránh spam

    except Exception as e:
        print("❌ Socket error:", e)

def send_batch(df, epoch_id):
    rows = [row.asDict() for row in df.collect()]
    asyncio.run(send_batch_async(rows))

# ================= KAFKA =================

def wait_for_kafka(host, port):
    print(f"[WAIT] Checking TCP connection to {host}:{port}...")
    while True:
        try:
            s = socket.create_connection((host, port), timeout=2)
            s.close()
            print("✅ Kafka ready")
            break
        except:
            time.sleep(2)

def wait_for_topic(topic_name, servers):
    print(f"[WAIT] Checking topic '{topic_name}'...")
    while True:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=servers)
            topics = admin_client.list_topics()
            admin_client.close()

            if topic_name in topics:
                print(f"✅ Topic '{topic_name}' ready")
                break
            else:
                print("⏳ Waiting producer...")
        except Exception as e:
            print("❌ Kafka error:", e)

        time.sleep(3)

# ================= START =================

wait_for_kafka("kafka", 9092)
wait_for_topic(TOPIC_NAME, BOOTSTRAP_SERVERS)

spark = SparkSession.builder \
    .appName("SocketConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ====== LẤY SCHEMA ======

dynamic_schema = None
print("⏳ Waiting sample data...")

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
            print("✅ Schema loaded")
        else:
            time.sleep(5)

    except Exception as e:
        print("Retry schema:", e)
        time.sleep(5)

# ====== STREAM ======

print("🚀 Start streaming...")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), dynamic_schema).alias("data")) \
    .select("data.*")

cleaned_df = parsed_df.select([
    col(f"`{c}`").alias(c.strip()) for c in parsed_df.columns
])

# KHÔNG FILTER (vì dataset bạn không có Label)
filtered_df = cleaned_df

# SELECT COLUMN
final_cols = [col(c) for c in SELECTED_COLUMNS if c in filtered_df.columns]

display_df = filtered_df.select(*final_cols)

# ====== OUTPUT ======

query = display_df.writeStream \
    .outputMode("append") \
    .foreachBatch(send_batch) \
    .start()

query.awaitTermination()