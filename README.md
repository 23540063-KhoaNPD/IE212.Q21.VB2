# IE212.Q21.VB2

Bước 1:
Chạy file tạo dữ liệu ảo
python data_generator_socket.py

Bước 2:
Chạy file xử lý luồng chính
spark-submit spark_streaming.py

Cài Kafka:
docker run -d --name kafka \
  -p 9092:9092 \
  apache/kafka:latest

Tạo topic:
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic sensor-data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

  
