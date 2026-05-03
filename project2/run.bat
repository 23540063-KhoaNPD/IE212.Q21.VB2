@echo off
echo ================================
echo STARTING SYSTEM...
echo ================================

echo.
echo [1] Start Docker Kafka...
docker-compose up -d

timeout /t 10

echo.
echo [2] Create Kafka topic...
docker exec -it project2-kafka-1 kafka-topics.sh --create --if-not-exists --topic logs --bootstrap-server localhost:9092

echo.
echo [3] Install Python Kafka lib...
pip install kafka-python

echo.
echo [4] Start Producer...
start cmd /k python log_producer.py

timeout /t 5

echo.
echo [5] Start Spark Streaming...
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 streaming.py

pause