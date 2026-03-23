start kafka

docker-compose -f docker/docker-compose.yml up -d


tạo topic

docker exec -it kafka kafka-topics.sh --create
--topic sensor-raw --bootstrap-server localhost:9092


docker exec -it kafka kafka-topics.sh --create
--topic sensor-processed --bootstrap-server localhost:9092


chạy producer

python producer/producer.py


Chạy spark

spark-submit spark/app.py


Chạy consumer

python consumer/consumer.py
