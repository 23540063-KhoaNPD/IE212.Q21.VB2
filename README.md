Hướng dẫn Setup Môi trường DemoKafka
Dự án sử dụng mô hình Kafka KRaft (không Zookeeper) và PySpark Structured Streaming.

1. Cấu hình Stack chuẩn (Bắt buộc)
Để tránh lỗi ClassNotFound và NoSuchMethod, yêu cầu cả nhóm cài đặt đúng phiên bản sau:

Python: 3.10 - 3.12

PySpark: 3.5.0

Java: JDK 11 hoặc 17

Kafka (Docker): apache/kafka:3.7.0

2. Cài đặt Thư viện
Kích hoạt môi trường ảo (venv) và chạy lệnh:

Bash
pip install pyspark==3.5.0 pandas kafka-python
3. Khởi động Kafka (KRaft Mode)
Sử dụng Docker Compose để dựng Broker:

Bash
docker-compose up -d
Lưu ý: Nếu đổi Cluster ID, hãy đảm bảo format đúng trong file .yml.

4. Luồng thực thi (Workflow)
Chạy Producer: Đọc dữ liệu từ CSV và đẩy vào Kafka topic my_dataset_topic.

Bash
python3 producer.py
Chạy Spark Check: Kiểm tra dữ liệu đã vào Kafka chưa.

Bash
python3 check_data.py
5. Lưu ý quan trọng về Spark-Kafka Connector
Trong code Spark, sử dụng bản build cho Scala 2.12 để khớp với nhân PySpark 3.5.0.

Cấu hình bắt buộc trong code:

Python

.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")

Tip: Nếu đổi máy hoặc đổi bản Spark mà bị lỗi thư viện, hãy xóa cache Ivy trước khi chạy lại:
rm -rf ~/.ivy2/cache