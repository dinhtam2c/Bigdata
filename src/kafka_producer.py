import csv
import json
import time
import os
import sys
from kafka import KafkaProducer

# Cấu hình lấy từ biến môi trường
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'covid-raw')
DATA_FILE = os.getenv('DATA_FILE', '/app/data/covid_0.csv')

KAFKA_CONFIG = {
    'bootstrap_servers': [BOOTSTRAP_SERVERS],
    'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
    'linger_ms': 20,
    'batch_size': 64 * 1024,
    'compression_type': 'gzip',
    'acks': 'all',
    'retries': 5
}

def create_producer():
    try:
        producer = KafkaProducer(**KAFKA_CONFIG)
        print("Connected to Kafka successfully!")
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None

def process_data(producer):
    if not producer:
        return

    print(f"Reading {DATA_FILE} -> Topic '{TOPIC_NAME}'...")
    
    # Kiểm tra file tồn tại
    if not os.path.exists(DATA_FILE):
        print(f"Error: Data file not found at {DATA_FILE}")
        sys.exit(1)

    try:
        with open(DATA_FILE, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            count = 0
            
            for row in reader:
                producer.send(TOPIC_NAME, value=row)
                count += 1
                
                if count % 2000 == 0:
                    print(f"Sent {count} messages...")

            producer.flush()
            print(f"Finished! Total sent: {count} messages.")
            
    except Exception as e:
        print(f"Processing error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    # Đợi file dữ liệu nếu cần (Logic wait sẽ nằm ở shell wrapper hoặc ở đây, 
    # nhưng tốt nhất là xử lý ở shell command trong yaml để python script clean)
    producer = create_producer()
    if producer:
        process_data(producer)
