import csv
import json
import time
import os
import sys
from kafka import KafkaProducer
from itertools import groupby

# Cấu hình Kafka
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'covid-raw')
DATA_FILE = os.getenv('DATA_FILE', '/app/data/covid_0.csv')

# Cấu hình Streaming: 5 giây cho mỗi ngày trong dữ liệu
DAY_DELAY = float(os.getenv('DAY_DELAY', '5.0')) 

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def main():
    print(f"Starting Realtime Streaming Simulation (Day-by-Day Mode)...")
    print(f"Server: {BOOTSTRAP_SERVERS}")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Data File: {DATA_FILE}")
    print(f"Delay per Simulated Day: {DAY_DELAY}s")

    while not os.path.exists(DATA_FILE):
        print(f"Waiting for file {DATA_FILE}...")
        time.sleep(2)

    try:
        producer = KafkaProducer(
            bootstrap_servers=[BOOTSTRAP_SERVERS],
            value_serializer=json_serializer
        )
        print("Kafka Producer connected!")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        sys.exit(1)

    while True: # Loop vô hạn để giả lập stream không ngừng nghỉ
        print("\n--- Starting/Restarting Simulation Loop (Reading CSV) ---")
        try:
            with open(DATA_FILE, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                # Đọc toàn bộ vào memory để sort và group (Data Covid không quá lớn, ok cho memory)
                print("Reading and sorting data...")
                all_data = sorted(list(reader), key=lambda x: x['Date_reported'])
                
                # Group by Date_reported
                for date, records in groupby(all_data, key=lambda x: x['Date_reported']):
                    print(f"--- Sending data for date: {date} ---")
                    
                    # Chuyển iterator thành list để đếm
                    daily_records = list(records)
                    
                    # Gửi từng record trong ngày đó (gần như tức thời)
                    for record in daily_records:
                        producer.send(TOPIC_NAME, record)
                    
                    producer.flush()
                    print(f"Sent {len(daily_records)} records for {date}. Waiting {DAY_DELAY}s...")
                    
                    # Delay giả lập chuyển ngày
                    time.sleep(DAY_DELAY)
                
                print(f"Finished one pass over historical data. Restarting in 10s...")
                time.sleep(10)

        except Exception as e:
            print(f"Error during streaming loop: {e}")
            time.sleep(5) # Đợi chút rồi thử lại nếu lỗi


if __name__ == "__main__":
    main()
