import os
import json
import time
import sys
import signal
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
import logging

# Cấu hình logging ra stdout
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Cấu hình từ biến môi trường
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'covid-raw')
HDFS_URL = os.getenv('HDFS_URL', 'http://hdfs-namenode-0.hdfs-namenode:9870')
HDFS_USER = os.getenv('HDFS_USER', 'root')
HDFS_PATH = os.getenv('HDFS_PATH', '/covid/raw')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '500'))
BATCH_TIMEOUT = int(os.getenv('BATCH_TIMEOUT', '30'))  # giây

# Group ID mới để đảm bảo đọc lại từ đầu nếu cần
GROUP_ID = 'hdfs-final-v1'

def get_hdfs_client():
    log.info(f"Connecting to HDFS at {HDFS_URL}...")
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER, timeout=10)
        client.list('/') # Test connect
        log.info("Connected to HDFS successfully!")
        return client
    except Exception as e:
        log.error(f"Error connecting to HDFS: {e}")
        return None

def get_kafka_consumer():
    log.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            max_poll_records=BATCH_SIZE,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            retry_backoff_ms=1000,
            reconnect_backoff_ms=1000
        )
        log.info("Connected to Kafka successfully!")
        return consumer
    except Exception as e:
        log.error(f"Error connecting to Kafka: {e}")
        return None

def flush_date_batch_to_hdfs(client, date_part, records):
    """Write a batch of records for a specific date to HDFS"""
    if not records:
        return True
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    hdfs_dir = f"{HDFS_PATH}/{date_part}"
    filename = f"{hdfs_dir}/covid_data_{timestamp}.jsonl"
    
    # Convert list of dicts to JSON lines
    data_str = "\n".join([json.dumps(record, ensure_ascii=False) for record in records])
    
    try:
        client.makedirs(hdfs_dir)
        with client.write(filename, encoding='utf-8') as writer:
            writer.write(data_str)
        log.info(f"Saved {len(records)} records for {date_part} to {filename}")
        return True
    except Exception as e:
        log.error(f"Error writing to HDFS for {date_part}: {e}")
        return False

running = True
def signal_handler(sig, frame):
    global running
    log.info('Stopping consumer...')
    running = False

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    log.info("Consumer Script Started...")
    
    # Retry loop cho kết nối
    hdfs_client = None
    consumer = None

    while running:
        if not hdfs_client:
            hdfs_client = get_hdfs_client()
            if not hdfs_client:
                time.sleep(5)
                continue
        
        if not consumer:
            consumer = get_kafka_consumer()
            if not consumer:
                time.sleep(5)
                continue
        
        # Main processing loop - buffer per date
        current_date = None
        buffer = []
        last_activity_time = time.time()
        
        log.info(f"Starting loop. Max records per file: {BATCH_SIZE}, Timeout: {BATCH_TIMEOUT}s")
        
        try:
            while running:
                # Poll messages
                msg_pack = consumer.poll(timeout_ms=1000)
                
                if msg_pack:
                    for tp, messages in msg_pack.items():
                        for message in messages:
                            record = message.value
                            date_str = record.get('Date_reported', 'unknown')
                            
                            if date_str and date_str != 'unknown':
                                # Convert 2020-01-04 -> year=2020/month=01/day=04 (Hive partitioning)
                                parts = date_str.split('-')
                                date_part = f"year={parts[0]}/month={parts[1]}/day={parts[2]}"
                            else:
                                # Fallback to current date if missing
                                now = datetime.now()
                                date_part = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
                            
                            # Detect date change -> flush previous date
                            if current_date and date_part != current_date and buffer:
                                log.info(f"Date changed from {current_date} to {date_part}. Flushing {len(buffer)} records...")
                                if not flush_date_batch_to_hdfs(hdfs_client, current_date, buffer):
                                    log.error("Failed to write to HDFS. Reconnecting...")
                                    hdfs_client = None
                                    break
                                consumer.commit()  # Commit offset after successful write
                                log.info("Offset committed after date change flush")
                                buffer = []
                            
                            # Update current date and add record
                            current_date = date_part
                            buffer.append(record)
                            last_activity_time = time.time()
                            
                            # Flush if batch size reached (control max file size)
                            if len(buffer) >= BATCH_SIZE:
                                log.info(f"Batch size reached ({BATCH_SIZE}). Flushing for date {current_date}...")
                                if not flush_date_batch_to_hdfs(hdfs_client, current_date, buffer):
                                    log.error("Failed to write to HDFS. Reconnecting...")
                                    hdfs_client = None
                                    break
                                consumer.commit()  # Commit offset after successful write
                                log.info("Offset committed after batch flush")
                                buffer = []
                                last_activity_time = time.time()
                
                # Check timeout - flush if no new data for BATCH_TIMEOUT seconds
                current_time = time.time()
                time_diff = current_time - last_activity_time
                
                if buffer and time_diff >= BATCH_TIMEOUT:
                    log.info(f"Timeout reached ({BATCH_TIMEOUT}s). Flushing {len(buffer)} records for date {current_date}...")
                    if not flush_date_batch_to_hdfs(hdfs_client, current_date, buffer):
                        log.error("Failed to write to HDFS. Reconnecting...")
                        hdfs_client = None
                        break
                    consumer.commit()  # Commit offset after successful write
                    log.info("Offset committed after timeout flush")
                    buffer = []
                    last_activity_time = current_time

        except Exception as e:
            log.error(f"Unexpected error in loop: {e}")
            consumer.close()
            consumer = None
            time.sleep(5)
    
    if consumer: consumer.close()
    log.info("Consumer stopped.")

if __name__ == "__main__":
    time.sleep(5) # Wait for network
    main()
