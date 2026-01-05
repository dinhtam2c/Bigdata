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
            enable_auto_commit=True,
            group_id=GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            request_timeout_ms=20000,
            max_poll_records=BATCH_SIZE
        )
        log.info("Connected to Kafka successfully!")
        return consumer
    except Exception as e:
        log.error(f"Error connecting to Kafka: {e}")
        return None

def flush_to_hdfs(client, buffer):
    if not buffer:
        return True

    # Thêm timestamp có microsecond để tránh trùng tên file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    
    # Partition theo ngày để quản lý dữ liệu tốt hơn cho Batch Job sau này
    date_part = datetime.now().strftime("%Y/%m/%d")
    hdfs_dir = f"{HDFS_PATH}/{date_part}"
    filename = f"{hdfs_dir}/covid_data_{timestamp}.jsonl"
    
    # Chuyển đổi list dict thành chuỗi JSON lines
    data_str = "\n".join([json.dumps(record, ensure_ascii=False) for record in buffer])
    
    try:
        client.makedirs(hdfs_dir) # Đảm bảo thư mục tồn tại
        with client.write(filename, encoding='utf-8') as writer:
            writer.write(data_str)
        log.info(f"Saved {len(buffer)} records to {filename}")
        return True
    except Exception as e:
        log.error(f"Error writing to HDFS: {e}")
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
        
        # Main processing loop
        buffer = []
        last_flush_time = time.time()
        
        log.info(f"Starting loop. Batch: {BATCH_SIZE}, Timeout: {BATCH_TIMEOUT}s")
        
        try:
            while running:
                # Poll message
                msg_pack = consumer.poll(timeout_ms=1000)
                
                if msg_pack:
                    for tp, messages in msg_pack.items():
                        for message in messages:
                            buffer.append(message.value)
                
                current_time = time.time()
                time_diff = current_time - last_flush_time
                
                # Check điều kiện flush
                if len(buffer) >= BATCH_SIZE or (len(buffer) > 0 and time_diff >= BATCH_TIMEOUT):
                    log.info(f"Flushing buffer: {len(buffer)} records...")
                    if flush_to_hdfs(hdfs_client, buffer):
                        buffer = []
                        last_flush_time = current_time
                    else:
                        # Nếu lỗi ghi HDFS, break ra để reconnect
                        log.error("Failed to write to HDFS. Reconnecting...")
                        hdfs_client = None
                        break 
                
                if not msg_pack and not buffer:
                    # Idle check log
                    pass

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
