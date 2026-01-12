import os
import json
import time
import sys
import signal
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Optional
from kafka import KafkaConsumer
from hdfs import InsecureClient
import logging

# ==================== CONFIGURATION ====================
class Config:
    """Centralized configuration management"""
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka:9092')
    TOPIC_NAME = os.getenv('TOPIC_NAME', 'covid-raw')
    HDFS_URL = os.getenv('HDFS_URL', 'http://hdfs-namenode-0.hdfs-namenode:9870')
    HDFS_USER = os.getenv('HDFS_USER', 'root')
    HDFS_PATH = os.getenv('HDFS_PATH', '/covid/raw')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '500'))
    BATCH_TIMEOUT = int(os.getenv('BATCH_TIMEOUT', '30'))
    GROUP_ID = os.getenv('GROUP_ID', 'hdfs-final-v1')
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))

# ==================== LOGGING SETUP ====================
def setup_logging():
    """Configure logging with custom format"""
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

log = setup_logging()

# ==================== CONNECTION MANAGEMENT ====================
def create_hdfs_client(max_retries: int = 3) -> Optional[InsecureClient]:
    """
    Create HDFS client with retry logic
    
    Args:
        max_retries: Maximum number of connection attempts
        
    Returns:
        InsecureClient if successful, None otherwise
    """
    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"Connecting to HDFS at {Config.HDFS_URL} (Attempt {attempt}/{max_retries})...")
            client = InsecureClient(Config.HDFS_URL, user=Config.HDFS_USER, timeout=10)
            client.list('/')  # Test connection
            log.info("✓ HDFS connection established")
            return client
        except Exception as e:
            log.error(f"✗ HDFS connection failed: {e}")
            if attempt < max_retries:
                time.sleep(Config.RETRY_DELAY)
    
    return None

def create_kafka_consumer(max_retries: int = 3) -> Optional[KafkaConsumer]:
    """
    Create Kafka consumer with retry logic
    
    Args:
        max_retries: Maximum number of connection attempts
        
    Returns:
        KafkaConsumer if successful, None otherwise
    """
    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"Connecting to Kafka at {Config.BOOTSTRAP_SERVERS} (Attempt {attempt}/{max_retries})...")
            consumer = KafkaConsumer(
                Config.TOPIC_NAME,
                bootstrap_servers=Config.BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=Config.GROUP_ID,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                request_timeout_ms=20000,
                max_poll_records=Config.BATCH_SIZE
            )
            log.info("✓ Kafka consumer created")
            return consumer
        except Exception as e:
            log.error(f"✗ Kafka connection failed: {e}")
            if attempt < max_retries:
                time.sleep(Config.RETRY_DELAY)
    
    return None

# ==================== DATA PROCESSING ====================
def parse_date_to_path(date_str: str) -> str:
    """
    Convert date string to HDFS path format
    
    Args:
        date_str: Date in format YYYY-MM-DD
        
    Returns:
        Path in format YYYY/MM/DD
    """
    if date_str and date_str != 'unknown':
        try:
            # Validate date format
            datetime.strptime(date_str, '%Y-%m-%d')
            return date_str.replace('-', '/')
        except ValueError:
            log.warning(f"Invalid date format: {date_str}, using current date")
    
    return datetime.now().strftime("%Y/%m/%d")

def group_records_by_date(buffer: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Group records by Date_reported field
    
    Args:
        buffer: List of record dictionaries
        
    Returns:
        Dictionary mapping date paths to lists of records
    """
    groups = defaultdict(list)
    
    for record in buffer:
        date_str = record.get('Date_reported', 'unknown')
        date_path = parse_date_to_path(date_str)
        groups[date_path].append(record)
    
    return groups

def write_records_to_hdfs(client: InsecureClient, date_path: str, records: List[Dict]) -> bool:
    """
    Write records to HDFS for a specific date
    
    Args:
        client: HDFS client
        date_path: Date path in format YYYY/MM/DD
        records: List of records to write
        
    Returns:
        True if successful, False otherwise
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    hdfs_dir = f"{Config.HDFS_PATH}/{date_path}"
    filename = f"{hdfs_dir}/covid_data_{timestamp}.jsonl"
    
    try:
        # Ensure directory exists
        client.makedirs(hdfs_dir)
        
        # Convert records to JSON lines format
        data_str = "\n".join([json.dumps(record, ensure_ascii=False) for record in records])
        
        # Write to HDFS
        with client.write(filename, encoding='utf-8') as writer:
            writer.write(data_str)
        
        log.info(f"✓ Saved {len(records)} records for {date_path} → {filename}")
        return True
        
    except Exception as e:
        log.error(f"✗ Failed to write to HDFS for {date_path}: {e}")
        return False

def flush_buffer_to_hdfs(client: InsecureClient, buffer: List[Dict]) -> bool:
    """
    Flush entire buffer to HDFS, grouped by date
    
    Args:
        client: HDFS client
        buffer: List of records to flush
        
    Returns:
        True if all writes successful, False otherwise
    """
    if not buffer:
        return True
    
    groups = group_records_by_date(buffer)
    all_success = True
    
    log.info(f"Flushing {len(buffer)} records across {len(groups)} date(s)...")
    
    for date_path, records in groups.items():
        if not write_records_to_hdfs(client, date_path, records):
            all_success = False
    
    return all_success

# ==================== MAIN PROCESSING LOOP ====================
class ConsumerState:
    """Manage consumer state"""
    def __init__(self):
        self.running = True
        self.buffer = []
        self.last_flush_time = time.time()
        
    def should_flush(self) -> bool:
        """Determine if buffer should be flushed"""
        if len(self.buffer) >= Config.BATCH_SIZE:
            return True
        
        if len(self.buffer) > 0:
            time_diff = time.time() - self.last_flush_time
            if time_diff >= Config.BATCH_TIMEOUT:
                return True
        
        return False
    
    def reset_buffer(self):
        """Reset buffer and update flush time"""
        self.buffer = []
        self.last_flush_time = time.time()

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    log.info('Received shutdown signal, stopping consumer...')
    global state
    state.running = False

def process_messages(consumer: KafkaConsumer, hdfs_client: InsecureClient, state: ConsumerState) -> bool:
    """
    Main message processing loop
    
    Returns:
        True if should continue, False if should reconnect
    """
    try:
        # Poll for messages
        msg_pack = consumer.poll(timeout_ms=1000)
        
        # Add messages to buffer
        if msg_pack:
            for tp, messages in msg_pack.items():
                for message in messages:
                    state.buffer.append(message.value)
        
        # Check if should flush
        if state.should_flush():
            log.info(f"Buffer threshold reached: {len(state.buffer)} records")
            if flush_buffer_to_hdfs(hdfs_client, state.buffer):
                state.reset_buffer()
            else:
                log.error("Failed to flush buffer, will reconnect to HDFS")
                return False
        
        return True
        
    except Exception as e:
        log.error(f"Error processing messages: {e}")
        return False

def main():
    global state
    state = ConsumerState()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    log.info("=" * 60)
    log.info("COVID Data Consumer - HDFS Writer")
    log.info("=" * 60)
    log.info(f"Kafka: {Config.BOOTSTRAP_SERVERS}")
    log.info(f"Topic: {Config.TOPIC_NAME}")
    log.info(f"HDFS: {Config.HDFS_URL}")
    log.info(f"Batch Size: {Config.BATCH_SIZE}")
    log.info(f"Batch Timeout: {Config.BATCH_TIMEOUT}s")
    log.info("=" * 60)
    
    hdfs_client = None
    consumer = None
    
    # Main loop with auto-reconnect
    while state.running:
        # Ensure HDFS connection
        if not hdfs_client:
            hdfs_client = create_hdfs_client(Config.MAX_RETRIES)
            if not hdfs_client:
                log.warning(f"Retrying HDFS connection in {Config.RETRY_DELAY}s...")
                time.sleep(Config.RETRY_DELAY)
                continue
        
        # Ensure Kafka connection
        if not consumer:
            consumer = create_kafka_consumer(Config.MAX_RETRIES)
            if not consumer:
                log.warning(f"Retrying Kafka connection in {Config.RETRY_DELAY}s...")
                time.sleep(Config.RETRY_DELAY)
                continue
        
        # Process messages
        if not process_messages(consumer, hdfs_client, state):
            # Reconnection needed
            hdfs_client = None
            time.sleep(Config.RETRY_DELAY)
    
    # Cleanup
    if state.buffer:
        log.info(f"Flushing remaining {len(state.buffer)} records before shutdown...")
        if hdfs_client:
            flush_buffer_to_hdfs(hdfs_client, state.buffer)
    
    if consumer:
        consumer.close()
        log.info("Kafka consumer closed")
    
    log.info("Consumer stopped gracefully")

if __name__ == "__main__":
    time.sleep(5)  # Initial delay for network stabilization
    main()