import csv
import json
import time
import os
import sys
from typing import Iterator, Dict
from kafka import KafkaProducer
import logging


class Config:
    """Centralized configuration management"""
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka:9092')
    TOPIC_NAME = os.getenv('TOPIC_NAME', 'covid-raw')
    DATA_FILE = os.getenv('DATA_FILE', '/app/data/covid_0.csv')
    BATCH_LOG_INTERVAL = int(os.getenv('BATCH_LOG_INTERVAL', '2000'))
    FILE_WAIT_TIMEOUT = int(os.getenv('FILE_WAIT_TIMEOUT', '60'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))


def setup_logging():
    """Configure logging with custom format"""
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format='%(asctime)s - [%(levelname)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

log = setup_logging()


KAFKA_CONFIG = {
    'value_serializer': lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
    'linger_ms': 20,
    'batch_size': 64 * 1024,
    'compression_type': 'gzip',
    'acks': 'all',
    'retries': 5,
    'max_in_flight_requests_per_connection': 5
}


def validate_file(filepath: str) -> bool:
    """
    Validate file exists and is readable
    
    Args:
        filepath: Path to file
        
    Returns:
        True if valid, False otherwise
    """
    if not os.path.exists(filepath):
        log.error(f"File not found: {filepath}")
        return False
    
    if not os.path.isfile(filepath):
        log.error(f"Path is not a file: {filepath}")
        return False
    
    if not os.access(filepath, os.R_OK):
        log.error(f"File is not readable: {filepath}")
        return False
    
    file_size = os.path.getsize(filepath)
    log.info(f"✓ File validated: {filepath} ({file_size:,} bytes)")
    return True

def wait_for_file(filepath: str, timeout: int = 60) -> bool:
    start_time = time.time()
    warned = False

    while not os.path.exists(filepath):
        elapsed = time.time() - start_time
        if elapsed >= timeout:
            log.error(f"Timeout waiting for file: {filepath}")
            return False

        if not warned:
            log.warning(f"Waiting for data file: {filepath}")
            warned = True

        time.sleep(2)

    return validate_file(filepath)


def read_csv_stream(filepath: str) -> Iterator[Dict]:
    """
    Stream CSV records one by one (memory efficient)
    """
    try:
        with open(filepath, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file)

            if not reader.fieldnames:
                raise ValueError("CSV file has no headers")

            for row in reader:
                yield row

    except Exception as e:
        log.error(f"Error reading CSV: {e}")
        raise


def create_kafka_producer(max_retries: int = 3) -> KafkaProducer:
    """
    Create Kafka producer with retry logic
    
    Args:
        max_retries: Maximum connection attempts
        
    Returns:
        KafkaProducer instance
        
    Raises:
        SystemExit if connection fails after retries
    """
    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"Connecting to Kafka at {Config.BOOTSTRAP_SERVERS} (Attempt {attempt}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=[Config.BOOTSTRAP_SERVERS],
                **KAFKA_CONFIG
            )
            log.info("✓ Kafka producer connected")
            return producer
            
        except Exception as e:
            log.error(f"✗ Kafka connection failed: {e}")
            if attempt < max_retries:
                time.sleep(Config.RETRY_DELAY)
            else:
                log.error("Max retries reached, exiting")
                sys.exit(1)

def send_callback(record_metadata):
    """Callback for successful send"""
    pass  # Silent success

def error_callback(exception):
    """Callback for send errors"""
    log.error(f"Send failed: {exception}")

def process_and_send_data(producer: KafkaProducer, filepath: str) -> int:
    """
    Process CSV and send all records to Kafka
    Log progress every 1 second
    """
    log.info(f"Start sending data → topic={Config.TOPIC_NAME}")

    count = 0
    start_time = time.time()
    last_log_time = start_time
    last_log_count = 0

    try:
        for row in read_csv_stream(filepath):
            producer.send(Config.TOPIC_NAME, value=row)
            count += 1

            now = time.time()
            if now - last_log_time >= 1.0:
                sent = count - last_log_count
                rate = sent / (now - last_log_time)

                log.info(
                    f"sent={count:,} | "
                    f"+{sent:,}/s | "
                    f"rate={rate:.0f} msg/s"
                )

                last_log_time = now
                last_log_count = count

        producer.flush()

        elapsed = time.time() - start_time
        avg_rate = count / elapsed if elapsed > 0 else 0

        log.info(
            f"Done: total={count:,}, "
            f"time={elapsed:.1f}s, "
            f"avg_rate={avg_rate:.0f} msg/s"
        )

        return count

    except Exception as e:
        log.error(f"Processing error: {e}")
        raise



def main():
    """Main entry point"""
    log.info("=" * 60)
    log.info("COVID Data Producer - Batch Mode")
    log.info("=" * 60)
    log.info(f"Kafka Server: {Config.BOOTSTRAP_SERVERS}")
    log.info(f"Target Topic: {Config.TOPIC_NAME}")
    log.info(f"Data File: {Config.DATA_FILE}")
    log.info("=" * 60)
    
    # Wait for and validate data file
    if not wait_for_file(Config.DATA_FILE, Config.FILE_WAIT_TIMEOUT):
        log.error("Data file not available, exiting")
        sys.exit(1)
    
    # Create Kafka producer
    producer = create_kafka_producer(Config.MAX_RETRIES)
    
    # Process and send data
    try:
        records_sent = process_and_send_data(producer, Config.DATA_FILE)
        
        if records_sent > 0:
            log.info("✓ All records sent successfully")
        else:
            log.warning("⚠ No records were sent")
            
    except KeyboardInterrupt:
        log.info("\n⚠ Interrupted by user")
        sys.exit(130)
    except Exception as e:
        log.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        log.info("Closing Kafka producer...")
        producer.close()
        log.info("Producer shutdown complete")

if __name__ == "__main__":
    main()