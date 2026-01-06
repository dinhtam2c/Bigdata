from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim
from pyspark.sql.types import IntegerType, LongType, StringType
from pyspark.sql import functions as F
import json
from urllib import request
from urllib.error import HTTPError, URLError

# Khởi tạo Spark
spark = SparkSession.builder \
    .appName("COVID HDFS to Elasticsearch") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()

# Elasticsearch configuration
ES_HOST = "http://elasticsearch:9200"
ES_INDEX = "covid-data"

def create_index_if_not_exists():
    try:
        # Kiểm tra index đã tồn tại chưa
        req = request.Request(f"{ES_HOST}/{ES_INDEX}", method='HEAD')
        with request.urlopen(req) as response:
            if response.status == 200:
                print(f"Index {ES_INDEX} đã tồn tại")
                return
    except HTTPError as e:
        if e.code != 404:
            print(f"Lỗi kiểm tra index: {e}")
            return
    except Exception:
        pass
    
    # Tạo index mới với mapping
    mapping = {
        "mappings": {
            "properties": {
                "Country": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Country_code": {"type": "keyword"},
                "WHO_region": {"type": "keyword"},
                "Date_reported": {"type": "date", "format": "yyyy-MM-dd"},
                "New_cases": {"type": "integer"},
                "Cumulative_cases": {"type": "long"},
                "New_deaths": {"type": "integer"},
                "Cumulative_deaths": {"type": "long"}
            }
        }
    }
    
    try:
        data = json.dumps(mapping).encode('utf-8')
        req = request.Request(
            f"{ES_HOST}/{ES_INDEX}",
            data=data,
            method='PUT',
            headers={'Content-Type': 'application/json'}
        )
        with request.urlopen(req) as response:
            if response.status in [200, 201]:
                print(f"Đã tạo index {ES_INDEX}")
    except Exception as e:
        print(f"Lỗi khi tạo index: {str(e)}")

def send_to_elasticsearch(partition):
    
    batch = list(partition)
    if not batch:
        return
    
    # Tạo bulk request
    bulk_data = []
    for record in batch:
        bulk_data.append(json.dumps({"index": {"_index": ES_INDEX}}))
        bulk_data.append(json.dumps(record))
    
    bulk_body = "\n".join(bulk_data) + "\n"
    
    try:
        data = bulk_body.encode('utf-8')
        req = request.Request(
            f"{ES_HOST}/_bulk",
            data=data,
            headers={'Content-Type': 'application/x-ndjson'}
        )
        
        with request.urlopen(req) as response:
            if response.status == 200:
                result = json.loads(response.read().decode('utf-8'))
                if result.get("errors"):
                    print(f"Một số documents gặp lỗi khi insert")
                else:
                    print(f"Đã ghi {len(batch)} documents")
            else:
                print(f"Lỗi bulk insert: {response.status}")
    except Exception as e:
        print(f"❌ Lỗi gửi data: {str(e)}")

# Tạo index trước
print("Chuẩn bị Elasticsearch index...")
create_index_if_not_exists()

# Đọc dữ liệu từ HDFS
print("\nĐang đọc dữ liệu từ HDFS...")
df = spark.read.json("hdfs://hdfs-namenode-0.hdfs-namenode:8020/covid/raw/2026/01/06/")

print("Đang xử lý và chuyển đổi kiểu dữ liệu...")

# Debug: in schema
df.printSchema()
print(f"Column names: {df.columns}")

# Get the Date_reported column by referencing df.columns
date_col = df.columns[7]  # Date_reported is at index 7
print(f"Date column: '{date_col}' (repr: {repr(date_col)})")

# Use F.col with the column name from the list to avoid resolution issues

df_cleaned = df.select(
    F.col("Country"),
    F.col("Country_code"),
    F.col("WHO_region"),
    F.col(date_col).alias("Date_reported"),
    F.when(F.trim(F.col("New_cases")) == "", 0).otherwise(F.col("New_cases").cast(IntegerType())).alias("New_cases"),
    F.when(F.trim(F.col("Cumulative_cases")) == "", 0).otherwise(F.col("Cumulative_cases").cast(LongType())).alias("Cumulative_cases"),
    F.when(F.trim(F.col("New_deaths")) == "", 0).otherwise(F.col("New_deaths").cast(IntegerType())).alias("New_deaths"),
    F.when(F.trim(F.col("Cumulative_deaths")) == "", 0).otherwise(F.col("Cumulative_deaths").cast(LongType())).alias("Cumulative_deaths")
)

# Đếm số records
total_count = df_cleaned.count()
print(f"Tìm thấy {total_count:,} records từ HDFS")

# Chuyển đổi sang RDD và ghi vào Elasticsearch theo batch
print("\nĐang ghi dữ liệu vào Elasticsearch...")
df_cleaned.rdd.map(lambda row: row.asDict()).repartition(10).foreachPartition(send_to_elasticsearch)

print(f"\nHoàn thành! Đã xử lý {total_count:,} records")
print(f"Kiểm tra tại: {ES_HOST}/{ES_INDEX}/_count")
spark.stop()