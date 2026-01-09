from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim
from pyspark.sql.types import IntegerType, LongType, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import json
import re
from functools import partial
from urllib import request
from urllib.error import HTTPError, URLError

RUN_LEVEL = "all"

def effective_levels(run_level: str) -> set[str]:
    lv = str(run_level).strip().lower()
    if lv == "all":
        return {"raw", "1", "23", "4", "5", "6", "7"}

    deps = {
        "raw": {"raw"},
        "1": {"1"},
        "23": {"23"},
        "4": {"1", "4"},          # Level 4 cần Level 1 (snapshot/country_last)
        "5": {"23", "5"},         # Level 5 cần Level 2-3 (df_country_daily)
        "6": {"1", "23", "6"},    # Level 6 cần Level 1 + 2-3
        "7": {"7"},               # Level 7 dùng df_raw + df_analysis
    }
    if lv not in deps:
        raise ValueError(f"RUN_LEVEL không hợp lệ: {run_level}")
    return deps[lv]

LEVELS = effective_levels(RUN_LEVEL)

def should_run(level: str) -> bool:
    return level in LEVELS

print(f"RUN_LEVEL={RUN_LEVEL} => WILL_RUN={sorted(LEVELS)}")

# Khởi tạo Spark
spark = SparkSession.builder \
    .appName("COVID HDFS to Elasticsearch") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")

# Elasticsearch configuration
ES_HOST = "http://elasticsearch:9200"
ES_INDEX = "covid-data"

# ES INDEX GROUPS (stats)
ES_COUNTRY_STATS_INDEX = "covid-stats-country"
ES_REGION_STATS_INDEX = "covid-stats-region"
ES_GLOBAL_DAILY_INDEX = "covid-ts-global-daily"
ES_COUNTRY_DAILY_INDEX = "covid-ts-country-daily"
ES_ANOMALIES_INDEX = "covid-events-anomalies"
ES_SEGMENTATION_INDEX = "covid-segmentation-country"
ES_QUALITY_INDEX = "covid-quality"
ES_RANKINGS_INDEX = "covid-rankings"

def json_safe_record(record: dict) -> dict:
    """
    Convert all date / datetime fields to ISO string
    """
    out = {}
    for k, v in record.items():
        if v is None:
            out[k] = None
        elif hasattr(v, "isoformat"):  # date or datetime
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out

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

def send_to_elasticsearch(partition, chunk_size: int = 2000):
    """
    Stream bulk indexing theo chunk để tránh list(partition) gây OOM.
    chunk_size: số documents mỗi lần _bulk (mỗi doc = 2 dòng NDJSON)
    """

    def _flush(lines: list[str], docs_count: int):
        if not lines:
            return 0
        bulk_body = "\n".join(lines) + "\n"
        try:
            req = request.Request(
                f"{ES_HOST}/_bulk",
                data=bulk_body.encode("utf-8"),
                headers={"Content-Type": "application/x-ndjson"},
                method="POST",
            )
            with request.urlopen(req) as response:
                if response.status == 200:
                    result = json.loads(response.read().decode("utf-8"))
                    if result.get("errors"):
                        print("RAW: Một số documents gặp lỗi khi insert")
                    return docs_count
                print(f"RAW: Lỗi bulk insert status={response.status}")
                return 0
        except Exception as e:
            print(f"RAW: Lỗi gửi data: {str(e)}")
            return 0

    lines: list[str] = []
    docs_in_chunk = 0
    total_written = 0

    for record in partition:
        country = record.get("Country")
        date_reported = record.get("Date_reported")
        doc_id = f"{country}_{date_reported}"

        lines.append(json.dumps({"index": {"_index": ES_INDEX, "_id": doc_id}}))
        lines.append(json.dumps(json_safe_record(record)))
        docs_in_chunk += 1

        if docs_in_chunk >= chunk_size:
            total_written += _flush(lines, docs_in_chunk)
            lines = []
            docs_in_chunk = 0

    total_written += _flush(lines, docs_in_chunk)

    if total_written:
        print(f"RAW: Đã ghi {total_written} documents")

def create_index_if_not_exists_generic(index_name: str, mapping: dict, settings: dict | None = None):
    try:
        req = request.Request(f"{ES_HOST}/{index_name}", method="HEAD")
        with request.urlopen(req) as resp:
            if resp.status == 200:
                print(f"Index {index_name} đã tồn tại")
                return
    except HTTPError as e:
        if e.code != 404:
            print(f"Lỗi kiểm tra index {index_name}: {e}")
            return
    except Exception:
        pass

    body = {"mappings": mapping.get("mappings", {})}
    if settings:
        body["settings"] = settings

    try:
        req = request.Request(
            f"{ES_HOST}/{index_name}",
            data=json.dumps(body).encode("utf-8"),
            method="PUT",
            headers={"Content-Type": "application/json"},
        )
        with request.urlopen(req) as resp:
            if resp.status in (200, 201):
                print(f"Đã tạo index {index_name}")
            else:
                print(f"Tạo index {index_name} thất bại: {resp.status}")
    except Exception as e:
        print(f"Lỗi khi tạo index {index_name}: {str(e)}")

def _sanitize_id_part(v):
    s = "" if v is None else str(v)
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-zA-Z0-9_.\-:@]+", "_", s)
    return s[:512] if len(s) > 512 else s

def send_to_es_index_chunked(index_name: str, id_cols: list[str], partition, chunk_size: int = 2000):
    """
    Flush theo chunk_size để không gom cả partition vào RAM.
    """
    def flush(lines: list[str]):
        if not lines:
            return
        body = "\n".join(lines) + "\n"
        try:
            req = request.Request(
                f"{ES_HOST}/_bulk",
                data=body.encode("utf-8"),
                headers={"Content-Type": "application/x-ndjson"},
                method="POST",
            )
            with request.urlopen(req) as resp:
                if resp.status == 200:
                    result = json.loads(resp.read().decode("utf-8"))
                    if result.get("errors"):
                        print(f"{index_name}: có documents lỗi")
                else:
                    print(f"{index_name}: bulk status={resp.status}")
        except Exception as e:
            print(f"{index_name}: bulk error {str(e)}")

    lines: list[str] = []
    for record in partition:
        doc_id = "_".join(_sanitize_id_part(record.get(c)) for c in id_cols) if id_cols else None
        meta = {"index": {"_index": index_name}}
        if doc_id:
            meta["index"]["_id"] = doc_id

        lines.append(json.dumps(meta))
        lines.append(json.dumps(json_safe_record(record)))

        if len(lines) >= chunk_size * 2:  # mỗi record 2 dòng
            flush(lines)
            lines = []

    flush(lines)


# =========================
# Tạo index RAW chỉ khi chạy raw
# =========================
print("Chuẩn bị Elasticsearch index...")
if should_run("raw"):
    create_index_if_not_exists()
else:
    print("Skip RAW index (không chạy level raw)")

# =========================
# Đọc dữ liệu từ HDFS (luôn đọc để có df_cleaned)
# =========================
print("\nĐang đọc dữ liệu từ HDFS...")
df = spark.read.json("hdfs://hdfs-namenode-0.hdfs-namenode:8020/covid/raw/*/*/*/*.jsonl")

print("Đang xử lý và chuyển đổi kiểu dữ liệu...")

df.printSchema()
print(f"Column names: {df.columns}")

for old_col in df.columns:
    clean_col = old_col.lstrip('\ufeff')
    if old_col != clean_col:
        df = df.withColumnRenamed(old_col, clean_col)

df_cleaned = df.select(
    F.col("Country"),
    F.col("Country_code"),
    F.col("WHO_region"),
    F.col("Date_reported"),
    F.when(F.trim(F.col("New_cases")) == "", 0).otherwise(F.col("New_cases").cast(IntegerType())).alias("New_cases"),
    F.when(F.trim(F.col("Cumulative_cases")) == "", 0).otherwise(F.col("Cumulative_cases").cast(LongType())).alias("Cumulative_cases"),
    F.when(F.trim(F.col("New_deaths")) == "", 0).otherwise(F.col("New_deaths").cast(IntegerType())).alias("New_deaths"),
    F.when(F.trim(F.col("Cumulative_deaths")) == "", 0).otherwise(F.col("Cumulative_deaths").cast(LongType())).alias("Cumulative_deaths")
)

total_count = df_cleaned.count()
print(f"Tìm thấy {total_count:,} records từ HDFS")

# =========================
# df_analysis chỉ cần khi chạy 1/23/4/5/6/7
# =========================
need_analysis = any(should_run(x) for x in ("1", "23", "4", "5", "6", "7"))
if need_analysis:
    print("\n================= PHÂN TÍCH DỮ LIỆU THÔ =================")
    df_analysis = (
        df_cleaned
        .withColumn("Date_reported", F.to_date(F.col("Date_reported").cast("string"), "yyyy-MM-dd"))
        .filter(F.col("Date_reported").isNotNull())
    )
    df_analysis.cache()
else:
    df_analysis = None
    print("Skip df_analysis (không chạy levels 1..7)")

# =========================
# Mappings + settings (giữ nguyên)
# =========================
INDEX_SETTINGS = {"number_of_shards": 1, "number_of_replicas": 0}

COUNTRY_STATS_MAPPING = {
    "mappings": {
        "properties": {
            "Country": {"type": "keyword"},
            "Country_code": {"type": "keyword"},
            "WHO_region": {"type": "keyword"},
            "Days_with_data": {"type": "integer"},
            "First_date": {"type": "date", "format": "yyyy-MM-dd"},
            "First_date_with_cases": {"type": "date", "format": "yyyy-MM-dd"},
            "Last_date_series": {"type": "date", "format": "yyyy-MM-dd"},
            "Expected_days": {"type": "integer"},
            "Completeness": {"type": "double"},
            "Sum_new_cases": {"type": "long"},
            "Sum_new_deaths": {"type": "long"},
            "Avg_daily_cases": {"type": "double"},
            "Avg_daily_deaths": {"type": "double"},
            "Total_cases": {"type": "long"},
            "Total_deaths": {"type": "long"},
            "CFR_percent": {"type": "double"},
            "Rank_total_cases": {"type": "integer"},
            "Rank_total_deaths": {"type": "integer"},
            "Rank_cfr": {"type": "integer"},
            "As_of_date": {"type": "date", "format": "yyyy-MM-dd"},
        }
    }
}

REGION_STATS_MAPPING = {
    "mappings": {
        "properties": {
            "WHO_region": {"type": "keyword"},
            "Countries": {"type": "integer"},
            "Total_cases": {"type": "long"},
            "Total_deaths": {"type": "long"},
            "Avg_daily_cases": {"type": "double"},
            "Avg_daily_deaths": {"type": "double"},
            "As_of_date": {"type": "date", "format": "yyyy-MM-dd"},
        }
    }
}

GLOBAL_DAILY_MAPPING = {
    "mappings": {
        "properties": {
            "Date_reported": {"type": "date", "format": "yyyy-MM-dd"},
            "Total_new_cases": {"type": "long"},
            "Total_new_deaths": {"type": "long"},
            "Countries_reported": {"type": "integer"},
        }
    }
}

COUNTRY_DAILY_MAPPING = {
    "mappings": {
        "properties": {
            "Country": {"type": "keyword"},
            "Country_code": {"type": "keyword"},
            "WHO_region": {"type": "keyword"},
            "Date_reported": {"type": "date", "format": "yyyy-MM-dd"},
            "New_cases": {"type": "integer"},
            "New_deaths": {"type": "integer"},
            "Cumulative_cases": {"type": "long"},
            "Cumulative_deaths": {"type": "long"},
            "MA7_new_cases": {"type": "double"},
            "MA14_new_cases": {"type": "double"},
            "STD14_new_cases": {"type": "double"},
            "Growth_MA7_cases_pct": {"type": "double"},
            "CFR_percent": {"type": "double"},
            "CFR_7d_percent": {"type": "double"},
        }
    }
}

ANOMALIES_MAPPING = {
    "mappings": {
        "properties": {
            "Country": {"type": "keyword"},
            "Country_code": {"type": "keyword"},
            "WHO_region": {"type": "keyword"},
            "Date_reported": {"type": "date", "format": "yyyy-MM-dd"},
            "type": {"type": "keyword"},
            "New_cases": {"type": "integer"},
            "New_deaths": {"type": "integer"},
            "MA14_new_cases": {"type": "double"},
            "STD14_new_cases": {"type": "double"},
            "Spike_threshold": {"type": "double"},
        }
    }
}

SEGMENTATION_MAPPING = {
    "mappings": {
        "properties": {
            "Country": {"type": "keyword"},
            "Country_code": {"type": "keyword"},
            "WHO_region": {"type": "keyword"},
            "Impact_segment": {"type": "keyword"},
            "Total_cases": {"type": "long"},
            "Total_deaths": {"type": "long"},
            "CFR_percent": {"type": "double"},
            "Peak_date": {"type": "date", "format": "yyyy-MM-dd"},
            "Peak_MA7_new_cases": {"type": "double"},
            "Pre30_cases": {"type": "long"},
            "Post30_cases": {"type": "long"},
            "As_of_date": {"type": "date", "format": "yyyy-MM-dd"},
        }
    }
}

QUALITY_MAPPING = {
    "mappings": {
        "properties": {
            "scope": {"type": "keyword"},
            "key": {"type": "keyword"},
            "rows": {"type": "long"},
            "null_rate_country_code": {"type": "double"},
            "null_rate_who_region": {"type": "double"},
            "null_rate_date_reported": {"type": "double"},
            "null_rate_new_cases_raw": {"type": "double"},
            "null_rate_cum_cases_raw": {"type": "double"},
            "null_rate_new_deaths_raw": {"type": "double"},
            "null_rate_cum_deaths_raw": {"type": "double"},
            "negative_rate_new_cases": {"type": "double"},
            "negative_rate_new_deaths": {"type": "double"},
        }
    }
}

RANKINGS_MAPPING = {
    "mappings": {
        "properties": {
            "metric": {"type": "keyword"},
            "WHO_region": {"type": "keyword"},
            "rank": {"type": "integer"},
            "Country": {"type": "keyword"},
            "Country_code": {"type": "keyword"},
            "value": {"type": "double"},
            "As_of_date": {"type": "date", "format": "yyyy-MM-dd"},
        }
    }
}

# =========================
# Tạo indices stats theo LEVELS (chỉ tạo cái cần)
# =========================
print("\nChuẩn bị Elasticsearch STATS indices...")
if should_run("1"):
    create_index_if_not_exists_generic(ES_COUNTRY_STATS_INDEX, COUNTRY_STATS_MAPPING, INDEX_SETTINGS)
    create_index_if_not_exists_generic(ES_REGION_STATS_INDEX, REGION_STATS_MAPPING, INDEX_SETTINGS)
    create_index_if_not_exists_generic(ES_GLOBAL_DAILY_INDEX, GLOBAL_DAILY_MAPPING, INDEX_SETTINGS)

if should_run("23"):
    create_index_if_not_exists_generic(ES_COUNTRY_DAILY_INDEX, COUNTRY_DAILY_MAPPING, INDEX_SETTINGS)

if should_run("5"):
    create_index_if_not_exists_generic(ES_ANOMALIES_INDEX, ANOMALIES_MAPPING, INDEX_SETTINGS)

if should_run("6"):
    create_index_if_not_exists_generic(ES_SEGMENTATION_INDEX, SEGMENTATION_MAPPING, INDEX_SETTINGS)

if should_run("7"):
    create_index_if_not_exists_generic(ES_QUALITY_INDEX, QUALITY_MAPPING, INDEX_SETTINGS)

if should_run("4"):
    create_index_if_not_exists_generic(ES_RANKINGS_INDEX, RANKINGS_MAPPING, INDEX_SETTINGS)

# =========================
# LEVEL 1
# =========================
country_last = None
country_stats_lvl1_ranked = None
region_stats_lvl1 = None
daily_stats_lvl1 = None
snapshot = None
as_of_date = None

if should_run("1"):
    print("\n[Mức 1] Thống kê trực tiếp...")

    w_last = Window.partitionBy("Country").orderBy(
        F.col("Date_reported").desc(),
        F.col("Cumulative_cases").desc(),
        F.col("Cumulative_deaths").desc()
    )

    country_last = (
        df_analysis
        .withColumn("rn", F.row_number().over(w_last))
        .filter(F.col("rn") == 1)
        .select(
            "Country", "Country_code", "WHO_region",
            F.col("Cumulative_cases").alias("Total_cases"),
            F.col("Cumulative_deaths").alias("Total_deaths"),
            F.col("Date_reported").alias("As_of_date")
        )
    )

    country_basic = (
        df_analysis.groupBy("Country", "Country_code", "WHO_region")
        .agg(
            F.countDistinct("Date_reported").cast("int").alias("Days_with_data"),
            F.min("Date_reported").alias("First_date"),
            F.min(F.when((F.col("New_cases") > 0) | (F.col("Cumulative_cases") > 0), F.col("Date_reported"))).alias("First_date_with_cases"),
            F.max("Date_reported").alias("Last_date_series"),
            F.sum("New_cases").cast("long").alias("Sum_new_cases"),
            F.sum("New_deaths").cast("long").alias("Sum_new_deaths"),
        )
        .withColumn("Expected_days", (F.datediff(F.col("Last_date_series"), F.col("First_date")) + F.lit(1)).cast("int"))
        .withColumn("Completeness", F.when(F.col("Expected_days") > 0, F.col("Days_with_data") / F.col("Expected_days")).otherwise(F.lit(None)))
    )

    country_stats_lvl1 = (
        country_basic.join(country_last, on=["Country", "Country_code", "WHO_region"], how="left")
        .withColumn("Avg_daily_cases", (F.col("Sum_new_cases") / F.col("Days_with_data")).cast("double"))
        .withColumn("Avg_daily_deaths", (F.col("Sum_new_deaths") / F.col("Days_with_data")).cast("double"))
        .withColumn("CFR_percent", F.when(F.col("Total_cases") > 0, (F.col("Total_deaths") / F.col("Total_cases")) * 100.0).otherwise(F.lit(0.0)))
    )

    w_rank_cases = Window.orderBy(F.col("Total_cases").desc_nulls_last())
    w_rank_deaths = Window.orderBy(F.col("Total_deaths").desc_nulls_last())
    w_rank_cfr = Window.orderBy(F.col("CFR_percent").desc_nulls_last())

    country_stats_lvl1_ranked = (
        country_stats_lvl1
        .withColumn("Rank_total_cases", F.dense_rank().over(w_rank_cases).cast("int"))
        .withColumn("Rank_total_deaths", F.dense_rank().over(w_rank_deaths).cast("int"))
        .withColumn("Rank_cfr", F.dense_rank().over(w_rank_cfr).cast("int"))
    )

    region_daily = (
        df_analysis.groupBy("WHO_region", "Date_reported")
        .agg(
            F.sum("New_cases").cast("long").alias("Daily_new_cases"),
            F.sum("New_deaths").cast("long").alias("Daily_new_deaths"),
        )
    )

    region_totals = (
        country_last.groupBy("WHO_region")
        .agg(
            F.countDistinct("Country").cast("int").alias("Countries"),
            F.sum("Total_cases").cast("long").alias("Total_cases"),
            F.sum("Total_deaths").cast("long").alias("Total_deaths"),
            F.max("As_of_date").alias("As_of_date"),
        )
    )

    region_stats_lvl1 = (
        region_daily.groupBy("WHO_region")
        .agg(
            F.avg("Daily_new_cases").cast("double").alias("Avg_daily_cases"),
            F.avg("Daily_new_deaths").cast("double").alias("Avg_daily_deaths"),
        )
        .join(region_totals, on="WHO_region", how="left")
    )

    daily_stats_lvl1 = (
        df_analysis.groupBy("Date_reported")
        .agg(
            F.sum("New_cases").cast("long").alias("Total_new_cases"),
            F.sum("New_deaths").cast("long").alias("Total_new_deaths"),
            F.countDistinct("Country").cast("int").alias("Countries_reported"),
        )
    )

    snapshot = (
        country_stats_lvl1_ranked.select(
            "Country", "Country_code", "WHO_region",
            "Total_cases", "Total_deaths", "CFR_percent", "As_of_date",
            "Rank_total_cases", "Rank_total_deaths", "Rank_cfr"
        )
    )

    as_of_date = country_last.agg(F.max("As_of_date").alias("as_of_date")).collect()[0]["as_of_date"]

# =========================
# LEVEL 2-3
# =========================
df_country_daily = None

if should_run("23"):
    print("\n[Mức 2-3] Xu hướng, moving average, growth, CFR...")

    w_country_date = Window.partitionBy("Country").orderBy("Date_reported")
    w_ma7 = w_country_date.rowsBetween(-6, 0)
    w_ma14 = w_country_date.rowsBetween(-13, 0)

    df_country_daily = (
        df_analysis.select(
            "Country", "Country_code", "WHO_region", "Date_reported",
            "New_cases", "New_deaths", "Cumulative_cases", "Cumulative_deaths"
        )
        .withColumn("MA7_new_cases", F.avg("New_cases").over(w_ma7).cast("double"))
        .withColumn("MA14_new_cases", F.avg("New_cases").over(w_ma14).cast("double"))
        .withColumn("STD14_new_cases", F.stddev_pop("New_cases").over(w_ma14).cast("double"))
        .withColumn("Prev_MA7_new_cases", F.lag("MA7_new_cases", 1).over(w_country_date))
        .withColumn(
            "Growth_MA7_cases_pct",
            F.when(
                F.col("Prev_MA7_new_cases") > 0,
                ((F.col("MA7_new_cases") - F.col("Prev_MA7_new_cases")) / F.col("Prev_MA7_new_cases")) * 100.0
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "CFR_percent",
            F.when(F.col("Cumulative_cases") > 0, (F.col("Cumulative_deaths") / F.col("Cumulative_cases")) * 100.0).otherwise(F.lit(0.0))
        )
        .withColumn("Sum7_cases", F.sum("New_cases").over(w_ma7).cast("double"))
        .withColumn("Sum7_deaths", F.sum("New_deaths").over(w_ma7).cast("double"))
        .withColumn(
            "CFR_7d_percent",
            F.when(F.col("Sum7_cases") > 0, (F.col("Sum7_deaths") / F.col("Sum7_cases")) * 100.0).otherwise(F.lit(None))
        )
        .drop("Prev_MA7_new_cases", "Sum7_cases", "Sum7_deaths")
    )

# =========================
# LEVEL 4
# =========================
rankings = None

if should_run("4"):
    print("\n[Mức 4] Xếp hạng (TopN + theo WHO_region)...")

    N_TOP = 20

    top_cases = (
        snapshot.select(
            F.lit("top_total_cases").alias("metric"),
            F.lit(None).cast("string").alias("WHO_region"),
            F.col("Rank_total_cases").alias("rank"),
            "Country", "Country_code",
            F.col("Total_cases").cast("double").alias("value"),
            F.col("As_of_date").alias("As_of_date"),
        )
        .orderBy("rank")
        .limit(N_TOP)
    )

    top_deaths = (
        snapshot.select(
            F.lit("top_total_deaths").alias("metric"),
            F.lit(None).cast("string").alias("WHO_region"),
            F.col("Rank_total_deaths").alias("rank"),
            "Country", "Country_code",
            F.col("Total_deaths").cast("double").alias("value"),
            F.col("As_of_date").alias("As_of_date"),
        )
        .orderBy("rank")
        .limit(N_TOP)
    )

    top_cfr = (
        snapshot.filter(F.col("Total_cases") >= 1000)
        .select(
            F.lit("top_cfr_percent").alias("metric"),
            F.lit(None).cast("string").alias("WHO_region"),
            F.col("Rank_cfr").alias("rank"),
            "Country", "Country_code",
            F.col("CFR_percent").cast("double").alias("value"),
            F.col("As_of_date").alias("As_of_date"),
        )
        .orderBy("rank")
        .limit(N_TOP)
    )

    w_rank_region_cases = Window.partitionBy("WHO_region").orderBy(F.col("Total_cases").desc_nulls_last())
    top_in_region = (
        snapshot
        .withColumn("rank", F.dense_rank().over(w_rank_region_cases).cast("int"))
        .filter(F.col("rank") <= 10)
        .select(
            F.lit("top_region_total_cases").alias("metric"),
            "WHO_region",
            "rank",
            "Country",
            "Country_code",
            F.col("Total_cases").cast("double").alias("value"),
            F.col("As_of_date").alias("As_of_date"),
        )
    )

    rankings = top_cases.unionByName(top_deaths).unionByName(top_cfr).unionByName(top_in_region)

# =========================
# LEVEL 5
# =========================
anomalies = None

if should_run("5"):
    print("\n[Mức 5] Phát hiện bất thường...")

    df_anomaly = (
        df_country_daily
        .withColumn("Spike_threshold", (F.col("MA14_new_cases") + (F.lit(3.0) * F.col("STD14_new_cases"))).cast("double"))
        .withColumn("Is_spike", (F.col("New_cases") > F.col("Spike_threshold")) & F.col("Spike_threshold").isNotNull() & (F.col("New_cases") > 0))
        .withColumn("Is_negative_cases", F.col("New_cases") < 0)
        .withColumn("Is_negative_deaths", F.col("New_deaths") < 0)
    )

    spikes = (
        df_anomaly.filter(F.col("Is_spike"))
        .select(
            "Country", "Country_code", "WHO_region", "Date_reported",
            F.lit("SPIKE_NEW_CASES").alias("type"),
            "New_cases", "New_deaths", "MA14_new_cases", "STD14_new_cases", "Spike_threshold"
        )
    )

    negatives = (
        df_anomaly.filter(F.col("Is_negative_cases") | F.col("Is_negative_deaths"))
        .select(
            "Country", "Country_code", "WHO_region", "Date_reported",
            F.when(F.col("Is_negative_cases") & F.col("Is_negative_deaths"), F.lit("NEG_CASES_AND_DEATHS"))
             .when(F.col("Is_negative_cases"), F.lit("NEG_NEW_CASES"))
             .otherwise(F.lit("NEG_NEW_DEATHS")).alias("type"),
            "New_cases", "New_deaths", "MA14_new_cases", "STD14_new_cases", "Spike_threshold"
        )
    )

    anomalies = spikes.unionByName(negatives)

# =========================
# LEVEL 6
# =========================
segmentation_out = None

if should_run("6"):
    print("\n[Mức 6] Phân đoạn + peak...")

    q_cases = snapshot.approxQuantile("Total_cases", [0.25, 0.75], 0.01)
    q_cfr = snapshot.approxQuantile("CFR_percent", [0.25, 0.75], 0.01)
    cases_q25, cases_q75 = (q_cases + [0.0, 0.0])[:2]
    cfr_q25, cfr_q75 = (q_cfr + [0.0, 0.0])[:2]

    segments = (
        snapshot
        .withColumn(
            "Impact_segment",
            F.when((F.col("Total_cases") >= F.lit(cases_q75)) | (F.col("CFR_percent") >= F.lit(cfr_q75)), F.lit("High"))
             .when((F.col("Total_cases") <= F.lit(cases_q25)) & (F.col("CFR_percent") <= F.lit(cfr_q25)), F.lit("Low"))
             .otherwise(F.lit("Medium"))
        )
    )

    w_peak = Window.partitionBy("Country").orderBy(F.col("MA7_new_cases").desc_nulls_last(), F.col("Date_reported").desc())
    peak = (
        df_country_daily
        .select("Country", "Date_reported", "MA7_new_cases")
        .withColumn("rn", F.row_number().over(w_peak))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumnRenamed("Date_reported", "Peak_date")
        .withColumnRenamed("MA7_new_cases", "Peak_MA7_new_cases")
    )

    prepost = (
        df_analysis.select("Country", "Date_reported", "New_cases")
        .join(peak.select("Country", "Peak_date"), on="Country", how="inner")
        .withColumn("pre_flag", (F.col("Date_reported") >= F.date_add(F.col("Peak_date"), -30)) & (F.col("Date_reported") < F.col("Peak_date")))
        .withColumn("post_flag", (F.col("Date_reported") > F.col("Peak_date")) & (F.col("Date_reported") <= F.date_add(F.col("Peak_date"), 30)))
        .groupBy("Country")
        .agg(
            F.sum(F.when(F.col("pre_flag"), F.col("New_cases")).otherwise(F.lit(0))).cast("long").alias("Pre30_cases"),
            F.sum(F.when(F.col("post_flag"), F.col("New_cases")).otherwise(F.lit(0))).cast("long").alias("Post30_cases"),
        )
    )

    segmentation_out = (
        segments
        .join(peak, on="Country", how="left")
        .join(prepost, on="Country", how="left")
        .fillna({"Pre30_cases": 0, "Post30_cases": 0})
        .withColumn("As_of_date", F.lit(as_of_date))
    )

# =========================
# LEVEL 7
# =========================
quality_out = None

if should_run("7"):
    print("\n[Mức 7] Chất lượng dữ liệu...")

    df_raw_quality = df
    rows_raw = df_raw_quality.count()

    def _null_or_blank(c):
        return F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")

    quality_global = (
        df_raw_quality.agg(
            F.lit("global").alias("scope"),
            F.lit("all").alias("key"),
            F.lit(rows_raw).cast("long").alias("rows"),
            (F.sum(F.when(_null_or_blank("Country_code"), 1).otherwise(0)) / F.lit(rows_raw)).cast("double").alias("null_rate_country_code"),
            (F.sum(F.when(_null_or_blank("WHO_region"), 1).otherwise(0)) / F.lit(rows_raw)).cast("double").alias("null_rate_who_region"),
            (F.sum(F.when(_null_or_blank("Date_reported"), 1).otherwise(0)) / F.lit(rows_raw)).cast("double").alias("null_rate_date_reported"),
            (F.sum(F.when(_null_or_blank("New_cases"), 1).otherwise(0)) / F.lit(rows_raw)).cast("double").alias("null_rate_new_cases_raw"),
            (F.sum(F.when(_null_or_blank("Cumulative_cases"), 1).otherwise(0)) / F.lit(rows_raw)).cast("double").alias("null_rate_cum_cases_raw"),
            (F.sum(F.when(_null_or_blank("New_deaths"), 1).otherwise(0)) / F.lit(rows_raw)).cast("double").alias("null_rate_new_deaths_raw"),
            (F.sum(F.when(_null_or_blank("Cumulative_deaths"), 1).otherwise(0)) / F.lit(rows_raw)).cast("double").alias("null_rate_cum_deaths_raw"),
        )
    )

    neg_quality = (
        df_analysis.agg(
            (F.sum(F.when(F.col("New_cases") < 0, 1).otherwise(0)) / F.count(F.lit(1))).cast("double").alias("negative_rate_new_cases"),
            (F.sum(F.when(F.col("New_deaths") < 0, 1).otherwise(0)) / F.count(F.lit(1))).cast("double").alias("negative_rate_new_deaths"),
        )
    )

    quality_out = quality_global.crossJoin(neg_quality)

# =========================
# Ghi STATS theo level được chạy
# =========================
print("\nĐang ghi STATS theo nhóm index vào Elasticsearch...")

if should_run("1"):
    country_sender = partial(send_to_es_index_chunked, ES_COUNTRY_STATS_INDEX, ["Country"])
    region_sender = partial(send_to_es_index_chunked, ES_REGION_STATS_INDEX, ["WHO_region"])
    global_daily_sender = partial(send_to_es_index_chunked, ES_GLOBAL_DAILY_INDEX, ["Date_reported"])

    country_stats_lvl1_ranked.rdd.map(lambda r: r.asDict()).repartition(1).foreachPartition(country_sender)
    region_stats_lvl1.rdd.map(lambda r: r.asDict()).repartition(1).foreachPartition(region_sender)
    daily_stats_lvl1.rdd.map(lambda r: r.asDict()).repartition(1).foreachPartition(global_daily_sender)

if should_run("23"):
    country_daily_sender = partial(send_to_es_index_chunked, ES_COUNTRY_DAILY_INDEX, ["Country", "Date_reported"])
    df_country_daily.rdd.map(lambda r: r.asDict()).repartition(1).foreachPartition(country_daily_sender)

if should_run("5"):
    anomalies_sender = partial(send_to_es_index_chunked, ES_ANOMALIES_INDEX, ["Country", "Date_reported", "type"])
    anomalies.rdd.map(lambda r: r.asDict()).repartition(1).foreachPartition(anomalies_sender)

if should_run("6"):
    seg_sender = partial(send_to_es_index_chunked, ES_SEGMENTATION_INDEX, ["Country"])
    segmentation_out.rdd.map(lambda r: r.asDict()).repartition(1).foreachPartition(seg_sender)

if should_run("7"):
    quality_sender = partial(send_to_es_index_chunked, ES_QUALITY_INDEX, ["scope", "key"])
    quality_out.rdd.map(lambda r: r.asDict()).repartition(1).foreachPartition(quality_sender)

if should_run("4"):
    rank_sender = partial(send_to_es_index_chunked, ES_RANKINGS_INDEX, ["metric", "WHO_region", "rank"])
    rankings.rdd.map(lambda r: r.asDict()).repartition(1).foreachPartition(rank_sender)

# =========================
# In endpoints (chỉ in cái liên quan)
# =========================
if should_run("1"):
    print(f"Stats country: {ES_HOST}/{ES_COUNTRY_STATS_INDEX}/_count")
    print(f"Stats region : {ES_HOST}/{ES_REGION_STATS_INDEX}/_count")
    print(f"TS global    : {ES_HOST}/{ES_GLOBAL_DAILY_INDEX}/_count")
if should_run("23"):
    print(f"TS country   : {ES_HOST}/{ES_COUNTRY_DAILY_INDEX}/_count")
if should_run("5"):
    print(f"Anomalies    : {ES_HOST}/{ES_ANOMALIES_INDEX}/_count")
if should_run("6"):
    print(f"Segmentation : {ES_HOST}/{ES_SEGMENTATION_INDEX}/_count")
if should_run("7"):
    print(f"Quality      : {ES_HOST}/{ES_QUALITY_INDEX}/_count")
if should_run("4"):
    print(f"Rankings     : {ES_HOST}/{ES_RANKINGS_INDEX}/_count")

# Cleanup cache
if df_analysis is not None:
    df_analysis.unpersist()

# =========================
# RAW write chỉ khi chạy raw
# =========================
if should_run("raw"):
    print("\nĐang ghi dữ liệu vào Elasticsearch...")
    df_cleaned.rdd.map(lambda row: row.asDict()).repartition(1).foreachPartition(send_to_elasticsearch)
    print(f"Kiểm tra tại: {ES_HOST}/{ES_INDEX}/_count")
else:
    print("\nSkip ghi RAW (không chạy level raw)")

print(f"\nHoàn thành! Đã xử lý {total_count:,} records")
spark.stop()
