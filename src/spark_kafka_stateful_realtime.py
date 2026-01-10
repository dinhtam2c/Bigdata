from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Dict, Iterable, List, Tuple
from urllib import request
from urllib.error import HTTPError, URLError

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DateType,
    TimestampType,
)

# Env / Config
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("TOPIC_NAME", "covid-raw")

ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
RT_SUFFIX = os.getenv("RT_SUFFIX", "_rt")

TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "20 seconds")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
FAIL_ON_DATA_LOSS = os.getenv("FAIL_ON_DATA_LOSS", "false")

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "hdfs://hdfs-namenode-0.hdfs-namenode:8020/covid/checkpoints/rt_simple",
)

SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")

ES_TIMEOUT_SECS = float(os.getenv("ES_TIMEOUT_SECS", "10"))
ES_RETRIES = int(os.getenv("ES_RETRIES", "3"))
ES_RETRY_BACKOFF_SECS = float(os.getenv("ES_RETRY_BACKOFF_SECS", "1.5"))

# Bulk tuning
BULK_DOCS_PER_FLUSH = int(os.getenv("BULK_DOCS_PER_FLUSH", "2000"))

INDEX_SETTINGS = {"number_of_shards": 1, "number_of_replicas": 0}

COMMON_MAPPINGS = {
    "properties": {
        "Country": {"type": "keyword"},
        "Country_code": {"type": "keyword"},
        "WHO_region": {"type": "keyword"},
        "Date_reported": {"type": "date"},

        "New_cases": {"type": "long"},
        "New_deaths": {"type": "long"},
        "Cumulative_cases": {"type": "long"},
        "Cumulative_deaths": {"type": "long"},

        "Total_new_cases": {"type": "long"},
        "Total_new_deaths": {"type": "long"},
        "Countries_reported": {"type": "integer"},

        "event_ts": {"type": "date"}
    }
}


# Which outputs to send
SEND_RAW = os.getenv("SEND_RAW", "false").lower() == "true"
SEND_COUNTRY_DAILY = os.getenv("SEND_COUNTRY_DAILY", "true").lower() == "true"
SEND_REGION_DAILY = os.getenv("SEND_REGION_DAILY", "true").lower() == "true"
SEND_GLOBAL_DAILY = os.getenv("SEND_GLOBAL_DAILY", "true").lower() == "true"


def _rt_index(env_key: str, base_default: str) -> str:
    name = os.getenv(env_key, base_default)
    suf = str(RT_SUFFIX or "").strip()
    if suf and not name.endswith(suf):
        return f"{name}{suf}"
    return name


# Indices
ES_INDEX_RAW = _rt_index("ES_INDEX_RAW", "covid-data")
ES_COUNTRY_DAILY_INDEX = _rt_index("ES_COUNTRY_DAILY_INDEX", "covid-ts-country-daily")
ES_REGION_DAILY_INDEX = _rt_index("ES_REGION_DAILY_INDEX", "covid-ts-region-daily")
ES_GLOBAL_DAILY_INDEX = _rt_index("ES_GLOBAL_DAILY_INDEX", "covid-ts-global-daily")

COVID_SCHEMA = StructType(
    [
        StructField("Country", StringType(), True),
        StructField("Country_code", StringType(), True),
        StructField("WHO_region", StringType(), True),
        StructField("Date_reported", StringType(), True),
        StructField("New_cases", StringType(), True),
        StructField("Cumulative_cases", StringType(), True),
        StructField("New_deaths", StringType(), True),
        StructField("Cumulative_deaths", StringType(), True),
    ]
)

# ES helpers
def _sanitize_id_part(v: Any) -> str:
    s = "" if v is None else str(v)
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-zA-Z0-9_.\-:@]+", "_", s)
    return s[:512] if len(s) > 512 else s


def _http_request(
    method: str,
    url: str,
    body_bytes: bytes | None = None,
    headers: Dict[str, str] | None = None,
    timeout: float = ES_TIMEOUT_SECS,
    retries: int = ES_RETRIES,
) -> Tuple[int, bytes]:
    last_err: Exception | None = None
    headers = headers or {}
    for attempt in range(retries + 1):
        try:
            req = request.Request(url, data=body_bytes, headers=headers, method=method)
            with request.urlopen(req, timeout=timeout) as resp:
                return int(resp.status), resp.read()
        except (HTTPError, URLError, TimeoutError) as e:
            last_err = e
            if attempt < retries:
                time.sleep(ES_RETRY_BACKOFF_SECS * (2**attempt))
                continue
            break
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(ES_RETRY_BACKOFF_SECS * (2**attempt))
                continue
            break
    raise RuntimeError(f"HTTP {method} failed: {url} err={last_err!r}")


def _bulk_flush(lines: List[str]) -> None:
    if not lines:
        return
    body = "\n".join(lines) + "\n"
    try:
        status, payload_bytes = _http_request(
            "POST",
            f"{ES_HOST}/_bulk",
            body_bytes=body.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
        )
        if status != 200:
            print(f"[ES] bulk status={status}")
            return
        payload = json.loads(payload_bytes.decode("utf-8"))
        if payload.get("errors"):
            # Avoid printing huge payload; just a signal
            print("[ES] bulk had errors (some docs failed)")
    except Exception as e:
        print(f"[ES] bulk error: {e!r}")


def send_docs_partition(partition: Iterable[Tuple[str, str, str]]) -> None:
    lines: List[str] = []
    docs = 0
    for index_name, doc_id, body_json in partition:
        meta = {"index": {"_index": index_name, "_id": doc_id}}
        lines.append(json.dumps(meta, ensure_ascii=False))
        lines.append(body_json)
        docs += 1
        if docs >= BULK_DOCS_PER_FLUSH:
            _bulk_flush(lines)
            lines = []
            docs = 0
    _bulk_flush(lines)


_INDICES_READY = False


def create_index_if_missing(index_name: str) -> None:
    try:
        status, _ = _http_request(
            "HEAD",
            f"{ES_HOST}/{index_name}",
            timeout=ES_TIMEOUT_SECS,
            retries=1,
        )
        if status == 200:
            return
    except RuntimeError as e:
        if "404" not in str(e):
            print(f"[ES] HEAD {index_name} error: {e}")
            return

    body = {
        "settings": INDEX_SETTINGS,
        "mappings": COMMON_MAPPINGS,
    }

    try:
        status, _ = _http_request(
            "PUT",
            f"{ES_HOST}/{index_name}",
            body_bytes=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        print(f"[ES] Create {index_name}: status={status}")
    except Exception as e:
        print(f"[ES] Create {index_name} failed: {e!r}")



def ensure_indices() -> None:
    global _INDICES_READY
    if _INDICES_READY:
        return
    for idx in [ES_COUNTRY_DAILY_INDEX, ES_REGION_DAILY_INDEX, ES_GLOBAL_DAILY_INDEX]:
        create_index_if_missing(idx)
    _INDICES_READY = True
    print(f"[RT] Indices ready, suffix={RT_SUFFIX!r}")


# Checkpoint dir helpers
def ensure_checkpoint_dir(spark: SparkSession, path: str) -> None:
    if path.startswith("hdfs://"):
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        hp = spark._jvm.org.apache.hadoop.fs.Path(path)
        if not fs.exists(hp):
            ok = fs.mkdirs(hp)
            print(f"[CKPT] mkdirs {path} -> {ok}")
    else:
        p = path
        if p.startswith("file:"):
            p = p[len("file:") :]
        os.makedirs(p, exist_ok=True)
        print(f"[CKPT] mkdirs {path} -> True")


# ForeachBatch: compute simple metrics only
def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        print(f"[BATCH] empty batch_id={batch_id}")
        return

    spark = batch_df.sparkSession
    ensure_indices()

    sanitize_udf = F.udf(lambda x: _sanitize_id_part(x), StringType())

    docs_dfs: List[DataFrame] = []

    # 1) RAW (optional)
    if SEND_RAW:
        raw_docs = (
            batch_df.select(
                "topic",
                "partition",
                "offset",
                "Country",
                "Country_code",
                "WHO_region",
                "Date_reported",
                "New_cases",
                "New_deaths",
                "Cumulative_cases",
                "Cumulative_deaths",
                "event_ts",
            )
            .withColumn(
                "doc_id",
                F.concat_ws(
                    "_",
                    sanitize_udf(F.col("topic")),
                    F.col("partition").cast("string"),
                    F.col("offset").cast("string"),
                ),
            )
            .withColumn(
                "body_json",
                F.to_json(
                    F.struct(
                        F.col("topic"),
                        F.col("partition"),
                        F.col("offset"),
                        F.col("Country"),
                        F.col("Country_code"),
                        F.col("WHO_region"),
                        F.date_format(F.col("Date_reported"), "yyyy-MM-dd").alias("Date_reported"),
                        F.col("New_cases"),
                        F.col("New_deaths"),
                        F.col("Cumulative_cases"),
                        F.col("Cumulative_deaths"),
                        F.col("event_ts"),
                    )
                ),
            )
            .select(F.lit(ES_INDEX_RAW).alias("index_name"), "doc_id", "body_json")
        )
        docs_dfs.append(raw_docs)

    # 2) Country daily
    if SEND_COUNTRY_DAILY:
        country_daily = (
            batch_df.groupBy("Country", "Country_code", "WHO_region", "Date_reported")
            .agg(
                F.sum("New_cases").cast("long").alias("New_cases"),
                F.sum("New_deaths").cast("long").alias("New_deaths"),
                F.max("Cumulative_cases").cast("long").alias("Cumulative_cases"),
                F.max("Cumulative_deaths").cast("long").alias("Cumulative_deaths"),
                F.max("event_ts").cast(TimestampType()).alias("event_ts"),
            )
            .withColumn(
                "doc_id",
                F.concat_ws(
                    "_",
                    sanitize_udf(F.col("Country")),
                    F.date_format(F.col("Date_reported"), "yyyy-MM-dd"),
                ),
            )
            .withColumn(
                "body_json",
                F.to_json(
                    F.struct(
                        F.col("Country"),
                        F.col("Country_code"),
                        F.col("WHO_region"),
                        F.date_format(F.col("Date_reported"), "yyyy-MM-dd").alias("Date_reported"),
                        F.col("New_cases"),
                        F.col("New_deaths"),
                        F.col("Cumulative_cases"),
                        F.col("Cumulative_deaths"),
                        F.col("event_ts"),
                    )
                ),
            )
            .select(F.lit(ES_COUNTRY_DAILY_INDEX).alias("index_name"), "doc_id", "body_json")
        )
        docs_dfs.append(country_daily)

    # 3) Region daily
    if SEND_REGION_DAILY:
        region_daily = (
            batch_df.groupBy("WHO_region", "Date_reported")
            .agg(
                F.sum("New_cases").cast("long").alias("Total_new_cases"),
                F.sum("New_deaths").cast("long").alias("Total_new_deaths"),
                F.approx_count_distinct("Country").cast("int").alias("Countries_reported"),
                F.max("event_ts").cast(TimestampType()).alias("event_ts"),
            )
            .withColumn(
                "doc_id",
                F.concat_ws(
                    "_",
                    sanitize_udf(F.col("WHO_region")),
                    F.date_format(F.col("Date_reported"), "yyyy-MM-dd"),
                ),
            )
            .withColumn(
                "body_json",
                F.to_json(
                    F.struct(
                        F.col("WHO_region"),
                        F.date_format(F.col("Date_reported"), "yyyy-MM-dd").alias("Date_reported"),
                        F.col("Total_new_cases"),
                        F.col("Total_new_deaths"),
                        F.col("Countries_reported"),
                        F.col("event_ts"),
                    )
                ),
            )
            .select(F.lit(ES_REGION_DAILY_INDEX).alias("index_name"), "doc_id", "body_json")
        )
        docs_dfs.append(region_daily)

    # 4) Global daily
    if SEND_GLOBAL_DAILY:
        global_daily = (
            batch_df.groupBy("Date_reported")
            .agg(
                F.sum("New_cases").cast("long").alias("Total_new_cases"),
                F.sum("New_deaths").cast("long").alias("Total_new_deaths"),
                F.approx_count_distinct("Country").cast("int").alias("Countries_reported"),
                F.max("event_ts").cast(TimestampType()).alias("event_ts"),
            )
            .withColumn("doc_id", F.date_format(F.col("Date_reported"), "yyyy-MM-dd"))
            .withColumn(
                "body_json",
                F.to_json(
                    F.struct(
                        F.date_format(F.col("Date_reported"), "yyyy-MM-dd").alias("Date_reported"),
                        F.col("Total_new_cases"),
                        F.col("Total_new_deaths"),
                        F.col("Countries_reported"),
                        F.col("event_ts"),
                    )
                ),
            )
            .select(F.lit(ES_GLOBAL_DAILY_INDEX).alias("index_name"), "doc_id", "body_json")
        )
        docs_dfs.append(global_daily)

    if not docs_dfs:
        print(f"[BATCH] nothing to send (all SEND_* disabled) batch_id={batch_id}")
        return

    to_send = docs_dfs[0]
    for d in docs_dfs[1:]:
        to_send = to_send.unionByName(d)

    (
        to_send.rdd.map(lambda r: (r["index_name"], r["doc_id"], r["body_json"]))
        .foreachPartition(send_docs_partition)
    )

    print(f"[BATCH] processed batch_id={batch_id}")


def main() -> None:
    spark = (
        SparkSession.builder.appName("COVID RT Kafka->ES (simple, no store)")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ensure_checkpoint_dir(spark, CHECKPOINT_LOCATION)
    ensure_indices()

    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", FAIL_ON_DATA_LOSS)
        .load()
    )

    df_json = df_kafka.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.col("value").cast("string").alias("value_str"),
    )

    df_parsed = df_json.select(
        "topic",
        "partition",
        "offset",
        "kafka_ts",
        F.from_json("value_str", COVID_SCHEMA).alias("j"),
    ).select("topic", "partition", "offset", "kafka_ts", "j.*")

    def to_int(colname: str):
        return F.when(F.trim(F.col(colname)) == "", F.lit(0)).otherwise(F.col(colname).cast(IntegerType()))

    def to_long(colname: str):
        return F.when(F.trim(F.col(colname)) == "", F.lit(0)).otherwise(F.col(colname).cast(LongType()))

    df_clean = (
        df_parsed.select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("kafka_ts"),
            F.col("Country").cast(StringType()).alias("Country"),
            F.col("Country_code").cast(StringType()).alias("Country_code"),
            F.col("WHO_region").cast(StringType()).alias("WHO_region"),
            F.to_date(F.col("Date_reported").cast("string"), "yyyy-MM-dd").alias("Date_reported"),
            to_int("New_cases").alias("New_cases"),
            to_long("Cumulative_cases").alias("Cumulative_cases"),
            to_int("New_deaths").alias("New_deaths"),
            to_long("Cumulative_deaths").alias("Cumulative_deaths"),
        )
        .filter(F.col("Country").isNotNull() & F.col("Date_reported").isNotNull())
        .withColumn("event_ts", F.to_timestamp(F.col("Date_reported").cast("string"), "yyyy-MM-dd"))
    )

    q = (
        df_clean.writeStream.foreachBatch(process_batch)
        .outputMode("update")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    print(
        f"""
[STARTED]
Kafka={BOOTSTRAP_SERVERS}/{TOPIC_NAME}
Trigger={TRIGGER_INTERVAL}
Offsets={STARTING_OFFSETS}
failOnDataLoss={FAIL_ON_DATA_LOSS}

[CHECKPOINT]
{CHECKPOINT_LOCATION}

[ES]
host={ES_HOST}
indices:
  raw={ES_INDEX_RAW} (SEND_RAW={SEND_RAW})
  country_daily={ES_COUNTRY_DAILY_INDEX} (SEND_COUNTRY_DAILY={SEND_COUNTRY_DAILY})
  region_daily={ES_REGION_DAILY_INDEX} (SEND_REGION_DAILY={SEND_REGION_DAILY})
  global_daily={ES_GLOBAL_DAILY_INDEX} (SEND_GLOBAL_DAILY={SEND_GLOBAL_DAILY})
"""
    )

    q.awaitTermination()


if __name__ == "__main__":
    main()