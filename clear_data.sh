#!/bin/bash

echo "=== Clearing all data from Kafka, HDFS, and Elasticsearch ==="

# 1. Xóa và tạo lại Kafka topic
echo ""
echo "1. Clearing Kafka topics..."
TOPICS=("covid-raw")

for TOPIC in "${TOPICS[@]}"; do
  echo "  - Deleting topic: $TOPIC"
  kubectl exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --delete \
    --topic $TOPIC \
    --bootstrap-server localhost:9092 2>/dev/null || echo "    Topic $TOPIC không tồn tại"
  
  sleep 2
  
  echo "  - Recreating topic: $TOPIC"
  kubectl exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --create \
    --topic $TOPIC \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=86400000
done

# 2. Xóa dữ liệu trong HDFS
echo ""
echo "2. Clearing HDFS data..."
kubectl exec hdfs-namenode-0 -- hdfs dfs -rm -r -f /covid 2>/dev/null || echo "  No data in HDFS to clear"
echo "  HDFS cleared"

# 3. Xóa indices trong Elasticsearch
echo ""
echo "3. Clearing Elasticsearch indices..."
ES_POD=$(kubectl get pod -l app=elasticsearch -o jsonpath='{.items[0].metadata.name}')
kubectl exec $ES_POD -- curl -X DELETE "localhost:9200/covid-data" 2>/dev/null || echo "  No indices to delete"
echo "  Elasticsearch indices cleared"

echo ""
echo "=== Data clearing completed! ==="
