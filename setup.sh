#!/bin/bash

# 1. Xóa cluster cũ nếu có (để dọn dẹp môi trường)
k3d cluster delete bigdata 2>/dev/null

# 2. Tạo cluster mới
k3d cluster create bigdata --servers 1 --agents 2 \
  --api-port 0.0.0.0:6443 \
  -p "9092:30092@agent:0" \
  -p "9870:30870@agent:0" \
  -p "8020:30020@agent:0" \
  -p "9866:30866@agent:1" \
  -p "9864:30864@agent:1" \
  -p "9867:30867@agent:1" \
  -p "8080:30080@agent:0" \
  -p "7077:30077@agent:0" \
  -p "9200:32000@agent:0" \
  -p "5601:32601@agent:0" \
  --k3s-arg "--disable=traefik@server:0"

# 3. Áp dụng các manifest
kubectl apply -f k8s-manifests/kafka.yaml
kubectl apply -f k8s-manifests/hdfs.yaml
kubectl apply -f k8s-manifests/spark.yaml
kubectl apply -f k8s-manifests/elastic-kibana.yaml

# 4. Chờ cho các pod sẵn sàng
echo "Waiting for core infrastructure pods..."
kubectl wait --for=condition=Ready pod -l app=kafka --timeout=300s
kubectl wait --for=condition=Ready pod -l app=hdfs-namenode --timeout=300s
kubectl wait --for=condition=Ready pod -l app=hdfs-datanode --timeout=300s
kubectl wait --for=condition=Ready pod -l app=elasticsearch --timeout=300s

echo "Waiting for application pods..."
kubectl wait --for=condition=Ready pod -l app=spark-master --timeout=120s 2>/dev/null || echo "Spark master not ready yet (non-critical)"
kubectl wait --for=condition=Ready pod -l app=spark-worker --timeout=120s 2>/dev/null || echo "Spark worker not ready yet (non-critical)"
kubectl wait --for=condition=Ready pod -l app=kibana --timeout=120s 2>/dev/null || echo "Kibana not ready yet (non-critical)"

# 5. Chờ HDFS NameNode thoát safe mode
echo "Waiting for HDFS NameNode (safe mode check)..."
TIMEOUT=120
ELAPSED=0
until kubectl exec hdfs-namenode-0 -- hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
  if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "WARNING: HDFS NameNode timeout after ${TIMEOUT}s, continuing anyway..."
    break
  fi
  sleep 3
  ELAPSED=$((ELAPSED + 3))
done
echo "HDFS NameNode ready!"

# 6. Chờ Kafka Broker sẵn sàng
echo "Waiting for Kafka broker..."
TIMEOUT=90
ELAPSED=0
until kubectl exec kafka-0 -- /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 &>/dev/null; do
  if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "ERROR: Kafka broker not ready after ${TIMEOUT}s"
    exit 1
  fi
  sleep 3
  ELAPSED=$((ELAPSED + 3))
done
echo "Kafka broker ready!"

# 7. Tạo các topic Kafka
TOPICS=("covid-raw")

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC"
  kubectl exec kafka-0 -- /opt/kafka/bin/kafka-topics.sh --create \
    --topic $TOPIC \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=86400000
done

echo "Infrastructure is ready!"
kubectl get pods
kubectl get svc
