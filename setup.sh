#!/bin/bash

# 1. Xóa cluster cũ nếu có (để dọn dẹp môi trường)
k3d cluster delete bigdata 2>/dev/null

# 2. Tạo cluster mới
k3d cluster create bigdata --servers 1 --agents 2 \
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
# Đảm bảo bạn đã ở đúng thư mục chứa folder k8s-manifests
kubectl apply -f k8s-manifests/

# 4. Chờ cho các pod sẵn sàng
echo "Waiting for pods to be ready..."
kubectl wait --for=condition=Ready pods --all --namespace default --timeout=600s

# 5. Chờ HDFS NameNode sẵn sàng
echo "Waiting for HDFS NameNode to be ready..."
until kubectl exec hdfs-namenode-0 -- hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
  echo "HDFS still in safe mode, waiting..."
  sleep 5
done
echo "HDFS NameNode is ready!"

# 6. Chờ Kafka Broker sẵn sàng
echo "Waiting for Kafka broker to be ready..."
for i in {1..30}; do
  if kubectl exec kafka-0 -- /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 &>/dev/null; then
    echo "Kafka broker is ready!"
    break
  fi
  echo "Kafka not ready yet, waiting... ($i/30)"
  sleep 5
done

# 7. Tạo các topic Kafka
TOPICS=("covid-test" "covid-raw" "covid-processed")

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
