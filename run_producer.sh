#!/bin/bash

JOB_NAME="covid-producer-job"

# 1. Cleanup: Kiểm tra và xóa Job cũ nếu tồn tại
echo "Checking for existing job..."
if kubectl get job $JOB_NAME &>/dev/null; then
  echo "Found existing job '$JOB_NAME'. Deleting..."
  kubectl delete job $JOB_NAME
  
  # Chờ cho Pod cũ bị xóa hẳn
  echo "Waiting for old pods to terminate..."
  kubectl wait --for=delete pod -l job-name=$JOB_NAME --timeout=60s
fi

# 2. Update ConfigMap từ file code bên ngoài
# Xóa ConfigMap cũ trước để đảm bảo cập nhật mới nhất
kubectl delete configmap producer-script --ignore-not-found
echo "Creating ConfigMap from src/kafka_producer.py..."
kubectl create configmap producer-script --from-file=kafka_producer.py=src/kafka_producer.py

# 3. Apply Job Manifest
echo "Deploying Job..."
kubectl apply -f k8s-manifests/producer.yaml

echo "Waiting for Job Pod to be created..."
sleep 5

# 4. Tìm Pod và copy dữ liệu
POD_NAME=$(kubectl get pods -l job-name=$JOB_NAME -o jsonpath="{.items[0].metadata.name}")

if [ -z "$POD_NAME" ]; then
  echo "Error: Could not find Pod for job $JOB_NAME. Retrying..."
  sleep 5
  POD_NAME=$(kubectl get pods -l job-name=$JOB_NAME -o jsonpath="{.items[0].metadata.name}")
fi

if [ -z "$POD_NAME" ]; then
  echo "FATAL: Still cannot find pod."
  exit 1
fi

echo "Found Pod: $POD_NAME"

# 5. Chờ Pod ở trạng thái Running
echo "Waiting for Pod to trigger (it needs to be running to accept file copy)..."
# Job này chạy lệnh pip install trước, nên sẽ mất chút thời gian để Running
kubectl wait --for=condition=Ready pod/$POD_NAME --timeout=120s

# 6. Copy dữ liệu
echo "Copying data file to Pod..."
kubectl cp data-sources/covid_0.csv $POD_NAME:/app/data/covid_0.csv

# 7. Log
echo "Data copied. Tailing logs..."
kubectl logs -f $POD_NAME
