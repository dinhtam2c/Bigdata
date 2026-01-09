#!/bin/bash

# Usage: 
#   bash run_producer.sh           # Batch mode (gửi tất cả nhanh)
#   bash run_producer.sh streaming # Streaming mode (gửi từng ngày)

MODE=${1:-batch}

if [ "$MODE" = "streaming" ]; then
  echo "=== STREAMING MODE: Gửi từng ngày (0.5s/ngày) ==="
  JOB_NAME="covid-producer-streaming"
  SCRIPT_FILE="kafka_producer_streaming.py"
  CONFIGMAP_NAME="producer-streaming-script"
  MANIFEST_FILE="producer-streaming.yaml"
  POD_LABEL="app=covid-producer-streaming"
else
  echo "=== BATCH MODE: Gửi toàn bộ dữ liệu nhanh nhất ==="
  JOB_NAME="covid-producer-job"
  SCRIPT_FILE="kafka_producer.py"
  CONFIGMAP_NAME="producer-script"
  MANIFEST_FILE="producer.yaml"
  POD_LABEL="job-name=covid-producer-job"
fi

# 1. Cleanup old job
if kubectl get job $JOB_NAME &>/dev/null; then
  echo "Deleting old job..."
  kubectl delete job $JOB_NAME
  kubectl wait --for=delete pod -l $POD_LABEL --timeout=60s 2>/dev/null || true
fi

# 2. Update ConfigMap
kubectl delete configmap $CONFIGMAP_NAME --ignore-not-found
echo "Creating ConfigMap from src/$SCRIPT_FILE..."
kubectl create configmap $CONFIGMAP_NAME --from-file=${SCRIPT_FILE%.*}.py=src/$SCRIPT_FILE

# 3. Apply Manifest
echo "Deploying $MODE producer..."
kubectl create -f k8s-manifests/$MANIFEST_FILE

echo "Waiting for Pod to be created..."
sleep 5

# 4. Tìm Pod và copy dữ liệu
POD_NAME=$(kubectl get pods -l $POD_LABEL -o jsonpath="{.items[0].metadata.name}")

if [ -z "$POD_NAME" ]; then
  echo "Error: Could not find Pod. Retrying..."
  sleep 5
  POD_NAME=$(kubectl get pods -l $POD_LABEL -o jsonpath="{.items[0].metadata.name}")
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
