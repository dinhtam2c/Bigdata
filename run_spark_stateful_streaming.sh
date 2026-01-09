#!/bin/bash
set -euo pipefail

kubectl delete configmap spark-stateful-script --ignore-not-found
kubectl create configmap spark-stateful-script \
  --from-file=spark_kafka_stateful_realtime.py=src/spark_kafka_stateful_realtime.py

kubectl apply -f k8s-manifests/spark-stateful-streaming.yaml
kubectl rollout restart deployment/spark-stateful-streaming
kubectl rollout status deployment/spark-stateful-streaming

echo "Logs:"
echo "kubectl logs -l app=spark-stateful-streaming -f"
