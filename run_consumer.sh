#!/bin/bash

# 1. Update ConfigMap cho Consumer
kubectl delete configmap consumer-script --ignore-not-found
echo "Creating ConfigMap from src/kafka_consumer.py..."
kubectl create configmap consumer-script --from-file=kafka_consumer.py=src/kafka_to_hdfs.py

# 2. Apply Deployment
echo "Deploying Consumer..."
kubectl apply -f k8s-manifests/consumer.yaml
kubectl rollout restart deployment/consumer

# 3. Theo dõi trạng thái
echo "Waiting for Consumer Pod..."
kubectl rollout status deployment/consumer

echo "Consumer is running! Check logs with:"
echo "kubectl logs -l app=consumer -f"
