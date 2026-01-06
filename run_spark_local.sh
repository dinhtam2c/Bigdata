#!/bin/bash

echo "Running Spark HDFS to Elasticsearch job locally..."

# Update script
kubectl delete configmap spark-script --ignore-not-found
kubectl create configmap spark-script --from-file=spark_hdfs_to_elasticsearch.py=src/spark_hdfs_to_elasticsearch.py

# Delete old job if exists
kubectl delete job spark-hdfs-to-es --ignore-not-found
kubectl wait --for=delete pod -l job-name=spark-hdfs-to-es --timeout=60s 2>/dev/null

# Apply job
kubectl apply -f k8s-manifests/spark-job.yaml

# Wait and follow logs
echo "Waiting for Spark job pod..."
sleep 5

POD_NAME=$(kubectl get pods -l job-name=spark-hdfs-to-es -o jsonpath="{.items[0].metadata.name}")

if [ -z "$POD_NAME" ]; then
  echo "Waiting longer for pod..."
  sleep 10
  POD_NAME=$(kubectl get pods -l job-name=spark-hdfs-to-es -o jsonpath="{.items[0].metadata.name}")
fi

if [ -n "$POD_NAME" ]; then
  echo "Following logs for pod: $POD_NAME"
  kubectl logs -f $POD_NAME
else
  echo "Error: Could not find Spark job pod"
  exit 1
fi
