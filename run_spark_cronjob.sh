#!/bin/bash

echo "Deploying Spark CronJob (runs every 2 minutes)..."

# Update script
kubectl delete configmap spark-script --ignore-not-found
kubectl create configmap spark-script --from-file=spark_hdfs_to_elasticsearch.py=src/spark_hdfs_to_elasticsearch.py

# Deploy CronJob
kubectl delete cronjob spark-hdfs-to-es-cron --ignore-not-found
kubectl apply -f k8s-manifests/spark-cronjob.yaml

echo ""
echo "✅ CronJob deployed successfully!"
echo ""
echo "CronJob will run every 2 minutes automatically."
echo ""
echo "Useful commands:"
echo "  • View cronjob:        kubectl get cronjob"
echo "  • View job history:    kubectl get jobs"
echo "  • View latest logs:    kubectl logs -l job-name=spark-hdfs-to-es-cron --tail=50"
echo "  • Trigger manually:    kubectl create job --from=cronjob/spark-hdfs-to-es-cron spark-manual-\$(date +%s)"
echo "  • Stop cronjob:        kubectl delete cronjob spark-hdfs-to-es-cron"
echo ""
