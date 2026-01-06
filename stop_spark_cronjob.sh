#!/bin/bash

echo "Stopping Spark CronJob..."
kubectl delete cronjob spark-hdfs-to-es-cron

echo "Cleaning up old jobs..."
kubectl delete jobs -l app=spark-cron

echo "✅ CronJob stopped and cleaned up!"
