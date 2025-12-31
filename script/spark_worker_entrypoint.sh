#!/bin/bash

until timeout 1s bash -c 'cat < /dev/null > /dev/tcp/spark-master/7077' 2>/dev/null; do
  echo 'Waiting for spark-master...'
  sleep 2
done

echo "Spark Master is up! Starting Worker..."
/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
