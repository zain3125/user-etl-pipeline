#!/bin/bash

/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master &

echo "Waiting for Spark Master to start..."
sleep 20

chmod +x /opt/spark/spark_stream.py

echo "Submitting Spark Job..."
/opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
    /opt/spark/spark_stream.py

EXIT_CODE=$?
echo "Spark process finished with code: $EXIT_CODE"
exit $EXIT_CODE
