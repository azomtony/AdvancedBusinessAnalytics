#!/bin/bash

python3 src/kafka_topic_producer.py &

/opt/spark/bin/spark-submit   --master spark://192.168.64.5:7077   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0   --driver-class-path /home/ubuntu/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar:/home/ubuntu/.ivy2/jars/org.mongodb_mongodb-driver-sync-4.8.2.jar:/home/ubuntu/.ivy2/jars/org.mongodb_mongodb-driver-core-4.8.2.jar:/home/ubuntu/.ivy2/jars/org.mongodb_bson-4.8.2.jar   --conf "spark.executor.extraClassPath=/home/ubuntu/.ivy2/jars/org.apache.commons_commons-pool2-2.11.1.jar:/home/ubuntu/.ivy2/jars/org.mongodb_mongodb-driver-sync-4.8.2.jar:/home/ubuntu/.ivy2/jars/org.mongodb_mongodb-driver-core-4.8.2.jar:/home/ubuntu/.ivy2/jars/org.mongodb_bson-4.8.2.jar"  src/spark_stream_processing.py & #change the path for your spark executables

python3 src/dash_analytics.py &

wait