#!/bin/sh

mkdir /tmp/.ivy

exec /opt/spark/bin/spark-submit \
--packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 \
--conf spark.jars.ivy=/tmp/.ivy \
--conf spark.sql.shuffle.partitions=50 \
--conf spark.executor.memory=2g \
--conf spark.driver.cores=1 \
--conf spark.ui.retainedJobs=200 \
--num-executors=1 \
/opt/application/etl_pipeline.py
