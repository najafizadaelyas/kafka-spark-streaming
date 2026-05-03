#!/usr/bin/env bash
# Submit the PySpark streaming job to the Spark master.
# Usage: ./scripts/run_spark_job.sh

SPARK_MASTER=${SPARK_MASTER:-local[*]}

spark-submit \
  --master "$SPARK_MASTER" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  spark_streaming/stream_processor.py
