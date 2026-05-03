"""
PySpark Structured Streaming job.

Pipeline:
  Kafka → parse JSON → enforce schema → transform → write to Delta Lake
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# ---------------------------------------------------------------------------
# Schema — mirrors schemas/event_schema.py EVENT_SCHEMA
# ---------------------------------------------------------------------------
EVENT_STRUCT = StructType([
    StructField("event_id",   StringType(),    False),
    StructField("user_id",    IntegerType(),   False),
    StructField("event_type", StringType(),    False),
    StructField("product_id", StringType(),    False),
    StructField("amount",     DoubleType(),    False),
    StructField("timestamp",  StringType(),    False),
])

VALID_EVENT_TYPES = {"click", "view", "purchase", "add_to_cart"}


def create_spark_session(app_name: str = "KafkaSparkDeltaStreaming") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.0.0")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_validate(raw_df):
    """Parse JSON payload and drop malformed / schema-violating rows."""
    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
        .select(from_json(col("json_str"), EVENT_STRUCT).alias("data"), col("kafka_ts"))
        .select("data.*", "kafka_ts")
    )

    # Drop rows where required fields are null (JSON parse failure)
    valid = parsed.filter(
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("event_type").isin(*VALID_EVENT_TYPES) &
        (col("amount") >= 0)
    )

    return valid.withColumn("event_ts", to_timestamp(col("timestamp"))) \
                .withColumn("ingested_at", current_timestamp()) \
                .drop("timestamp")


def write_to_delta(stream_df, delta_path: str, checkpoint_path: str):
    return (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(processingTime="10 seconds")
        .start(delta_path)
    )


def run():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic             = os.getenv("KAFKA_TOPIC",             "user-events")
    delta_path        = os.getenv("DELTA_OUTPUT_PATH",       "/tmp/delta/user-events")
    checkpoint_path   = os.getenv("CHECKPOINT_PATH",         "/tmp/checkpoints/user-events")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw   = read_kafka_stream(spark, bootstrap_servers, topic)
    clean = parse_and_validate(raw)
    query = write_to_delta(clean, delta_path, checkpoint_path)

    query.awaitTermination()


if __name__ == "__main__":
    run()
