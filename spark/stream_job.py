#!/usr/bin/env python
import os
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, expr,
    sum as _sum, avg, max as _max, count, from_unixtime,
    when, lit, current_timestamp, array, explode
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# ---------- Config ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "energy.readings")
REDIS_HOST      = os.getenv("REDIS_HOST", "redis")
REDIS_PORT      = int(os.getenv("REDIS_PORT", "6379"))

WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")  # tumbling window
CHECKPOINT_AGG  = os.getenv("CHECKPOINT_AGG", "/tmp/energy_checkpoints/agg")
CHECKPOINT_ALERT= os.getenv("CHECKPOINT_ALERT", "/tmp/energy_checkpoints/alerts")

# Thresholds
TOTAL_WATTS_THRESHOLD = float(os.getenv("TOTAL_WATTS_THRESHOLD", "65000"))
MAX_WATTS_THRESHOLD   = float(os.getenv("MAX_WATTS_THRESHOLD",   "2180"))
AVG_WATTS_THRESHOLD   = float(os.getenv("AVG_WATTS_THRESHOLD",   "1350"))

# Ensure Kafka source works when launched via "python ..."
SPARK_KAFKA_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--packages {SPARK_KAFKA_PACKAGES} pyspark-shell")

# ---------- Schema ----------
SCHEMA = StructType([
    StructField("city", StringType()),
    StructField("household_id", StringType()),
    StructField("appliance", StringType()),
    StructField("watts", DoubleType()),
    StructField("timestamp", LongType()),  # epoch seconds
])

# ---------- Redis sinks ----------
def write_aggs_to_redis(batch_df, _):
    if batch_df.rdd.isEmpty():
        return
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    pipe = r.pipeline(transaction=False)

    for row in batch_df.collect():
        win_start = row["window"].start.strftime("%Y%m%d%H%M")
        key = f"city:{row['city']}:window:{win_start}"
        mapping = {
            "total_watts":  str(row["total_watts"]),
            "avg_watts":    str(row["avg_watts"]),
            "max_watts":    str(row["max_watts"]),
            "count":        str(row["count"]),
            "window_start": str(row["window"].start),
            "window_end":   str(row["window"].end),
        }
        pipe.hset(key, mapping=mapping)
        pipe.expire(key, 24 * 3600)
        pipe.set(f"city:{row['city']}:latest", key, ex=24 * 3600)  # pointer to latest per city

    pipe.execute()

def write_alerts_to_redis(batch_df, _):
    if batch_df.rdd.isEmpty():
        return
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    pipe = r.pipeline(transaction=False)

    for row in batch_df.collect():
        win_start = row["window"].start.strftime("%Y%m%d%H%M")
        # include anomaly type in the key so multiple alerts for same window don't overwrite
        alert_key = f"alert:{row['city']}:{win_start}:{row['anomaly_type']}"
        mapping = {
            "city":         row["city"],
            "anomaly_type": row["anomaly_type"],
            "total_watts":  str(row["total_watts"]),
            "avg_watts":    str(row["avg_watts"]),
            "max_watts":    str(row["max_watts"]),
            "window_start": str(row["window"].start),
            "window_end":   str(row["window"].end),
            "alert_time":   str(row["processing_time"]),
        }
        pipe.hset(alert_key, mapping=mapping)
        pipe.expire(alert_key, 24 * 3600)
        pipe.lpush(f"alerts:{row['city']}", alert_key)
        pipe.ltrim(f"alerts:{row['city']}", 0, 199)  # cap list

    pipe.execute()

def main():
    spark = (
        SparkSession.builder
        .appName("EnergyStreamingWithAnomalies")
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGES)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Quiet logs (including state store chatter)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        log4j = spark._jvm.org.apache.log4j
        for name in [
            "org.apache.spark",
            "org.apache.kafka",
            "org.apache.spark.sql.kafka010",
            "org.apache.spark.sql.execution.streaming.state",
            "org.apache.hadoop.util.NativeCodeLoader",
            "org.apache.spark.storage",
        ]:
            log4j.LogManager.getLogger(name).setLevel(log4j.Level.ERROR)
    except Exception:
        pass

    # 1) Kafka
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2) Parse
    parsed = raw.selectExpr("CAST(value AS STRING) AS json") \
                .select(from_json(col("json"), SCHEMA).alias("d")) \
                .select("d.*")

    # 3) Event time + watermark
    events = parsed.withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp")) \
                   .withWatermark("event_time", "2 minutes")

    # 4) Tumbling window aggregates
    windowed = (
        events.groupBy(window(col("event_time"), WINDOW_DURATION), col("city"))
        .agg(
            _sum("watts").alias("total_watts"),
            avg("watts").alias("avg_watts"),
            _max("watts").alias("max_watts"),
            count("household_id").alias("count"),
        )
    )

    # 5) Multi-type anomaly detection
    # Build an array of matched types, filter nulls, then explode => one row per matched type
    with_anoms_array = windowed.withColumn(
        "anoms",
        array(
            when(col("total_watts") > lit(TOTAL_WATTS_THRESHOLD), lit("HIGH_TOTAL")),
            when(col("max_watts")   > lit(MAX_WATTS_THRESHOLD),   lit("SPIKE")),
            when(col("avg_watts")   > lit(AVG_WATTS_THRESHOLD),   lit("HIGH_AVG")),
        )
    ).withColumn(
        "anoms",
        expr("filter(anoms, x -> x is not null)")
    )

    anomalies = (
        with_anoms_array
        .withColumn("anomaly_type", explode(col("anoms")))
        .withColumn("processing_time", current_timestamp())
        .drop("anoms")
    )

    # 6a) Console (aggregates for sanity)
    query_console = (
        windowed.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 50)
        .start()
    )

    # 6b) Aggregates → Redis
    query_agg = (
        windowed.writeStream
        .foreachBatch(write_aggs_to_redis)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_AGG)
        .start()
    )

    # 6c) Alerts → Redis (one per anomaly type)
    query_alerts = (
        anomalies.writeStream
        .foreachBatch(write_alerts_to_redis)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_ALERT)
        .start()
    )

    print("Streaming started: Kafka → window(1m) → Redis (aggs + multi-anomaly alerts). Logs set to ERROR.")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
