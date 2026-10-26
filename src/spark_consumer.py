from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, stddev

from config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, OUTPUT_PATH

SCHEMA = """
    unit_number INT,
    time_in_cycles INT,
    op_setting_1 DOUBLE,
    op_setting_2 DOUBLE,
    op_setting_3 DOUBLE,
    sensor_1 DOUBLE,
    sensor_2 DOUBLE,
    sensor_3 DOUBLE,
    sensor_4 DOUBLE,
    sensor_5 DOUBLE,
    sensor_6 DOUBLE,
    sensor_7 DOUBLE,
    sensor_8 DOUBLE,
    sensor_9 DOUBLE,
    sensor_10 DOUBLE,
    sensor_11 DOUBLE,
    sensor_12 DOUBLE,
    sensor_13 DOUBLE,
    sensor_14 DOUBLE,
    sensor_15 DOUBLE,
    sensor_16 DOUBLE,
    sensor_17 DOUBLE,
    sensor_18 DOUBLE,
    sensor_19 DOUBLE,
    sensor_20 DOUBLE,
    sensor_21 DOUBLE,
    event_timestamp TIMESTAMP
"""

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CMAPSSMiniPipeline")
        .master("local[*]")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), SCHEMA).alias("data"))
        .select("data.*")
    )

    aggregated = (
        parsed
        .withWatermark("event_timestamp", "30 seconds")
        .groupBy(
            window(col("event_timestamp"), "10 seconds"),
            col("unit_number"),
        )
        .agg(
            avg("sensor_2").alias("sensor_2_avg"),
            stddev("sensor_2").alias("sensor_2_stddev"),
            avg("sensor_3").alias("sensor_3_avg"),
            avg("sensor_4").alias("sensor_4_avg"),
        )
    )

    query = (
        aggregated
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()