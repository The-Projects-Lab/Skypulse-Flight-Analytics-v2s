import json
import os

import pyspark
from pyspark.sql.functions import col, from_json, max as spark_max
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from src.utils.spark_session import create_spark_session


# ==================================================
# CONFIGURATION
# ==================================================

TOPIC = "flight_prices_live"
BRONZE_PATH = "data/bronze/live_flight_prices"
OFFSET_STATE_FILE = "data/live_kafka_offsets.json"

KAFKA_PACKAGE = (
    "org.apache.spark:"
    "spark-sql-kafka-0-10_2.13:"
    f"{pyspark.__version__}"
)


# ==================================================
# OFFSET STATE
# ==================================================

def load_offsets():
    """
    Load the next Kafka offsets to consume.

    On the first run, read all available messages from
    the topic. Later runs continue from the saved offset.
    """

    if not os.path.exists(OFFSET_STATE_FILE):
        return "earliest"

    with open(OFFSET_STATE_FILE, "r") as file:
        return json.dumps(json.load(file))


def save_offsets(kafka_df):
    """
    Save exclusive next offsets after a successful Bronze write.
    """

    offset_rows = (
        kafka_df
        .groupBy(
            "topic",
            "partition"
        )
        .agg(
            (spark_max(col("offset")) + 1).alias("next_offset")
        )
        .collect()
    )

    offsets = {}

    for row in offset_rows:

        topic_offsets = offsets.setdefault(
            row["topic"],
            {}
        )

        topic_offsets[str(row["partition"])] = int(
            row["next_offset"]
        )

    os.makedirs(
        os.path.dirname(OFFSET_STATE_FILE),
        exist_ok=True
    )

    with open(OFFSET_STATE_FILE, "w") as file:
        json.dump(
            offsets,
            file,
            indent=4
        )

    return offsets


# ==================================================
# SPARK SESSION
# ==================================================

spark = create_spark_session(
    app_name="SkyPulse-Live-Kafka-Consumer",
    extra_packages=[
        KAFKA_PACKAGE
    ]
)


# ==================================================
# KAFKA EVENT SCHEMA
# ==================================================

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("observed_at", StringType(), True),
    StructField("source_city", StringType(), True),
    StructField("destination_city", StringType(), True),
    StructField("travel_date", StringType(), True),
    StructField("airline", StringType(), True),
    StructField("airline_code", StringType(), True),
    StructField("departure_time", StringType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("stops", StringType(), True),
    StructField("duration", IntegerType(), True),
    StructField("class", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("days_left", IntegerType(), True)
])


# ==================================================
# READ AVAILABLE KAFKA MESSAGES
# ==================================================

starting_offsets = load_offsets()

print("\n========================================")
print("SkyPulse Kafka Batch Consumer Started")
print("========================================")
print("Kafka Topic :", TOPIC)
print("Bronze      :", BRONZE_PATH)
print("Offsets     :", starting_offsets)
print("========================================\n")

kafka_df = (
    spark.read
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        TOPIC
    )
    .option(
        "startingOffsets",
        starting_offsets
    )
    .option(
        "endingOffsets",
        "latest"
    )
    .load()
)

record_count = kafka_df.count()

if record_count == 0:

    print("No new Kafka records found. Bronze unchanged.")

    spark.stop()

else:

    # ==================================================
    # PARSE JSON PAYLOAD
    # ==================================================

    flight_df = (
        kafka_df
        .select(
            from_json(
                col("value").cast("string"),
                schema
            ).alias("data")
        )
        .select("data.*")
    )


    # ==================================================
    # WRITE TO BRONZE DELTA
    # ==================================================

    (
        flight_df.write
        .format("delta")
        .mode("append")
        .save(BRONZE_PATH)
    )

    saved_offsets = save_offsets(kafka_df)

    print("\n========================================")
    print("KAFKA -> BRONZE BATCH PROCESSED")
    print("========================================")
    print("Records Consumed :", record_count)
    print("Source           : Kafka")
    print("Target           : Bronze Delta")
    print("Saved Offsets    :", saved_offsets)
    print("========================================\n")

    flight_df.select(
        "event_id",
        "source_city",
        "destination_city",
        "airline",
        "travel_date",
        "price",
        "observed_at"
    ).show(
        5,
        truncate=False
    )

    spark.stop()
