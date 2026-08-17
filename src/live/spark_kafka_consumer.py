from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

from src.utils.spark_session import create_spark_session


# SPARK SESSION

spark = create_spark_session(
    app_name="SkyPulse-Live-Kafka-Consumer"
)


# KAFKA EVENT SCHEMA


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


# READ STREAM FROM KAFKA

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "flight_prices_live"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)



# CONVERT KAFKA VALUE TO JSON STRING

json_df = kafka_df.select(
    col("value")
    .cast("string")
    .alias("json_data")
)



# PARSE JSON


flight_df = (
    json_df
    .select(
        from_json(
            col("json_data"),
            schema
        ).alias("data")
    )
    .select("data.*")
)



# PROCESS EACH MICRO-BATCH


def process_batch(batch_df, batch_id):

    record_count = batch_df.count()

    if record_count == 0:
        return


    # WRITE TO BRONZE DELTA

    (
        batch_df.write
        .format("delta")
        .mode("append")
        .save(
            "data/bronze/live_flight_prices"
        )
    )


    # SHOW STREAMING PROGRESS IN TERMINAL

    print("\n========================================")
    print("KAFKA → SPARK STREAMING BATCH PROCESSED")
    print("========================================")
    print(f"Batch ID         : {batch_id}")
    print(f"Records Consumed : {record_count}")
    print("Source           : Kafka")
    print("Target           : Bronze Delta")
    print("========================================\n")

    batch_df.select(
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



# START STREAMING QUERY


query = (
    flight_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "data/checkpoints/live_flight_prices"
    )
    .trigger(
        availableNow=True
    )
    .start()
)


# STARTUP MESSAGE


print("\n========================================")
print("SkyPulse Live Streaming Started")
print("========================================")
print("Kafka Topic : flight_prices_live")
print("Bronze      : data/bronze/live_flight_prices")
print("Checkpoint  : data/checkpoints/live_flight_prices")
print("Trigger     : Every 10 seconds")
print("========================================")
print("Waiting for Kafka events...")
print("Press Ctrl+C to stop the consumer.\n")



# KEEP STREAM RUNNING

query.awaitTermination()
