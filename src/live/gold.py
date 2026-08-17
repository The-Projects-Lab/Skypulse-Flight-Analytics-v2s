from src.utils.spark_session import create_spark_session


# ==================================================
# SPARK SESSION
# ==================================================

spark = create_spark_session(
    "SkyPulse-Silver-to-Gold"
)


# ==================================================
# PATHS
# ==================================================

SILVER_PATH = "data/silver/live_flight_prices"

GOLD_ROUTE_PATH = "data/gold/route_price_analytics"

GOLD_AIRLINE_PATH = "data/gold/airline_price_analytics"

GOLD_WINDOW_PATH = "data/gold/price_window_analytics"


# ==================================================
# CHECKPOINT
# ==================================================

GOLD_CHECKPOINT = "data/checkpoints/gold_analytics"


# ==================================================
# READ SILVER AS STREAM
# ==================================================

silver_stream = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
)


# ==================================================
# PROCESS GOLD
# ==================================================

def process_gold(batch_df, batch_id):

    print("\n========================================")
    print(f"Processing Gold Batch: {batch_id}")
    print("========================================")


    # ----------------------------------------------
    # CHECK FOR EMPTY BATCH
    # ----------------------------------------------

    if batch_df.rdd.isEmpty():

        print("Empty batch. Skipping.")

        return


    # ==================================================
    # READ COMPLETE SILVER DATA
    #
    # Gold is recomputed from the complete Silver
    # Delta dataset so Gold always represents the
    # latest complete analytics state.
    # ==================================================

    silver_df = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
    )


    # ----------------------------------------------
    # Safety check
    # ----------------------------------------------

    if silver_df.rdd.isEmpty():

        print("Silver dataset is empty. Skipping.")

        return


    # ==================================================
    # CREATE TEMP VIEW
    #
    # IMPORTANT:
    # The Airflow Gold task runs in a separate Spark
    # application. Therefore this view must be created
    # inside this process before Spark SQL uses it.
    # ==================================================

    silver_df.createOrReplaceTempView(
        "silver_flights"
    )


    print(
        "Silver temporary view created successfully."
    )


    # ==================================================
    # 1. ROUTE PRICE ANALYTICS
    # ==================================================

    route_analytics = spark.sql("""
        SELECT
            route,
            travel_date,
            class,

            MIN(price) AS min_price,
            MAX(price) AS max_price,

            ROUND(
                AVG(price),
                2
            ) AS avg_price,

            COUNT(*) AS flight_count

        FROM silver_flights

        GROUP BY
            route,
            travel_date,
            class
    """)


    (
        route_analytics.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .save(GOLD_ROUTE_PATH)
    )


    print(
        "Route price analytics completed."
    )


    # ==================================================
    # 2. AIRLINE PRICE ANALYTICS
    # ==================================================

    airline_analytics = spark.sql("""
        SELECT
            route,
            travel_date,

            airline,
            airline_code,

            class,

            MIN(price) AS min_price,
            MAX(price) AS max_price,

            ROUND(
                AVG(price),
                2
            ) AS avg_price,

            COUNT(*) AS flight_count

        FROM silver_flights

        GROUP BY
            route,
            travel_date,
            airline,
            airline_code,
            class
    """)


    (
        airline_analytics.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .save(GOLD_AIRLINE_PATH)
    )


    print(
        "Airline price analytics completed."
    )


    # ==================================================
    # 3. TIME WINDOW PRICE ANALYTICS
    #
    # 30-minute window
    # Sliding every 10 minutes
    # ==================================================

    window_analytics = spark.sql("""
        SELECT
            window(
                event_timestamp,
                '30 minutes',
                '10 minutes'
            ).start AS window_start,

            window(
                event_timestamp,
                '30 minutes',
                '10 minutes'
            ).end AS window_end,

            route,
            class,

            MIN(price) AS min_price,
            MAX(price) AS max_price,

            ROUND(
                AVG(price),
                2
            ) AS avg_price,

            COUNT(*) AS price_observations

        FROM silver_flights

        GROUP BY
            window(
                event_timestamp,
                '30 minutes',
                '10 minutes'
            ),
            route,
            class
    """)


    (
        window_analytics.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .save(GOLD_WINDOW_PATH)
    )


    print(
        "Time window price analytics completed."
    )


    # ==================================================
    # BATCH COMPLETE
    # ==================================================

    print("\n----------------------------------------")
    print("Gold Batch Completed Successfully")
    print("----------------------------------------")
    print("Batch ID:", batch_id)
    print("----------------------------------------\n")


# ==================================================
# START GOLD STREAM
# ==================================================

gold_query = (
    silver_stream.writeStream
    .foreachBatch(process_gold)
    .outputMode("append")
    .option(
        "checkpointLocation",
        GOLD_CHECKPOINT
    )
    .trigger(
        availableNow=True
    )
    .start()
)


# ==================================================
# STATUS
# ==================================================

print("\n========================================")
print("SkyPulse Gold Streaming Started")
print("========================================")

print("Silver Source:")
print(SILVER_PATH)

print("----------------------------------------")

print("Gold Route Analytics:")
print(GOLD_ROUTE_PATH)

print("Gold Airline Analytics:")
print(GOLD_AIRLINE_PATH)

print("Gold Window Analytics:")
print(GOLD_WINDOW_PATH)

print("----------------------------------------")

print("Transformation : Spark SQL")
print("Storage        : Delta Lake")
print("Trigger        : AvailableNow")
print("Mode           : Finite Airflow-compatible job")

print("========================================\n")


# ==================================================
# WAIT UNTIL AVAILABLE DATA IS PROCESSED
# ==================================================

gold_query.awaitTermination()
