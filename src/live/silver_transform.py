from src.utils.spark_session import create_spark_session

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lower,
    concat_ws,
    to_date,
    to_timestamp,
    when
)


# ==================================================
# SPARK SESSION
# ==================================================

spark = create_spark_session(
    "SkyPulse-Live-Silver-Transformation"
)


# ==================================================
# PATHS
# ==================================================

BRONZE_PATH = "data/bronze/live_flight_prices"

SILVER_PATH = "data/silver/live_flight_prices"

# ==================================================
# INDIAN AIRLINE CONFIGURATION
# ==================================================

# Primary filter: airline IATA codes
INDIAN_AIRLINE_CODES = [
    "AI",   # Air India
    "IX",   # Air India Express
    "6E",   # IndiGo
    "SG",   # SpiceJet
    "QP"    # Akasa Air
]


# Backup filter: normalized airline names
INDIAN_AIRLINE_NAMES = [
    "AIR INDIA",
    "AIR INDIA EXPRESS",
    "INDIGO",
    "SPICEJET",
    "AKASA AIR"
]


# ==================================================
# READ BRONZE DELTA
# ==================================================

bronze_df = (
    spark.read
    .format("delta")
    .load(BRONZE_PATH)
)


# ==================================================
# SILVER TRANSFORMATIONS
# ==================================================

silver_df = (
    bronze_df

    # ----------------------------------------------
    # Remove records missing critical fields
    # ----------------------------------------------
    .filter(
        col("event_id").isNotNull()
    )
    .filter(
        col("observed_at").isNotNull()
    )
    .filter(
        col("source_city").isNotNull()
    )
    .filter(
        col("destination_city").isNotNull()
    )
    .filter(
        col("travel_date").isNotNull()
    )
    .filter(
        col("airline").isNotNull()
    )
    .filter(
        col("price").isNotNull()
    )

    # ----------------------------------------------
    # Validate numerical values
    # ----------------------------------------------
    .filter(
        col("price") > 0
    )
    .filter(
        col("duration") > 0
    )
    .filter(
        col("days_left") >= 0
    )

    # ----------------------------------------------
    # Standardize text columns
    # ----------------------------------------------
    .withColumn(
        "source_city",
        trim(col("source_city"))
    )
    .withColumn(
        "destination_city",
        trim(col("destination_city"))
    )
    .withColumn(
        "airline",
        trim(col("airline"))
    )
    .withColumn(
        "airline_code",
        upper(trim(col("airline_code")))
    )
    .withColumn(
        "class",
        trim(col("class"))
    )
    .withColumn(
        "currency",
        upper(trim(col("currency")))
    )
    .withColumn(
        "stops",
        lower(trim(col("stops")))
    )

    # ----------------------------------------------
    # Indian domestic airline scope filter
    #
    # Keep a record if either:
    # 1. Its airline code belongs to an Indian carrier
    # OR
    # 2. Its normalized airline name belongs to an
    #    Indian carrier
    # ----------------------------------------------
    .filter(
        col("airline_code").isin(
            INDIAN_AIRLINE_CODES
        )
        |
        upper(col("airline")).isin(
            INDIAN_AIRLINE_NAMES
        )
    )

    # ----------------------------------------------
    # Convert date/time columns
    # ----------------------------------------------
    .withColumn(
        "travel_date",
        to_date(
            col("travel_date"),
            "yyyy-MM-dd"
        )
    )
    .withColumn(
        "event_timestamp",
        to_timestamp(
            col("observed_at"),
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
        )
    )

    # ----------------------------------------------
    # Remove invalid timestamps/dates
    # ----------------------------------------------
    .filter(
        col("travel_date").isNotNull()
    )
    .filter(
        col("event_timestamp").isNotNull()
    )

    # ----------------------------------------------
    # Create route
    # ----------------------------------------------
    .withColumn(
        "route",
        concat_ws(
            "-",
            upper(col("source_city")),
            upper(col("destination_city"))
        )
    )

    # ----------------------------------------------
    # Standardize stops
    # ----------------------------------------------
    .withColumn(
        "stops_count",
        when(col("stops") == "nonstop", 0)
        .when(col("stops") == "zero", 0)
        .when(col("stops") == "one", 1)
        .when(col("stops") == "two", 2)
        .otherwise(-1)
    )

    # ----------------------------------------------
    # Deduplicate logical flight observations
    #
    # event_id is NOT used because every scrape
    # creates a new event_id.
    #
    # Including price means a changed fare is kept
    # as a meaningful price update.
    # ----------------------------------------------
    .dropDuplicates([
        "source_city",
        "destination_city",
        "travel_date",
        "airline",
        "departure_time",
        "arrival_time",
        "class",
        "price"
    ])
)


# ==================================================
# WRITE SILVER DELTA
# ==================================================

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .save(SILVER_PATH)
)


# ==================================================
# PIPELINE STATUS
# ==================================================

print("========================================")
print("SkyPulse Silver Transformation Started")
print("========================================")
print("Source       :", BRONZE_PATH)
print("Target       :", SILVER_PATH)
print("Format       : Delta Lake")
print("Airline Scope: Indian Airlines Only")
print("Deduplication: Logical flight + price")
print("Mode         : Batch overwrite")
print("========================================")

print("========================================")
print("SkyPulse Silver Transformation Completed")
print("========================================")

spark.stop()
