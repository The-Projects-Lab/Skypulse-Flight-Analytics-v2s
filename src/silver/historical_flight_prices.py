"""
Historical Silver Layer
-----------------------

Transforms the historical Bronze Delta table into the
canonical 15-column Silver schema.

Important:
- Bronze preserves the source data.
- Silver performs business/data standardization.
- No fake dates or flight information are created.
- Silver is stored as a Delta table.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from configs.config import (
    BRONZE_DIR,
    SILVER_DIR,
)

from configs.schema import COMMON_SCHEMA

from src.utils.spark_session import create_spark_session


# ==========================================================
# Paths
# ==========================================================

BRONZE_TABLE_PATH = (
    BRONZE_DIR / "historical_flight_prices"
)

SILVER_TABLE_PATH = (
    SILVER_DIR / "historical_flight_prices"
)


# ==========================================================
# Read Bronze Delta
# ==========================================================

def read_bronze_data(spark) -> DataFrame:
    """
    Read the historical Bronze Delta table.
    """

    print("Reading Bronze Delta table...")

    df = (
        spark.read
        .format("delta")
        .load(str(BRONZE_TABLE_PATH))
    )

    return df


# ==========================================================
# Airline Standardization
# ==========================================================

def standardize_airline(df: DataFrame) -> DataFrame:
    """
    Standardize historical airline names so that they are
    compatible with the naming used by the live pipeline.

    Historical values:
        Vistara
        Air_India
        Indigo
        GO_FIRST
        AirAsia
        SpiceJet

    Canonical values:
        Air India
        IndiGo
        Go First
        Air India Express
        SpiceJet
    """

    return (
        df.withColumn(
            "airline",
            F.when(
                F.lower(F.trim(F.col("airline"))) == "vistara",
                F.lit("Air India"),
            )
            .when(
                F.lower(F.trim(F.col("airline"))) == "air_india",
                F.lit("Air India"),
            )
            .when(
                F.lower(F.trim(F.col("airline"))) == "indigo",
                F.lit("IndiGo"),
            )
            .when(
                F.lower(F.trim(F.col("airline"))) == "go_first",
                F.lit("Go First"),
            )
            .when(
                F.lower(F.trim(F.col("airline"))) == "airasia",
                F.lit("Air India Express"),
            )
            .when(
                F.lower(F.trim(F.col("airline"))) == "spicejet",
                F.lit("SpiceJet"),
            )
            .otherwise(F.trim(F.col("airline")))
        )
    )


# ==========================================================
# Stops Standardization
# ==========================================================

def standardize_stops(df: DataFrame) -> DataFrame:
    """
    Convert historical stop categories into integer values.

    zero        -> 0
    one         -> 1
    two_or_more -> 2

    2 represents two or more stops.
    """

    return (
        df.withColumn(
            "stops",
            F.when(
                F.lower(F.trim(F.col("stops"))) == "zero",
                F.lit(0),
            )
            .when(
                F.lower(F.trim(F.col("stops"))) == "one",
                F.lit(1),
            )
            .when(
                F.lower(F.trim(F.col("stops"))) == "two_or_more",
                F.lit(2),
            )
            .otherwise(F.lit(None).cast("int"))
        )
    )


# ==========================================================
# Duration Transformation
# ==========================================================

def transform_duration(df: DataFrame) -> DataFrame:
    """
    Convert historical duration from hours to minutes.

    Example:
        2.17 hours -> approximately 130 minutes
    """

    return (
        df.withColumn(
            "duration_minutes",
            F.round(
                F.col("duration") * F.lit(60)
            ).cast("int")
        )
    )


# ==========================================================
# Price Validation
# ==========================================================

def validate_price(df: DataFrame) -> DataFrame:
    """
    Validate the historical price field.

    Invalid prices are converted to NULL.

    A price must be greater than zero.
    """

    return (
        df.withColumn(
            "price",
            F.when(
                F.col("price") > 0,
                F.col("price"),
            )
            .otherwise(F.lit(None).cast("int"))
        )
    )


# ==========================================================
# Build Canonical Silver Data
# ==========================================================

def transform_to_silver(df: DataFrame) -> DataFrame:
    """
    Transform Bronze data into the canonical Silver schema.
    """

    print("Transforming Bronze data into Silver...")

    # ------------------------------------------------------
    # Remove source CSV index
    # ------------------------------------------------------

    if "_c0" in df.columns:
        df = df.drop("_c0")

    # ------------------------------------------------------
    # Rename columns
    # ------------------------------------------------------

    df = (
        df
        .withColumnRenamed(
            "flight",
            "flight_code",
        )
        .withColumnRenamed(
            "class",
            "cabin_class",
        )
    )

    # ------------------------------------------------------
    # Standardize airline names
    # ------------------------------------------------------

    df = standardize_airline(df)

    # ------------------------------------------------------
    # Standardize stops
    # ------------------------------------------------------

    df = standardize_stops(df)

    # ------------------------------------------------------
    # Duration: hours -> minutes
    # ------------------------------------------------------

    df = transform_duration(df)

    # ------------------------------------------------------
    # Validate price
    # ------------------------------------------------------

    df = validate_price(df)

    # ------------------------------------------------------
    # Standardize string columns
    # ------------------------------------------------------

    string_columns = [
        "flight_code",
        "source_city",
        "destination_city",
        "departure_time",
        "arrival_time",
        "cabin_class",
    ]

    for column in string_columns:

        df = df.withColumn(
            column,
            F.trim(F.col(column))
        )

    # ------------------------------------------------------
    # Historical dataset does NOT contain actual dates
    # ------------------------------------------------------
    #
    # Therefore we deliberately keep:
    #
    # departure_date = NULL
    # arrival_date   = NULL
    #
    # We do NOT manufacture dates from days_left.
    # ------------------------------------------------------

    df = (
        df
        .withColumn(
            "departure_date",
            F.lit(None).cast("date")
        )
        .withColumn(
            "arrival_date",
            F.lit(None).cast("date")
        )
    )

    # ------------------------------------------------------
    # Historical dataset does not contain aircraft
    # or carbon emission.
    #
    # These fields are not part of the 15-column schema,
    # so they are intentionally not carried into Silver.
    # ------------------------------------------------------

    # ------------------------------------------------------
    # Pipeline source
    # ------------------------------------------------------

    df = df.withColumn(
        "source",
        F.lit("historical")
    )

    # ------------------------------------------------------
    # Select canonical columns
    # ------------------------------------------------------

    silver_df = df.select(

        # Flight Identity
        F.col("airline").cast("string"),
        F.col("flight_code").cast("string"),

        # Route
        F.col("source_city").cast("string"),
        F.col("destination_city").cast("string"),

        # Departure
        F.col("departure_date").cast("date"),
        F.col("departure_time").cast("string"),

        # Arrival
        F.col("arrival_date").cast("date"),
        F.col("arrival_time").cast("string"),

        # Flight Characteristics
        F.col("duration_minutes").cast("int"),
        F.col("stops").cast("int"),

        # ML Features
        F.col("days_left").cast("int"),
        F.col("cabin_class").cast("string"),

        # Target
        F.col("price").cast("int"),

        # Metadata
        F.col("source").cast("string"),
        F.col("ingestion_timestamp").cast("timestamp"),
    )

    return silver_df


# ==========================================================
# Validate Silver Schema
# ==========================================================

def validate_silver_schema(df: DataFrame) -> None:
    """
    Validate that the Silver DataFrame follows COMMON_SCHEMA.
    """

    expected_columns = [
        field.name
        for field in COMMON_SCHEMA.fields
    ]

    actual_columns = df.columns

    if actual_columns != expected_columns:

        raise ValueError(
            "\nSilver schema validation failed.\n"
            f"Expected columns:\n{expected_columns}\n\n"
            f"Actual columns:\n{actual_columns}"
        )

    print("Silver schema validation: PASSED")


# ==========================================================
# Write Silver Delta
# ==========================================================

def write_silver_delta(df: DataFrame) -> None:
    """
    Write Silver data as a Delta table.
    """

    print("Writing Silver Delta table...")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(SILVER_TABLE_PATH))
    )

    print("Silver Delta write: SUCCESS")


# ==========================================================
# Silver Validation
# ==========================================================

def validate_silver_data(spark) -> None:
    """
    Read the Silver Delta table again and validate
    row count and schema.
    """

    print("\nValidating Silver Delta table...")

    silver_df = (
        spark.read
        .format("delta")
        .load(str(SILVER_TABLE_PATH))
    )

    row_count = silver_df.count()

    print(f"Silver rows    : {row_count}")
    print(f"Silver columns : {len(silver_df.columns)}")

    print("\nSilver Schema")
    silver_df.printSchema()

    print("\nSample Silver Records")
    silver_df.show(10, truncate=False)

    print("\nAirline Values")

    (
        silver_df
        .select("airline")
        .distinct()
        .orderBy("airline")
        .show(100, truncate=False)
    )

    print("\nStops Values")

    (
        silver_df
        .select("stops")
        .distinct()
        .orderBy("stops")
        .show()
    )


# ==========================================================
# Main
# ==========================================================

def main():

    spark = create_spark_session(
        "SkyPulse Historical Silver"
    )

    print("\n")
    print("=" * 90)
    print(" SKY PULSE - HISTORICAL SILVER TRANSFORMATION ")
    print("=" * 90)

    try:

        # --------------------------------------------------
        # Read Bronze
        # --------------------------------------------------

        bronze_df = read_bronze_data(spark)

        print(
            f"Bronze rows    : {bronze_df.count()}"
        )

        print(
            f"Bronze columns : {len(bronze_df.columns)}"
        )

        # --------------------------------------------------
        # Transform
        # --------------------------------------------------

        silver_df = transform_to_silver(
            bronze_df
        )

        # --------------------------------------------------
        # Validate schema
        # --------------------------------------------------

        validate_silver_schema(
            silver_df
        )

        # --------------------------------------------------
        # Display transformation information
        # --------------------------------------------------

        print("\nSilver columns")

        for column in silver_df.columns:
            print(f"  - {column}")

        # --------------------------------------------------
        # Write Delta
        # --------------------------------------------------

        write_silver_delta(
            silver_df
        )

        # --------------------------------------------------
        # Validate written Delta table
        # --------------------------------------------------

        validate_silver_data(
            spark
        )

        print("\n")
        print("=" * 90)
        print(" SILVER TRANSFORMATION COMPLETED SUCCESSFULLY ")
        print("=" * 90)

    finally:

        spark.stop()


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    main()
