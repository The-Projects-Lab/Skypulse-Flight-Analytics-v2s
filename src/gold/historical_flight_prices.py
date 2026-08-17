"""
SkyPulse Aviation Analytics Platform
------------------------------------

Gold Layer for historical flight data.

Reads the Silver Delta table and creates three Gold Delta tables:

1. flight_fare_analytics
   Business-level airline/fare analytics.

2. route_fare_analytics
   Route-level fare analytics.

3. ml_training_data
   ML-ready historical dataset for flight-price prediction.

All Gold datasets are stored as Delta tables.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.spark_session import create_spark_session


# ==========================================================
# Paths
# ==========================================================

SILVER_TABLE_PATH = "data/silver/historical_flight_prices"

GOLD_DIR = "data/gold"

FLIGHT_FARE_ANALYTICS_PATH = (
    f"{GOLD_DIR}/flight_fare_analytics"
)

ROUTE_FARE_ANALYTICS_PATH = (
    f"{GOLD_DIR}/route_fare_analytics"
)

ML_TRAINING_DATA_PATH = (
    f"{GOLD_DIR}/ml_training_data"
)


# ==========================================================
# Read Silver Delta
# ==========================================================

def read_silver_data(spark) -> DataFrame:
    """
    Read the historical Silver Delta table.
    """

    print("\nReading Silver Delta table...")

    df = (
        spark.read
        .format("delta")
        .load(SILVER_TABLE_PATH)
    )

    print(f"Silver rows    : {df.count()}")
    print(f"Silver columns : {len(df.columns)}")

    return df


# ==========================================================
# Gold 1 - Flight Fare Analytics
# ==========================================================

def create_flight_fare_analytics(
    silver_df: DataFrame
) -> DataFrame:
    """
    Create airline/cabin-level fare analytics.

    Grain:
        airline + source_city + destination_city + cabin_class
    """

    print("\nCreating flight fare analytics...")

    gold_df = (
        silver_df
        .groupBy(
            "airline",
            "source_city",
            "destination_city",
            "cabin_class",
        )
        .agg(
            F.count("*").alias("flight_count"),

            F.round(
                F.avg("price"),
                2
            ).alias("avg_price"),

            F.min("price").alias("min_price"),

            F.max("price").alias("max_price"),

            F.round(
                F.avg("duration_minutes"),
                2
            ).alias("avg_duration_minutes"),

            F.round(
                F.avg("days_left"),
                2
            ).alias("avg_days_left"),

            F.round(
                F.avg("stops"),
                2
            ).alias("avg_stops"),
        )
    )

    return gold_df


# ==========================================================
# Gold 2 - Route Fare Analytics
# ==========================================================

def create_route_fare_analytics(
    silver_df: DataFrame
) -> DataFrame:
    """
    Create route-level fare analytics.

    Grain:
        source_city + destination_city + cabin_class
    """

    print("\nCreating route fare analytics...")

    gold_df = (
        silver_df
        .groupBy(
            "source_city",
            "destination_city",
            "cabin_class",
        )
        .agg(
            F.count("*").alias("flight_count"),

            F.round(
                F.avg("price"),
                2
            ).alias("avg_price"),

            F.min("price").alias("min_price"),

            F.max("price").alias("max_price"),

            F.round(
                F.avg("duration_minutes"),
                2
            ).alias("avg_duration_minutes"),

            F.round(
                F.avg("days_left"),
                2
            ).alias("avg_days_left"),

            F.round(
                F.avg("stops"),
                2
            ).alias("avg_stops"),
        )
    )

    return gold_df


# ==========================================================
# Gold 3 - ML Training Data
# ==========================================================

def create_ml_training_data(
    silver_df: DataFrame
) -> DataFrame:
    """
    Create the ML-ready historical dataset.

    Only features available in the historical dataset
    and useful for price prediction are retained.

    Price remains the target variable.
    """

    print("\nCreating ML training dataset...")

    ml_df = (
        silver_df
        .select(
            "airline",
            "flight_code",
            "source_city",
            "destination_city",
            "departure_time",
            "arrival_time",
            "duration_minutes",
            "stops",
            "days_left",
            "cabin_class",
            "price",
        )
        .filter(
            F.col("price").isNotNull()
        )
        .filter(
            F.col("price") > 0
        )
    )

    return ml_df


# ==========================================================
# Write Delta Table
# ==========================================================

def write_delta(
    df: DataFrame,
    path: str,
    table_name: str,
) -> None:
    """
    Write a DataFrame as a Delta table.
    """

    print(f"\nWriting {table_name}...")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .save(path)
    )

    print(
        f"{table_name} Delta write: SUCCESS"
    )


# ==========================================================
# Validate Gold Table
# ==========================================================

def validate_gold_table(
    df: DataFrame,
    table_name: str,
) -> None:
    """
    Display basic validation information.
    """

    print("\n" + "-" * 90)
    print(f"VALIDATION: {table_name}")
    print("-" * 90)

    print(f"Rows    : {df.count()}")
    print(f"Columns : {len(df.columns)}")

    print("\nSchema:")

    df.printSchema()

    print("\nSample records:")

    df.show(
        5,
        truncate=False
    )


# ==========================================================
# Main
# ==========================================================

def main():

    spark = create_spark_session(
        "SkyPulse Historical Gold Layer"
    )

    print("\n")
    print("=" * 90)
    print(" SKY PULSE - HISTORICAL GOLD LAYER ")
    print("=" * 90)

    # ------------------------------------------------------
    # Read Silver
    # ------------------------------------------------------

    silver_df = read_silver_data(spark)

    # ------------------------------------------------------
    # Create Gold datasets
    # ------------------------------------------------------

    flight_fare_df = create_flight_fare_analytics(
        silver_df
    )

    route_fare_df = create_route_fare_analytics(
        silver_df
    )

    ml_training_df = create_ml_training_data(
        silver_df
    )

    # ------------------------------------------------------
    # Write Gold Delta tables
    # ------------------------------------------------------

    write_delta(
        flight_fare_df,
        FLIGHT_FARE_ANALYTICS_PATH,
        "flight_fare_analytics",
    )

    write_delta(
        route_fare_df,
        ROUTE_FARE_ANALYTICS_PATH,
        "route_fare_analytics",
    )

    write_delta(
        ml_training_df,
        ML_TRAINING_DATA_PATH,
        "ml_training_data",
    )

    # ------------------------------------------------------
    # Validation
    # ------------------------------------------------------

    validate_gold_table(
        flight_fare_df,
        "flight_fare_analytics",
    )

    validate_gold_table(
        route_fare_df,
        "route_fare_analytics",
    )

    validate_gold_table(
        ml_training_df,
        "ml_training_data",
    )

    # ------------------------------------------------------
    # Final Summary
    # ------------------------------------------------------

    print("\n")
    print("=" * 90)
    print(" GOLD LAYER COMPLETED SUCCESSFULLY ")
    print("=" * 90)

    print(
        f"""
Gold Delta tables created:

1. {FLIGHT_FARE_ANALYTICS_PATH}

2. {ROUTE_FARE_ANALYTICS_PATH}

3. {ML_TRAINING_DATA_PATH}
"""
    )

    spark.stop()


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    main()
