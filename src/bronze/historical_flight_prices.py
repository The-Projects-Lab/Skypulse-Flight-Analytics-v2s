"""
SkyPulse Aviation Analytics
Phase 1 - Bronze Layer

Purpose
-------
Ingest the historical flight dataset from the Raw layer
into a Bronze Delta table.

Bronze principles
-----------------
- Preserve source data as much as possible
- No business transformations
- No ML feature engineering
- Add only technical metadata
- Store data using Delta Lake
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from configs.config import HISTORICAL_PRICES_FILE, BRONZE_DIR
from src.utils.spark_session import create_spark_session


# ==========================================================
# Constants
# ==========================================================

BRONZE_TABLE_PATH = BRONZE_DIR / "historical_flight_prices"


# ==========================================================
# Read Raw Historical Data
# ==========================================================

def read_historical_data(spark) -> DataFrame:
    """
    Read the historical flight CSV from the Raw layer.

    The source file contains an unnamed first column generated
    by the original dataset index.
    """

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(HISTORICAL_PRICES_FILE))
    )

    return df


# ==========================================================
# Bronze Preparation
# ==========================================================

def add_bronze_metadata(df: DataFrame) -> DataFrame:
    """
    Prepare raw data for Bronze storage.

    Only technical metadata is added here.
    No business transformations are performed.
    """

    bronze_df = (
        df
        .withColumn(
            "source_dataset",
            F.lit("historical_flight_prices")
        )
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )
    )

    return bronze_df


# ==========================================================
# Write Bronze Delta Table
# ==========================================================

def write_bronze_delta(df: DataFrame) -> None:
    """
    Write the historical dataset to a Bronze Delta table.
    """

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(BRONZE_TABLE_PATH))
    )


# ==========================================================
# Main
# ==========================================================

def main():

    spark = create_spark_session(
        "SkyPulse - Historical Bronze Ingestion"
    )

    print("\n")
    print("=" * 90)
    print(" SKY PULSE - HISTORICAL BRONZE INGESTION ")
    print("=" * 90)

    # ------------------------------------------------------
    # Read Raw Data
    # ------------------------------------------------------

    print("\nReading raw historical dataset...")

    raw_df = read_historical_data(spark)

    print(f"Raw rows    : {raw_df.count()}")
    print(f"Raw columns : {len(raw_df.columns)}")

    print("\nRaw Schema")
    raw_df.printSchema()

    # ------------------------------------------------------
    # Prepare Bronze
    # ------------------------------------------------------

    print("\nAdding bronze metadeta...")

    bronze_df = add_bronze_metadata(raw_df)

    # ------------------------------------------------------
    # Write Delta
    # ------------------------------------------------------

    print("\nWriting Bronze Delta table...")

    write_bronze_delta(bronze_df)

    # ------------------------------------------------------
    # Validation
    # ------------------------------------------------------

    print("\nValidating Bronze Delta table...")

    bronze_check = (
        spark.read
        .format("delta")
        .load(str(BRONZE_TABLE_PATH))
    )

    print(f"Bronze rows    : {bronze_check.count()}")
    print(f"Bronze columns : {len(bronze_check.columns)}")

    print("\nBronze Schema")
    bronze_check.printSchema()

    print("\nSample Bronze Records")

    bronze_check.show(
        5,
        truncate=False
    )

    # ------------------------------------------------------
    # Final Message
    # ------------------------------------------------------

    print("\n")
    print("=" * 90)
    print(" BRONZE INGESTION COMPLETED SUCCESSFULLY ")
    print("=" * 90)

    print(f"\nDelta table location:")
    print(BRONZE_TABLE_PATH)

    print("\nBronze Layer Characteristics:")
    print("  ✓ Historical source data preserved")
    print("  ✓ No business transformations")
    print("  ✓ Technical metadata added")
    print("  ✓ Stored as Delta")
    print("  ✓ Ready for Silver transformation")

    spark.stop()


# ==========================================================
# Entry Point
# ==========================================================

if __name__ == "__main__":
    main()
