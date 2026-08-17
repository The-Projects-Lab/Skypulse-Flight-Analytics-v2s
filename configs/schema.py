"""
Canonical schema for the SkyPulse Aviation Analytics Platform.

The canonical schema is based on the normalized flight structure
used by the live Fast Flights pipeline.

Historical flight data is transformed into this same structure
so that historical ML predictions and live flight prices can
be compared consistently.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    TimestampType,
)


# ==========================================================
# Canonical Flight Schema
# ==========================================================

COMMON_SCHEMA = StructType([

    # ------------------------------------------------------
    # Flight Identity
    # ------------------------------------------------------

    StructField(
        "airline",
        StringType(),
        True,
    ),

    StructField(
        "flight_code",
        StringType(),
        True,
    ),

    # ------------------------------------------------------
    # Route
    # ------------------------------------------------------

    StructField(
        "source_city",
        StringType(),
        True,
    ),

    StructField(
        "destination_city",
        StringType(),
        True,
    ),

    # ------------------------------------------------------
    # Departure
    # ------------------------------------------------------

    StructField(
        "departure_date",
        DateType(),
        True,
    ),

    StructField(
        "departure_time",
        StringType(),
        True,
    ),

    # ------------------------------------------------------
    # Arrival
    # ------------------------------------------------------

    StructField(
        "arrival_date",
        DateType(),
        True,
    ),

    StructField(
        "arrival_time",
        StringType(),
        True,
    ),

    # ------------------------------------------------------
    # Flight Characteristics
    # ------------------------------------------------------

    StructField(
        "duration_minutes",
        IntegerType(),
        True,
    ),

    StructField(
        "stops",
        IntegerType(),
        True,
    ),

    # ------------------------------------------------------
    # Booking / ML Features
    # ------------------------------------------------------

    StructField(
        "days_left",
        IntegerType(),
        True,
    ),

    StructField(
        "cabin_class",
        StringType(),
        True,
    ),

    # ------------------------------------------------------
    # Target / Price
    # ------------------------------------------------------

    StructField(
        "price",
        IntegerType(),
        True,
    ),

    # ------------------------------------------------------
    # Pipeline Metadata
    # ------------------------------------------------------

    StructField(
        "source",
        StringType(),
        True,
    ),

    StructField(
        "ingestion_timestamp",
        TimestampType(),
        True,
    ),
])
