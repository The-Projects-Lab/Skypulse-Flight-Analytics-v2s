"""
Reusable dataset profiling utilities.

This module provides reusable profiling functions for Spark DataFrames.

The profiler is designed to be reusable across:

    Raw
      ↓
    Bronze
      ↓
    Silver
      ↓
    Gold

For the Phase 1 historical pipeline, it is primarily used to
understand and validate historical_flight_prices.csv before
designing the Bronze and Silver transformations.
"""

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    NumericType,
    StringType,
    DateType,
    TimestampType,
)

from src.analysis.quality_checks import analyze_categorical_quality


# ============================================================
# Main Dataset Profiler
# ============================================================

def profile_dataset(
    df: DataFrame,
    dataset_name: str,
) -> dict[str, Any]:
    """
    Generate a comprehensive profiling report for a Spark DataFrame.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame to profile.

    dataset_name : str
        Logical name of the dataset.

    Returns
    -------
    dict
        Complete profiling report.
    """

    # ========================================================
    # General Information
    # ========================================================

    row_count = df.count()
    column_count = len(df.columns)

    # Number of duplicate rows
    duplicate_rows = (
        row_count
        - df.dropDuplicates().count()
    )

    # ========================================================
    # Schema
    # ========================================================

    schema = {
        field.name: field.dataType.simpleString()
        for field in df.schema.fields
    }

    # ========================================================
    # Missing Values
    # ========================================================

    null_counts = (
        df.select(
            [
                F.count(
                    F.when(
                        F.col(column).isNull(),
                        column,
                    )
                ).alias(column)
                for column in df.columns
            ]
        )
        .first()
        .asDict()
    )

    missing_values = {}

    for column, count in null_counts.items():

        percentage = (
            round(
                (count / row_count) * 100,
                2,
            )
            if row_count > 0
            else 0
        )

        missing_values[column] = {
            "count": count,
            "percentage": percentage,
        }

    # ========================================================
    # Numeric Statistics
    # ========================================================

    numeric_statistics = {}

    for field in df.schema.fields:

        if isinstance(field.dataType, NumericType):

            stats = (
                df.select(
                    F.min(field.name).alias("min"),
                    F.max(field.name).alias("max"),
                    F.avg(field.name).alias("avg"),
                )
                .first()
            )

            average = stats["avg"]

            numeric_statistics[field.name] = {
                "min": stats["min"],
                "max": stats["max"],
                "avg": (
                    round(average, 2)
                    if average is not None
                    else None
                ),
            }

    # ========================================================
    # Categorical Columns
    # ========================================================

    categorical_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    # ========================================================
    # Categorical Summary
    # ========================================================

    categorical_summary = {}

    for column in categorical_columns:

        # ----------------------------------------------------
        # Distinct count
        # ----------------------------------------------------

        distinct_count = (
            df.select(column)
            .where(F.col(column).isNotNull())
            .distinct()
            .count()
        )

        # ----------------------------------------------------
        # Top 5 values
        # ----------------------------------------------------

        top_rows = (
            df.groupBy(column)
            .count()
            .orderBy(
                F.desc("count")
            )
            .limit(5)
            .collect()
        )

        top_values = [
            {
                "value": row[column],
                "count": row["count"],
            }
            for row in top_rows
        ]

        # ----------------------------------------------------
        # All distinct values
        #
        # Used for identifying hidden categorical issues such
        # as:
        #
        # IndiGo
        # indigo
        # INDIGO
        #
        # or leading/trailing spaces.
        # ----------------------------------------------------

        all_rows = (
            df.select(column)
            .where(F.col(column).isNotNull())
            .distinct()
            .orderBy(
                F.asc(column)
            )
            .collect()
        )

        all_values = [
            row[column]
            for row in all_rows
        ]

        categorical_summary[column] = {
            "distinct_values": distinct_count,
            "top_values": top_values,
            "all_values": all_values,
        }

    # ========================================================
    # Column Classification
    # ========================================================

    column_types = {}

    for field in df.schema.fields:

        column_name = field.name
        dtype = field.dataType

        # ----------------------------------------------------
        # Target variable
        # ----------------------------------------------------

        if column_name.lower() == "price":

            category = "Target Variable"

        # ----------------------------------------------------
        # Duration feature
        # ----------------------------------------------------

        elif "duration" in column_name.lower():

            category = "Duration Feature"

        # ----------------------------------------------------
        # Timestamp
        # ----------------------------------------------------

        elif isinstance(dtype, TimestampType):

            category = "Timestamp"

        # ----------------------------------------------------
        # Date
        # ----------------------------------------------------

        elif isinstance(dtype, DateType):

            category = "Date"

        # ----------------------------------------------------
        # Numeric
        # ----------------------------------------------------

        elif isinstance(dtype, NumericType):

            category = "Numeric"

        # ----------------------------------------------------
        # String / categorical
        # ----------------------------------------------------

        elif isinstance(dtype, StringType):

            category = "Categorical"

        # ----------------------------------------------------
        # Other
        # ----------------------------------------------------

        else:

            category = "Other"

        column_types[column_name] = category

    # ========================================================
    # Sample Records
    # ========================================================

    sample_records = [
        row.asDict()
        for row in df.limit(5).collect()
    ]

    # ========================================================
    # Data Quality Summary
    # ========================================================

    columns_with_missing_values = sum(
        1
        for values in missing_values.values()
        if values["count"] > 0
    )

    high_cardinality_columns = sum(
        1
        for values in categorical_summary.values()
        if values["distinct_values"] > 100
    )

    data_quality_summary = {

        "rows": row_count,

        "columns": column_count,

        "duplicate_rows": duplicate_rows,

        "columns_with_missing_values":
            columns_with_missing_values,

        "high_cardinality_columns":
            high_cardinality_columns,
    }

    # ========================================================
    # Categorical Quality Checks
    # ========================================================

    quality_checks = analyze_categorical_quality(df)

    # ========================================================
    # Final Report
    # ========================================================

    return {

        "dataset_name": dataset_name,

        "row_count": row_count,

        "column_count": column_count,

        "columns": df.columns,

        "schema": schema,

        "missing_values": missing_values,

        "duplicate_rows": duplicate_rows,

        "numeric_statistics": numeric_statistics,

        "categorical_summary": categorical_summary,

        "column_types": column_types,

        "sample_records": sample_records,

        "data_quality_summary": data_quality_summary,

        "quality_checks": quality_checks,
    }


# ============================================================
# Schema Comparison
# ============================================================

def compare_schemas(
    datasets: dict[str, DataFrame],
) -> dict[str, list[str]]:
    """
    Compare column names across multiple datasets.

    Parameters
    ----------
    datasets : dict[str, DataFrame]
        Dictionary containing dataset names and DataFrames.

    Returns
    -------
    dict
        Dataset name mapped to its column names.
    """

    return {
        dataset_name: dataframe.columns
        for dataset_name, dataframe in datasets.items()
    }
