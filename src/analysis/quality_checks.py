"""
Data Quality Checks

Reusable helper functions for detecting common data quality
issues in categorical/string columns.

These checks are diagnostic only.

They identify problems but do not modify the DataFrame.
Actual cleaning and standardization happen in the Silver layer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ============================================================
# Categorical Quality Analysis
# ============================================================

def analyze_categorical_quality(
    df: DataFrame,
) -> dict:
    """
    Analyze all string columns for common data-quality issues.

    Checks performed
    ----------------
    1. Case inconsistencies
       Example:
           IndiGo
           indigo
           INDIGO

    2. Leading whitespace
       Example:
           " IndiGo"

    3. Trailing whitespace
       Example:
           "IndiGo "

    4. Empty strings
       Example:
           ""

    5. Whitespace-only strings
       Example:
           "   "

    Notes
    -----
    This function only reports issues.

    It does NOT modify the DataFrame.

    Returns
    -------
    dict
        Column-level data-quality report.
    """

    quality_report = {}

    # ========================================================
    # Identify String Columns
    # ========================================================

    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(
            field.dataType,
            StringType,
        )
    ]

    # ========================================================
    # Analyze Each String Column
    # ========================================================

    for column in string_columns:

        # ----------------------------------------------------
        # Get distinct non-null values
        # ----------------------------------------------------

        values = (
            df.select(column)
            .where(
                F.col(column).isNotNull()
            )
            .distinct()
            .collect()
        )

        original_values = [
            row[column]
            for row in values
            if row[column] is not None
        ]

        # ----------------------------------------------------
        # Case / Whitespace Normalization
        # ----------------------------------------------------
        #
        # Example:
        #
        # "IndiGo"
        # "indigo"
        # " INDIGO "
        #
        # all become:
        #
        # "indigo"
        #
        # This allows us to identify different representations
        # of the same categorical value.
        # ----------------------------------------------------

        normalized = {}

        for value in original_values:

            key = value.strip().lower()

            normalized.setdefault(
                key,
                [],
            ).append(value)

        # ----------------------------------------------------
        # Case inconsistencies
        # ----------------------------------------------------

        case_inconsistencies = {
            key: sorted(set(variants))
            for key, variants in normalized.items()
            if len(set(variants)) > 1
        }

        # ----------------------------------------------------
        # Leading whitespace
        # ----------------------------------------------------

        leading_spaces = [
            value
            for value in original_values
            if value != value.lstrip()
        ]

        # ----------------------------------------------------
        # Trailing whitespace
        # ----------------------------------------------------

        trailing_spaces = [
            value
            for value in original_values
            if value != value.rstrip()
        ]

        # ----------------------------------------------------
        # Empty strings
        # ----------------------------------------------------

        empty_strings = (
            df.filter(
                F.col(column) == ""
            )
            .count()
        )

        # ----------------------------------------------------
        # Whitespace-only strings
        # ----------------------------------------------------
        #
        # Example:
        #
        # " "
        # "   "
        # "\t"
        #
        # These are not technically empty strings but behave
        # like missing categorical values.
        # ----------------------------------------------------

        whitespace_only_strings = (
            df.filter(
                F.col(column).isNotNull()
                & (F.trim(F.col(column)) == "")
            )
            .count()
        )

        # ----------------------------------------------------
        # Store report
        # ----------------------------------------------------

        quality_report[column] = {

            "case_inconsistencies":
                case_inconsistencies,

            "leading_spaces":
                sorted(set(leading_spaces)),

            "trailing_spaces":
                sorted(set(trailing_spaces)),

            "empty_strings":
                empty_strings,

            "whitespace_only_strings":
                whitespace_only_strings,
        }

    return quality_report
