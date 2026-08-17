"""
Analyze historical flight dataset.

Phase 1:
    Historical Flight Data Analysis

This script:
    1. Loads the historical flight-price dataset
    2. Profiles the dataset
    3. Reports schema and column types
    4. Reports missing values
    5. Reports numeric statistics
    6. Reports categorical distributions
    7. Displays categorical values alphabetically
    8. Detects categorical data-quality issues
    9. Displays sample records
    10. Produces a data-quality summary

The output of this analysis is used to design
the Bronze and Silver layers.
"""

from pyspark.sql import DataFrame

from configs.config import HISTORICAL_PRICES_FILE

from src.utils.spark_session import create_spark_session

from src.analysis.profiler import profile_dataset


# ============================================================
# Load Historical Dataset
# ============================================================

def load_dataset(spark) -> DataFrame:
    """
    Load the historical flight-price dataset.
    """

    historical_flight_prices = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(HISTORICAL_PRICES_FILE))
    )

    return historical_flight_prices


# ============================================================
# Main
# ============================================================

def main():

    spark = create_spark_session()

    try:

        # ----------------------------------------------------
        # Load Dataset
        # ----------------------------------------------------

        df = load_dataset(spark)

        dataset_name = "historical_flight_prices"

        # ----------------------------------------------------
        # Generate Profile
        # ----------------------------------------------------

        report = profile_dataset(
            df,
            dataset_name,
        )

        # ----------------------------------------------------
        # Header
        # ----------------------------------------------------

        print("\n")
        print("=" * 100)
        print(" SKY PULSE - HISTORICAL DATA PROFILING REPORT ")
        print("=" * 100)

        print("\n")
        print("=" * 100)
        print(f"Dataset : {report['dataset_name']}")
        print("=" * 100)

        # ====================================================
        # GENERAL INFORMATION
        # ====================================================

        print("\nGENERAL INFORMATION")

        print(
            f"Rows              : "
            f"{report['row_count']}"
        )

        print(
            f"Columns           : "
            f"{report['column_count']}"
        )

        print(
            f"Duplicate Rows    : "
            f"{report['duplicate_rows']}"
        )

        # ====================================================
        # SCHEMA
        # ====================================================

        print("\nSCHEMA")

        for column, datatype in report["schema"].items():

            print(
                f"  {column:<30}"
                f"{datatype}"
            )

        # ====================================================
        # COLUMN CLASSIFICATION
        # ====================================================

        print("\nCOLUMN CLASSIFICATION")

        for column, category in report["column_types"].items():

            print(
                f"  {column:<30}"
                f"{category}"
            )

        # ====================================================
        # MISSING VALUES
        # ====================================================

        print("\nMISSING VALUES")

        for column, values in report["missing_values"].items():

            print(
                f"  {column:<30}"
                f"{values['count']:>8}"
                f" ({values['percentage']}%)"
            )

        # ====================================================
        # NUMERIC STATISTICS
        # ====================================================

        print("\nNUMERIC STATISTICS")

        if report["numeric_statistics"]:

            for column, stats in (
                report["numeric_statistics"].items()
            ):

                print(
                    f"  {column:<25}"
                    f"Min={stats['min']}   "
                    f"Max={stats['max']}   "
                    f"Avg={stats['avg']}"
                )

        else:

            print("  No numeric columns found.")

        # ====================================================
        # CATEGORICAL SUMMARY
        # ====================================================

        print("\nCATEGORICAL SUMMARY")

        if report["categorical_summary"]:

            for column, values in (
                report["categorical_summary"].items()
            ):

                print(f"\n{column}")

                # --------------------------------------------
                # Distinct Count
                # --------------------------------------------

                print(
                    f"  Distinct Values : "
                    f"{values['distinct_values']}"
                )

                # --------------------------------------------
                # Top Values
                # --------------------------------------------

                print("  Top Values")

                for item in values["top_values"]:

                    print(
                        f"    "
                        f"{str(item['value']):<30}"
                        f"{item['count']}"
                    )

                # --------------------------------------------
                # Alphabetical Values
                # --------------------------------------------

                print(
                    "\n  All Distinct Values "
                    "(Alphabetical)"
                )

                all_values = values["all_values"]

                if len(all_values) <= 50:

                    for value in all_values:

                        print(
                            f"    {value}"
                        )

                else:

                    for value in all_values[:50]:

                        print(
                            f"    {value}"
                        )

                    print("    ...")

                    print(
                        f"    "
                        f"({len(all_values)} total values. "
                        f"Showing first 50.)"
                    )

        else:

            print(
                "  No categorical columns found."
            )

        # ====================================================
        # DATA QUALITY CHECKS
        # ====================================================

        print("\nDATA QUALITY CHECKS")

        for column, issues in (
            report["quality_checks"].items()
        ):

            print(f"\n{column}")

            found_issue = False

            # --------------------------------------------
            # Case inconsistencies
            # --------------------------------------------

            if issues["case_inconsistencies"]:

                found_issue = True

                print(
                    "  Possible Case "
                    "Inconsistencies"
                )

                for (
                    normalized_value,
                    variants,
                ) in (
                    issues[
                        "case_inconsistencies"
                    ].items()
                ):

                    print(
                        f"    "
                        f"{normalized_value}: "
                        f"{variants}"
                    )

            # --------------------------------------------
            # Leading spaces
            # --------------------------------------------

            if issues["leading_spaces"]:

                found_issue = True

                print(
                    "  Leading Spaces Found:"
                )

                for value in (
                    issues["leading_spaces"]
                ):

                    print(
                        f"    '{value}'"
                    )

            # --------------------------------------------
            # Trailing spaces
            # --------------------------------------------

            if issues["trailing_spaces"]:

                found_issue = True

                print(
                    "  Trailing Spaces Found:"
                )

                for value in (
                    issues["trailing_spaces"]
                ):

                    print(
                        f"    '{value}'"
                    )

            # --------------------------------------------
            # Empty strings
            # --------------------------------------------

            if issues["empty_strings"] > 0:

                found_issue = True

                print(
                    f"  Empty Strings : "
                    f"{issues['empty_strings']}"
                )

            # --------------------------------------------
            # Whitespace-only strings
            # --------------------------------------------

            if (
                issues[
                    "whitespace_only_strings"
                ] > 0
            ):

                found_issue = True

                print(
                    f"  Whitespace-only "
                    f"Strings : "
                    f"{issues['whitespace_only_strings']}"
                )

            # --------------------------------------------
            # No issues
            # --------------------------------------------

            if not found_issue:

                print(
                    "  No Issues Found"
                )

        # ====================================================
        # SAMPLE RECORDS
        # ====================================================

        print("\nSAMPLE RECORDS")

        for row in report["sample_records"]:

            print(row)

        # ====================================================
        # DATA QUALITY SUMMARY
        # ====================================================

        print("\nDATA QUALITY SUMMARY")

        for (
            key,
            value,
        ) in report[
            "data_quality_summary"
        ].items():

            print(
                f"{key:<35}"
                f"{value}"
            )

        # ====================================================
        # END OF REPORT
        # ====================================================

        print("\n")
        print("=" * 100)
        print(" END OF HISTORICAL DATA PROFILING REPORT ")
        print("=" * 100)

    finally:

        spark.stop()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":

    main()
