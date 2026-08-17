from pyspark.sql.functions import concat_ws, col

from src.utils.spark_session import create_spark_session


spark = create_spark_session("SkyPulse-Historical-Metrics")


# ==================================================
# PATHS
# ==================================================

BRONZE_PATH = "data/bronze/historical_flight_prices"
SILVER_PATH = "data/silver/historical_flight_prices"

ROUTE_GOLD_PATH = "data/gold/route_fare_analytics"
FLIGHT_GOLD_PATH = "data/gold/flight_fare_analytics"
ML_GOLD_PATH = "data/gold/ml_training_data"


# ==================================================
# READ DATA
# ==================================================

bronze_df = spark.read.format("delta").load(BRONZE_PATH)

silver_df = spark.read.format("delta").load(SILVER_PATH)

route_gold_df = spark.read.format("delta").load(ROUTE_GOLD_PATH)

flight_gold_df = spark.read.format("delta").load(FLIGHT_GOLD_PATH)

ml_gold_df = spark.read.format("delta").load(ML_GOLD_PATH)


# ==================================================
# CREATE DERIVED ROUTE
# ==================================================

silver_with_route = silver_df.withColumn(
    "route",
    concat_ws(
        "-",
        col("source_city"),
        col("destination_city")
    )
)


# ==================================================
# FINAL METRICS
# ==================================================

print("=" * 55)
print("SKYPULSE FINAL HISTORICAL PIPELINE METRICS")
print("=" * 55)

print(f"Bronze records              : {bronze_df.count()}")
print(f"Silver records              : {silver_df.count()}")

print(
    f"Unique airlines             : "
    f"{silver_df.select('airline').distinct().count()}"
)

print(
    f"Unique routes               : "
    f"{silver_with_route.select('route').distinct().count()}"
)

print(
    f"Route analytics records     : "
    f"{route_gold_df.count()}"
)

print(
    f"Flight analytics records    : "
    f"{flight_gold_df.count()}"
)

print(
    f"ML training records         : "
    f"{ml_gold_df.count()}"
)

print("=" * 55)


# ==================================================
# AIRLINES
# ==================================================

print("\nAIRLINES:")

silver_df.select(
    "airline"
).distinct().orderBy(
    "airline"
).show(50, truncate=False)


# ==================================================
# ROUTES
# ==================================================

print("\nUNIQUE ROUTES:")

silver_with_route.select(
    "route"
).distinct().orderBy(
    "route"
).show(50, truncate=False)


# ==================================================
# SILVER SCHEMA
# ==================================================

print("\nSILVER SCHEMA:")

silver_df.printSchema()


# ==================================================
# STOP SPARK
# ==================================================

spark.stop()
