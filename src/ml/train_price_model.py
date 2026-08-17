from src.utils.spark_session import create_spark_session

from pyspark.sql import functions as F

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler
)

from pyspark.ml.regression import (
    LinearRegression,
    RandomForestRegressor,
    GBTRegressor
)

from pyspark.ml.evaluation import RegressionEvaluator


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = create_spark_session("SkyPulse Price Model Training")

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. READ GOLD ML TRAINING DATA
# ============================================================

INPUT_PATH = "data/gold/ml_training_data"

df = (
    spark.read
    .format("delta")
    .load(INPUT_PATH)
)

print("\n========== DATASET ==========")
print(f"Rows: {df.count()}")
print(f"Columns: {df.columns}")


# ============================================================
# 3. SELECT FEATURES
# ============================================================

feature_columns = [
    "airline",
    "source_city",
    "destination_city",
    "departure_time",
    "arrival_time",
    "duration_minutes",
    "stops",
    "days_left",
    "cabin_class"
]

target_column = "price"

df = df.select(
    *feature_columns,
    target_column
)


# ============================================================
# 4. DATA QUALITY
# ============================================================

print("\n========== NULL COUNTS ==========")

df.select([
    F.sum(
        F.col(c).isNull().cast("int")
    ).alias(c)
    for c in feature_columns + [target_column]
]).show()


# Remove rows with missing values

df = df.dropna(
    subset=feature_columns + [target_column]
)

print("\nRows after cleaning:", df.count())


# ============================================================
# 5. CATEGORICAL FEATURES
# ============================================================

categorical_columns = [
    "airline",
    "source_city",
    "destination_city",
    "departure_time",
    "arrival_time",
    "cabin_class"
]

numeric_columns = [
    "duration_minutes",
    "stops",
    "days_left"
]


# ============================================================
# 6. STRING INDEXING
# ============================================================

indexers = [
    StringIndexer(
        inputCol=column,
        outputCol=f"{column}_index",
        handleInvalid="keep"
    )
    for column in categorical_columns
]


# ============================================================
# 7. ONE-HOT ENCODING
# ============================================================

encoder = OneHotEncoder(
    inputCols=[
        f"{c}_index"
        for c in categorical_columns
    ],
    outputCols=[
        f"{c}_encoded"
        for c in categorical_columns
    ]
)


# ============================================================
# 8. CREATE FEATURE VECTOR
# ============================================================

assembler = VectorAssembler(
    inputCols=[
        f"{c}_encoded"
        for c in categorical_columns
    ] + numeric_columns,
    outputCol="features",
    handleInvalid="keep"
)


# ============================================================
# 9. TRAIN / TEST SPLIT
# ============================================================

train_df, test_df = df.randomSplit(
    [0.8, 0.2],
    seed=42
)

print("\n========== TRAIN / TEST ==========")
print("Training rows:", train_df.count())
print("Testing rows :", test_df.count())


# ============================================================
# 10. DEFINE MODELS
# ============================================================

models = {

    "LinearRegression": LinearRegression(
        featuresCol="features",
        labelCol=target_column,
        predictionCol="prediction"
    ),

    "RandomForest": RandomForestRegressor(
        featuresCol="features",
        labelCol=target_column,
        predictionCol="prediction",
        numTrees=100,
        maxDepth=10,
        seed=42
    ),

    "GBT": GBTRegressor(
        featuresCol="features",
        labelCol=target_column,
        predictionCol="prediction",
        maxIter=100,
        maxDepth=8,
        stepSize=0.1,
        seed=42
    )
}


# ============================================================
# 11. EVALUATORS
# ============================================================

rmse_evaluator = RegressionEvaluator(
    labelCol=target_column,
    predictionCol="prediction",
    metricName="rmse"
)

mae_evaluator = RegressionEvaluator(
    labelCol=target_column,
    predictionCol="prediction",
    metricName="mae"
)

r2_evaluator = RegressionEvaluator(
    labelCol=target_column,
    predictionCol="prediction",
    metricName="r2"
)


# ============================================================
# 12. TRAIN ALL MODELS
# ============================================================

results = []

trained_models = {}

for model_name, model in models.items():

    print("\n" + "=" * 60)
    print(f"TRAINING MODEL: {model_name}")
    print("=" * 60)

    pipeline = Pipeline(
        stages=indexers + [
            encoder,
            assembler,
            model
        ]
    )

    fitted_pipeline = pipeline.fit(train_df)

    predictions = fitted_pipeline.transform(test_df)

    rmse = rmse_evaluator.evaluate(predictions)
    mae = mae_evaluator.evaluate(predictions)
    r2 = r2_evaluator.evaluate(predictions)

    results.append(
        (
            model_name,
            rmse,
            mae,
            r2
        )
    )

    trained_models[model_name] = fitted_pipeline

    print(f"RMSE : {rmse:.2f}")
    print(f"MAE  : {mae:.2f}")
    print(f"R2   : {r2:.4f}")


# ============================================================
# 13. MODEL COMPARISON
# ============================================================

print("\n")
print("=" * 75)
print("MODEL COMPARISON")
print("=" * 75)

results_sorted = sorted(
    results,
    key=lambda x: x[1]
)

print(
    f"{'Model':<20}"
    f"{'RMSE':>15}"
    f"{'MAE':>15}"
    f"{'R2':>15}"
)

print("-" * 75)

for model_name, rmse, mae, r2 in results_sorted:

    print(
        f"{model_name:<20}"
        f"{rmse:>15.2f}"
        f"{mae:>15.2f}"
        f"{r2:>15.4f}"
    )


# ============================================================
# 14. SELECT BEST MODEL
# ============================================================

best_model_name = results_sorted[0][0]

best_rmse = results_sorted[0][1]
best_mae = results_sorted[0][2]
best_r2 = results_sorted[0][3]

print("\n")
print("=" * 60)
print("BEST MODEL")
print("=" * 60)

print("Model:", best_model_name)
print(f"RMSE : {best_rmse:.2f}")
print(f"MAE  : {best_mae:.2f}")
print(f"R2   : {best_r2:.4f}")


# ============================================================
# 15. SAVE BEST MODEL
# ============================================================

MODEL_PATH = "models/flight_price_model"

best_model = trained_models[best_model_name]

best_model.write().overwrite().save(
    MODEL_PATH
)

print("\nModel saved to:")
print(MODEL_PATH)


# ============================================================
# 16. SHOW SAMPLE PREDICTIONS
# ============================================================

print("\n")
print("=" * 60)
print("SAMPLE PREDICTIONS")
print("=" * 60)

predictions = best_model.transform(test_df)

predictions.select(
    "airline",
    "source_city",
    "destination_city",
    "duration_minutes",
    "stops",
    "days_left",
    "cabin_class",
    "price",
    F.round(
        "prediction",
        2
    ).alias("predicted_price")
).show(
    20,
    truncate=False
)


# ============================================================
# 17. STOP SPARK
# ============================================================

spark.stop()
