# SkyPulse Aviation Analytics Platform

live : https://skypulse-flights.streamlit.app/

SkyPulse is a local aviation fare analytics project built for batch and live
flight-price analysis. It demonstrates a lakehouse-style data engineering
workflow using PySpark, Kafka, Spark Structured Streaming, Delta Lake, Spark SQL,
Spark ML, Airflow, and Streamlit.

The project contains:

- A historical batch pipeline for approximately 300k flight-price records.
- A live finite micro-batch streaming pipeline using Kafka and Spark Structured
  Streaming.
- Bronze, Silver, and Gold Delta layers.
- A Spark ML regression model for fare prediction.
- A Streamlit dashboard for analytics and live-vs-predicted fare comparison.
- An Airflow DAG for orchestrating the live pipeline.

Important: this repository is a local implementation. It does not include
Docker, Kubernetes, cloud deployment files, MLflow tracking, Kafka schema
registry, or production monitoring.

---

## Business Objective

The goal is to analyze historical and live Indian domestic flight fares and
compare live market fares with prices predicted by a historical Spark ML model.

The project answers questions such as:

- What are average, minimum, and maximum fares by route?
- How do fares vary by airline and cabin class?
- How do live observed fares move over event-time windows?
- Is a current live fare higher or lower than the ML-predicted fare?

---

## Architecture Overview

### Historical Batch Pipeline

```text
data/raw/historical_flight_prices.csv
        |
        v
src/bronze/historical_flight_prices.py
        |
        v
data/bronze/historical_flight_prices        Delta
        |
        v
src/silver/historical_flight_prices.py
        |
        v
data/silver/historical_flight_prices        Delta
        |
        v
src/gold/historical_flight_prices.py
        |
        v
data/gold/flight_fare_analytics             Delta
data/gold/route_fare_analytics              Delta
data/gold/ml_training_data                  Delta
        |
        v
src/ml/train_price_model.py
        |
        v
models/flight_price_model
```

### Live Pipeline

```text
fast-flights API/library
        |
        v
src/live/kafka_producer.py
        |
        v
Kafka topic: flight_prices_live
        |
        v
src/live/spark_kafka_consumer.py
        |
        v
data/bronze/live_flight_prices              Delta
        |
        v
src/live/silver_transform.py
        |
        v
data/silver/live_flight_prices              Delta
        |
        v
src/live/gold.py
        |
        v
data/gold/route_price_analytics             Delta
data/gold/airline_price_analytics           Delta
data/gold/price_window_analytics            Delta
```

### Dashboard Flow

```text
Streamlit user input: route, class, travel date
        |
        v
SerpAPI Google Flights request
        |
        v
Live flight options
        |
        v
Saved Spark ML PipelineModel
        |
        v
Live fare vs predicted fare comparison
```

---

## Technology Stack

| Area | Technology |
| --- | --- |
| Batch ETL | PySpark |
| Streaming | Spark Structured Streaming |
| Messaging | Kafka |
| Storage | Delta Lake on local filesystem |
| Analytics | Spark SQL, PySpark aggregations |
| ML | Spark ML |
| Orchestration | Apache Airflow |
| Dashboard | Streamlit |
| External data | fast-flights, SerpAPI |

---

## Repository Structure

```text
.
├── airflow/dags/skypulse_pipeline_dag.py
├── app.py
├── configs/
│   ├── config.py
│   ├── schema.py
│   └── live_routes.csv
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── checkpoints/
├── interview_prep/
│   ├── skypulse_interview_guide.md
│   ├── skypulse_interview_guide.pdf
│   └── render_pdf.py
├── models/flight_price_model/
├── src/
│   ├── analysis/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── live/
│   ├── ml/
│   └── utils/
├── requirements.txt
└── run_pipeline.sh
```

---

## Important Files

### `src/utils/spark_session.py`

Creates the common local Spark session and configures Delta Lake:

- `spark.sql.extensions`
- `spark.sql.catalog.spark_catalog`
- `configure_spark_with_delta_pip`

The session runs locally with:

```python
.master("local[*]")
```

### `configs/config.py`

Defines local project paths:

- `DATA_DIR`
- `RAW_DIR`
- `BRONZE_DIR`
- `SILVER_DIR`
- `GOLD_DIR`
- `HISTORICAL_PRICES_FILE`
- `MODELS_DIR`

### `configs/schema.py`

Defines the canonical historical Silver schema, `COMMON_SCHEMA`.

Fields include:

- `airline`
- `flight_code`
- `source_city`
- `destination_city`
- `departure_date`
- `arrival_date`
- `duration_minutes`
- `stops`
- `days_left`
- `cabin_class`
- `price`
- `source`
- `ingestion_timestamp`

Note: this schema is enforced in the historical Silver pipeline, but the live
Silver pipeline currently has a wider live-specific schema.

---

## Historical Batch Pipeline

### 1. Bronze Layer

File:

```text
src/bronze/historical_flight_prices.py
```

Input:

```text
data/raw/historical_flight_prices.csv
```

Output:

```text
data/bronze/historical_flight_prices
```

Main logic:

```python
raw_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(HISTORICAL_PRICES_FILE))
)

bronze_df = (
    raw_df
    .withColumn("source_dataset", F.lit("historical_flight_prices"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
)

bronze_df.write.format("delta").mode("overwrite").save(str(BRONZE_TABLE_PATH))
```

Purpose:

- Land raw data into Delta.
- Preserve source values.
- Add only technical metadata.

### 2. Silver Layer

File:

```text
src/silver/historical_flight_prices.py
```

Input:

```text
data/bronze/historical_flight_prices
```

Output:

```text
data/silver/historical_flight_prices
```

Main transformations:

- Drop `_c0` source CSV index column.
- Rename `flight` to `flight_code`.
- Rename `class` to `cabin_class`.
- Standardize airline names.
- Convert stops from text to integer.
- Convert duration from hours to minutes.
- Validate price.
- Trim string columns.
- Set `departure_date` and `arrival_date` to null because the source does not
  contain actual dates.
- Select canonical 15-column schema.

Important code pattern:

```python
df = (
    df
    .withColumnRenamed("flight", "flight_code")
    .withColumnRenamed("class", "cabin_class")
)

df = df.withColumn(
    "duration_minutes",
    F.round(F.col("duration") * F.lit(60)).cast("int")
)

df = df.withColumn(
    "price",
    F.when(F.col("price") > 0, F.col("price"))
     .otherwise(F.lit(None).cast("int"))
)

df = (
    df
    .withColumn("departure_date", F.lit(None).cast("date"))
    .withColumn("arrival_date", F.lit(None).cast("date"))
)
```

### 3. Gold Layer

File:

```text
src/gold/historical_flight_prices.py
```

Input:

```text
data/silver/historical_flight_prices
```

Outputs:

```text
data/gold/flight_fare_analytics
data/gold/route_fare_analytics
data/gold/ml_training_data
```

Gold tables:

- `flight_fare_analytics`: airline + source + destination + cabin class.
- `route_fare_analytics`: source + destination + cabin class.
- `ml_training_data`: record-level ML-ready dataset.

Example aggregation:

```python
route_gold_df = (
    silver_df
    .groupBy("source_city", "destination_city", "cabin_class")
    .agg(
        F.count("*").alias("flight_count"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.avg("days_left"), 2).alias("avg_days_left"),
        F.round(F.avg("stops"), 2).alias("avg_stops"),
    )
)
```

---

## Live Pipeline

### 1. Kafka Producer

File:

```text
src/live/kafka_producer.py
```

Inputs:

- `configs/live_routes.csv`
- `data/live_pipeline_state.json`
- `fast-flights` results

Output:

```text
Kafka topic: flight_prices_live
```

Main code:

```python
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

producer.send("flight_prices_live", value=event)
producer.flush()
```

The producer loops through route pairs and classes:

```python
for _, route in routes.iterrows():
    source_city = route["source_city"]
    destination_city = route["destination_city"]

    for class_name, seat in classes.items():
        query = create_query(
            flights=[
                FlightQuery(
                    date=travel_date,
                    from_airport=airport_codes[source_city],
                    to_airport=airport_codes[destination_city]
                )
            ],
            trip="one-way",
            seat=seat,
            passengers=Passengers(adults=1),
            language="en-US",
            currency="INR"
        )

        results = get_flights(query)
        events = parse_results(
            results,
            source_city,
            destination_city,
            travel_date,
            class_name
        )
```

### 2. Event Parser

File:

```text
src/live/fast_flights_parser.py
```

Purpose:

Converts `fast-flights` result objects into dictionaries suitable for Kafka.

Event fields:

- `event_id`
- `observed_at`
- `source_city`
- `destination_city`
- `travel_date`
- `airline`
- `airline_code`
- `departure_time`
- `arrival_time`
- `stops`
- `duration`
- `class`
- `price`
- `currency`
- `days_left`

### 3. Spark Kafka Consumer

File:

```text
src/live/spark_kafka_consumer.py
```

Input:

```text
Kafka topic: flight_prices_live
```

Output:

```text
data/bronze/live_flight_prices
```

Main code:

```python
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "flight_prices_live")
    .option("startingOffsets", "earliest")
    .load()
)

flight_df = (
    kafka_df
    .select(col("value").cast("string").alias("json_data"))
    .select(from_json(col("json_data"), schema).alias("data"))
    .select("data.*")
)
```

Micro-batch write:

```python
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    (
        batch_df.write
        .format("delta")
        .mode("append")
        .save("data/bronze/live_flight_prices")
    )

query = (
    flight_df.writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option("checkpointLocation", "data/checkpoints/live_flight_prices")
    .trigger(availableNow=True)
    .start()
)
```

### 4. Live Silver

File:

```text
src/live/silver_transform.py
```

Input:

```text
data/bronze/live_flight_prices
```

Output:

```text
data/silver/live_flight_prices
```

Main work:

- Null filtering.
- Numeric validation.
- String standardization.
- Indian airline filtering.
- Date parsing.
- Timestamp parsing.
- Route creation.
- Stop count creation.
- Watermarking.
- Deduplication.

Main code:

```python
bronze_df = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)

silver_df = (
    bronze_df
    .filter(col("event_id").isNotNull())
    .filter(col("observed_at").isNotNull())
    .filter(col("source_city").isNotNull())
    .filter(col("destination_city").isNotNull())
    .filter(col("travel_date").isNotNull())
    .filter(col("airline").isNotNull())
    .filter(col("price").isNotNull())
    .filter(col("price") > 0)
    .filter(col("duration") > 0)
    .filter(col("days_left") >= 0)
)
```

Watermark and deduplication:

```python
silver_df = (
    silver_df
    .withColumn(
        "event_timestamp",
        to_timestamp(col("observed_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    )
    .withWatermark("event_timestamp", "1 hour")
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
```

Write:

```python
query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start(SILVER_PATH)
)
```

### 5. Live Gold

File:

```text
src/live/gold.py
```

Input:

```text
data/silver/live_flight_prices
```

Outputs:

```text
data/gold/route_price_analytics
data/gold/airline_price_analytics
data/gold/price_window_analytics
```

Main code:

```python
silver_df = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

silver_df.createOrReplaceTempView("silver_flights")

route_analytics = spark.sql("""
    SELECT
        route,
        travel_date,
        class,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        ROUND(AVG(price), 2) AS avg_price,
        COUNT(*) AS flight_count
    FROM silver_flights
    GROUP BY route, travel_date, class
""")
```

Window analytics:

```sql
SELECT
    window(event_timestamp, '30 minutes', '10 minutes').start AS window_start,
    window(event_timestamp, '30 minutes', '10 minutes').end AS window_end,
    route,
    class,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    ROUND(AVG(price), 2) AS avg_price,
    COUNT(*) AS price_observations
FROM silver_flights
GROUP BY
    window(event_timestamp, '30 minutes', '10 minutes'),
    route,
    class
```

Note:
The live Gold job re-reads the full Silver Delta table and overwrites Gold
tables. This creates complete latest-state analytics, but it is not scalable for
large production workloads.

---

## Spark ML Pipeline

File:

```text
src/ml/train_price_model.py
```

Input:

```text
data/gold/ml_training_data
```

Output:

```text
models/flight_price_model
```

Features:

- `airline`
- `source_city`
- `destination_city`
- `departure_time`
- `arrival_time`
- `duration_minutes`
- `stops`
- `days_left`
- `cabin_class`

Target:

- `price`

Models trained:

- Linear Regression
- Random Forest Regressor
- GBT Regressor

Metrics:

- RMSE
- MAE
- R2

Main code pattern:

```python
indexers = [
    StringIndexer(
        inputCol=column,
        outputCol=f"{column}_index",
        handleInvalid="keep"
    )
    for column in categorical_columns
]

encoder = OneHotEncoder(
    inputCols=[f"{c}_index" for c in categorical_columns],
    outputCols=[f"{c}_encoded" for c in categorical_columns]
)

assembler = VectorAssembler(
    inputCols=[f"{c}_encoded" for c in categorical_columns] + numeric_columns,
    outputCol="features",
    handleInvalid="keep"
)
```

Model selection:

```python
results_sorted = sorted(results, key=lambda x: x[1])
best_model_name = results_sorted[0][0]
best_model = trained_models[best_model_name]
best_model.write().overwrite().save("models/flight_price_model")
```

Important:
The code compares three regressors and saves the lowest-RMSE model. The saved
artifact in this workspace appears to be GBT, but the training script does not
hardcode GBT as the final model.

---

## Streamlit Dashboard

File:

```text
app.py
```

Main pages:

- Dashboard
- Live vs Predicted
- Historical Analytics
- Gold Analytics
- Live Pipeline

The dashboard:

- Loads historical CSV using Pandas.
- Loads Delta tables using Spark and converts to Pandas.
- Loads Spark ML PipelineModel.
- Calls SerpAPI for interactive live Google Flights searches.
- Compares live fare with predicted fare.

SerpAPI call:

```python
params = {
    "engine": "google_flights",
    "departure_id": departure_id,
    "arrival_id": arrival_id,
    "outbound_date": str(travel_date),
    "currency": "INR",
    "hl": "en",
    "gl": "in",
    "type": 2,
    "travel_class": travel_class_map[travel_class],
    "api_key": SERPAPI_KEY
}

response = requests.get(
    "https://serpapi.com/search",
    params=params,
    timeout=60
)
```

Prediction call:

```python
model = PipelineModel.load(MODEL_PATH)
prediction_df = model.transform(input_df)
prediction = prediction_df.select("prediction").first()[0]
```

Limitation:
The dashboard converts Spark DataFrames to Pandas. This is acceptable for a
local demo but not for very large production tables.

---

## Airflow Orchestration

File:

```text
airflow/dags/skypulse_pipeline_dag.py
```

DAG:

```text
skypulse_aviation_pipeline
```

Schedule:

```text
0 0 * * *
```

Task chain:

```text
kafka_producer
    -> kafka_to_bronze
    -> bronze_to_silver
    -> silver_to_gold
```

Main code:

```python
kafka_producer = BashOperator(
    task_id="kafka_producer",
    bash_command=f"""
    cd {PROJECT_DIR}
    {PYTHON_BIN} -m src.live.kafka_producer
    """,
)

kafka_to_bronze = BashOperator(
    task_id="kafka_to_bronze",
    bash_command=f"""
    cd {PROJECT_DIR}
    {PYTHON_BIN} -m src.live.spark_kafka_consumer
    """,
)

kafka_producer >> kafka_to_bronze >> bronze_to_silver >> silver_to_gold
```

Important:
The Airflow DAG orchestrates the live pipeline only. It does not orchestrate the
historical batch pipeline or ML training.

---

## Setup

### 1. Create virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Airflow note:
For a clean Airflow installation, use the official Airflow constraints file for
your Python version. Installing `apache-airflow` directly can work locally, but
constraints are recommended.

### 2. Configure environment variables

Create a `.env` file for Streamlit's SerpAPI feature:

```text
SERPAPI_KEY=your_serpapi_key_here
```

### 3. Kafka requirement for Spark

The Python requirements install the Kafka producer client. Spark's Kafka source
is a JVM connector, not a pip dependency.

If Spark cannot find the Kafka source, start PySpark jobs with the Spark Kafka
package available. For PySpark 4.0.1, this is typically:

```bash
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 pyspark-shell"
```

Then run the streaming modules from the same shell.

### 4. Start Kafka locally

The local runner expects Kafka under:

```text
/home/sunbeam/kafka_2.12-2.7.0
```

Update `run_pipeline.sh` if your Kafka path is different.

---

## Running the Project

### Historical pipeline

```bash
python3 -m src.bronze.historical_flight_prices
python3 -m src.silver.historical_flight_prices
python3 -m src.gold.historical_flight_prices
python3 -m src.ml.train_price_model
```

### Live pipeline manually

Start Zookeeper and Kafka first. Then run:

```bash
python3 -m src.live.kafka_producer
python3 -m src.live.spark_kafka_consumer
python3 -m src.live.silver_transform
python3 -m src.live.gold
```

### Live pipeline helper script

```bash
bash run_pipeline.sh
```

This script opens multiple `gnome-terminal` windows. It is intended for a local
demo environment.

### Streamlit dashboard

```bash
streamlit run app.py
```

### Airflow DAG

The DAG is located at:

```text
airflow/dags/skypulse_pipeline_dag.py
```

It runs the live pipeline daily at midnight. Kafka and Zookeeper must already be
running.

---

## Data Quality

Profiling utilities:

- `src/analysis/profiler.py`
- `src/analysis/quality_checks.py`
- `src/analysis/analyze_datasets.py`

The profiler reports:

- Row count.
- Column count.
- Duplicate rows.
- Schema.
- Missing values.
- Numeric min/max/average.
- Categorical values.
- Case and whitespace issues.

Actual cleaning happens in Silver, not in the profiler.

Current limitation:
Invalid records are filtered out but not written to a quarantine table.

---

## Checkpointing, Watermarking, and Deduplication

Checkpointing is used in:

- `data/checkpoints/live_flight_prices`
- `data/checkpoints/silver_live_flight_prices`
- `data/checkpoints/gold_analytics`

Checkpointing solves recovery and progress tracking.

Watermarking is used in live Silver:

```python
.withWatermark("event_timestamp", "1 hour")
```

Watermarking helps Spark bound state for late events and deduplication.

Deduplication is also in live Silver:

```python
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
```

`event_id` is not used for deduplication because every scrape generates a new
UUID.

---

## Known Limitations

- Local Spark only: `.master("local[*]")`.
- Local Kafka only: `localhost:9092`.
- No Dockerfile or docker-compose.
- No Kubernetes or cloud deployment files.
- No MLflow tracking even though it existed in the older dependency list.
- No scikit-learn model training.
- No Kafka schema registry.
- No Kafka dead-letter queue.
- No idempotent producer configuration.
- No topic creation logic.
- No rejected-record quarantine.
- No automated tests.
- No production monitoring or alerting.
- Live and historical Silver schemas are not fully unified.
- Live Gold re-reads full Silver and overwrites Gold tables.
- Streamlit converts Spark DataFrames to Pandas.
- The Airflow DAG orchestrates only the live pipeline.

---

## Production Improvements

Recommended next steps:

1. Add Docker Compose for Kafka, Spark-compatible environment, and Streamlit.
2. Externalize all paths and service endpoints.
3. Use durable storage such as ADLS, S3, HDFS, or managed lakehouse storage.
4. Store raw Kafka payload and Kafka metadata in Bronze.
5. Add schema registry with Avro or Protobuf.
6. Add DLQ and quarantine tables.
7. Persist data quality metrics.
8. Replace full Gold recomputation with incremental aggregation or Delta MERGE.
9. Add partitioning and Delta optimization strategy.
10. Add MLflow experiment tracking and model registry.
11. Add validation and monitoring tasks to Airflow.
12. Add tests for Bronze, Silver, Gold, and ML feature generation.

---

```text
SkyPulse is a local lakehouse-style aviation analytics project. It demonstrates
batch ETL, Kafka ingestion, finite Spark Structured Streaming, Delta Medallion
layers, Spark SQL analytics, Spark ML prediction, Airflow orchestration for the
live pipeline, and Streamlit visualization. It is not production-grade yet, and
I can clearly explain what would need to change for production.
```

