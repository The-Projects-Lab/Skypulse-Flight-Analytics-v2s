from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_DIR = "/home/sunbeam/skypulse-aviation-analytics-platform"

PYTHON_BIN = "/usr/bin/python3"


# ============================================================
# DAG DEFAULT ARGUMENTS
# ============================================================

default_args = {
    "owner": "sunbeam",
    "depends_on_past": False,
    "retries": 1,
}


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="skypulse_aviation_pipeline",
    description=(
        "Orchestrates the SkyPulse aviation streaming "
        "pipeline from Kafka producer to Bronze, Silver and Gold"
    ),
    default_args=default_args,
    start_date=datetime(2026, 8, 16),
    schedule="0 0 * * *",
    catchup=False,
    tags=[
        "skypulse",
        "aviation",
        "kafka",
        "spark",
        "delta-lake"
    ],
) as dag:


    # ========================================================
    # 1. KAFKA PRODUCER
    # ========================================================

    kafka_producer = BashOperator(
        task_id="kafka_producer",
        bash_command=f"""
        cd {PROJECT_DIR}
        {PYTHON_BIN} -m src.live.kafka_producer
        """,
    )


    # ========================================================
    # 2. KAFKA → BRONZE
    #
    # Spark consumer reads available Kafka events
    # and writes them to Bronze Delta.
    # ========================================================

    kafka_to_bronze = BashOperator(
        task_id="kafka_to_bronze",
        bash_command=f"""
        cd {PROJECT_DIR}
        {PYTHON_BIN} -m src.live.spark_kafka_consumer
        """,
    )


    # ========================================================
    # 3. BRONZE → SILVER
    #
    # Cleans, validates, standardizes and deduplicates
    # streaming flight data.
    # ========================================================

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"""
        cd {PROJECT_DIR}
        {PYTHON_BIN} -m src.live.silver_transform
        """,
    )


    # ========================================================
    # 4. SILVER → GOLD
    #
    # Generates route, airline and window analytics.
    # ========================================================

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"""
        cd {PROJECT_DIR}
        {PYTHON_BIN} -m src.live.gold
        """,
    )


    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================

    (
        kafka_producer
        >> kafka_to_bronze
        >> bronze_to_silver
        >> silver_to_gold
    )
