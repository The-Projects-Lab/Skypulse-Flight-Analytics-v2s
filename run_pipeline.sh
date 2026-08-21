#!/bin/bash
set -euo pipefail

# ============================================================
# SkyPulse Aviation Analytics - Complete Pipeline Runner
# ============================================================

PROJECT_DIR="/home/sunbeam/skypulse-aviation-analytics-platform"
KAFKA_DIR="/home/sunbeam/kafka_2.12-2.7.0"
TOPIC="flight_prices_live"

if [ ! -d "$PROJECT_DIR" ]; then
    echo "Project directory not found: $PROJECT_DIR"
    exit 1
fi

if [ ! -d "$KAFKA_DIR" ]; then
    echo "Kafka directory not found: $KAFKA_DIR"
    echo "Update KAFKA_DIR in run_pipeline.sh."
    exit 1
fi

if ! command -v gnome-terminal >/dev/null 2>&1; then
    echo "gnome-terminal is required to start Zookeeper and Kafka in separate windows."
    exit 1
fi

echo ""
echo "============================================================"
echo "        SkyPulse Aviation Analytics Pipeline"
echo "============================================================"
echo ""

cd "$PROJECT_DIR"

if [ -d ".venv" ]; then
    source ".venv/bin/activate"
fi

unset PYSPARK_SUBMIT_ARGS


# ============================================================
# 1. ZOOKEEPER
# ============================================================

echo "[1/7] Starting Zookeeper..."

gnome-terminal -- bash -c "
\"$KAFKA_DIR/bin/zookeeper-server-start.sh\" \
\"$KAFKA_DIR/config/zookeeper.properties\"
exec bash
"

echo "Waiting 10 seconds for Zookeeper..."
sleep 10


# ============================================================
# 2. KAFKA
# ============================================================

echo "[2/7] Starting Kafka..."

gnome-terminal -- bash -c "
\"$KAFKA_DIR/bin/kafka-server-start.sh\" \
\"$KAFKA_DIR/config/server.properties\"
exec bash
"

echo "Waiting 10 seconds for Kafka..."
sleep 10


# ============================================================
# 3. KAFKA TOPIC
# ============================================================

echo "[3/7] Ensuring Kafka topic exists..."

if "$KAFKA_DIR/bin/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --list | grep -qx "$TOPIC"; then

    echo "Kafka topic already exists: $TOPIC"

else

    "$KAFKA_DIR/bin/kafka-topics.sh" \
        --create \
        --topic "$TOPIC" \
        --bootstrap-server localhost:9092 \
        --partitions 1 \
        --replication-factor 1

fi


# ============================================================
# 4. KAFKA PRODUCER
# Sends live flight events
# ============================================================

echo "[4/7] Running Kafka Producer..."
python3 -m src.live.kafka_producer



# ============================================================
# 5. SPARK KAFKA CONSUMER
# Kafka -> Bronze
# ============================================================

echo "[5/7] Running Spark Kafka Consumer..."
echo "Kafka -> Bronze Layer"
python3 -m src.live.spark_kafka_consumer



# ============================================================
# 6. SILVER + GOLD TRANSFORMATIONS
# ============================================================

echo "[6/7] Running Silver Transformation..."
echo "Bronze -> Silver Delta Layer"
python3 -m src.live.silver_transform

echo "[6/7] Running Gold Analytics..."
echo "Silver -> Gold Delta Analytics"
python3 -m src.live.gold



# ============================================================
# 7. STREAMLIT UI
# ============================================================

echo "[7/7] Starting Streamlit UI..."

streamlit run app.py


# ============================================================
# STATUS
# ============================================================

echo ""
echo "============================================================"
echo "SKYPULSE PIPELINE COMPLETED"
echo "============================================================"
echo ""
echo "Complete Pipeline Flow:"
echo ""
echo "SerpAPI / Flight Source"
echo "          ↓"
echo "Kafka Producer"
echo "          ↓"
echo "Kafka"
echo "          ↓"
echo "Spark Kafka Consumer"
echo "          ↓"
echo "Bronze Delta Layer"
echo "          ↓"
echo "Silver Transformation"
echo "          ↓"
echo "Silver Delta Layer"
echo "          ↓"
echo "Gold Streaming (Spark SQL)"
echo "          ↓"
echo "Gold Delta Analytics"
echo "          ↓"
echo "Streamlit Dashboard"
echo ""
echo "============================================================"
