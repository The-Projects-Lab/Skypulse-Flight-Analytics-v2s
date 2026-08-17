#!/bin/bash

# ============================================================
# SkyPulse Aviation Analytics - Complete Pipeline Runner
# ============================================================

PROJECT_DIR="/home/sunbeam/skypulse-aviation-analytics-platform"
KAFKA_DIR="/home/sunbeam/kafka_2.12-2.7.0"

echo ""
echo "============================================================"
echo "        SkyPulse Aviation Analytics Pipeline"
echo "============================================================"
echo ""


# ============================================================
# 1. ZOOKEEPER
# ============================================================

echo "[1/7] Starting Zookeeper..."

gnome-terminal -- bash -c "
$KAFKA_DIR/bin/zookeeper-server-start.sh \
$KAFKA_DIR/config/zookeeper.properties
exec bash
"

echo "Waiting 10 seconds for Zookeeper..."
sleep 10


# ============================================================
# 2. KAFKA
# ============================================================

echo "[2/7] Starting Kafka..."

gnome-terminal -- bash -c "
$KAFKA_DIR/bin/kafka-server-start.sh \
$KAFKA_DIR/config/server.properties
exec bash
"

echo "Waiting 10 seconds for Kafka..."
sleep 10


# ============================================================
# 3. SPARK KAFKA CONSUMER
# Kafka -> Bronze
# ============================================================

echo "[3/7] Starting Spark Kafka Consumer..."
echo "Kafka -> Bronze Layer"

gnome-terminal -- bash -c "
cd $PROJECT_DIR
python3 -m src.live.spark_kafka_consumer
exec bash
"

echo "Waiting 10 seconds for Bronze pipeline..."
sleep 10


# ============================================================
# 4. SILVER TRANSFORMATION
# Bronze -> Silver
# ============================================================

echo "[4/7] Starting Silver Transformation..."
echo "Bronze -> Silver Delta Layer"

gnome-terminal -- bash -c "
cd $PROJECT_DIR
python3 -m src.live.silver_transform
exec bash
"

echo "Waiting 10 seconds for Silver pipeline..."
sleep 10


# ============================================================
# 5. GOLD STREAMING
# Silver -> Gold
# Spark SQL Analytics
# ============================================================

echo "[5/7] Starting Gold Streaming Pipeline..."
echo "Silver -> Gold Delta Analytics"

gnome-terminal -- bash -c "
cd $PROJECT_DIR
python3 -m src.live.gold
exec bash
"

echo "Waiting 10 seconds for Gold pipeline..."
sleep 10


# ============================================================
# 6. KAFKA PRODUCER
# Sends live flight events
# ============================================================

echo "[6/7] Starting Kafka Producer..."

gnome-terminal -- bash -c "
cd $PROJECT_DIR
python3 -m src.live.kafka_producer
exec bash
"

echo "Waiting 10 seconds for Producer..."
sleep 10


# ============================================================
# 7. STREAMLIT UI
# ============================================================

echo "[7/7] Starting Streamlit UI..."

gnome-terminal -- bash -c "
cd $PROJECT_DIR
streamlit run app.py
exec bash
"


# ============================================================
# STATUS
# ============================================================

echo ""
echo "============================================================"
echo "ALL SKY PULSE SERVICES STARTED"
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
