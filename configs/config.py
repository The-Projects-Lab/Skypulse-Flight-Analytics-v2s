from pathlib import Path


# ============================================================
# Project Root
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ============================================================
# Data Directories
# ============================================================

DATA_DIR = PROJECT_ROOT / "data"

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"


# ============================================================
# Raw Dataset
# ============================================================

HISTORICAL_PRICES_FILE = (
    RAW_DIR / "historical_flight_prices.csv"
)


# ============================================================
# Model Directory
# ============================================================

MODELS_DIR = PROJECT_ROOT / "models"


# ============================================================
# Logs Directory
# ============================================================

LOGS_DIR = PROJECT_ROOT / "logs"
