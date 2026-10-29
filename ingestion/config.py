"""
Configuration module for the Batch + CDC Medallion Data Platform.
Provides centralized path, connection, schema, and API settings.
"""
from __future__ import annotations
import os
from pathlib import Path
from dataclasses import dataclass

# Project Root Directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Data Directories
DATA_DIR = PROJECT_ROOT / "data"
CSV_LANDING_DIR = DATA_DIR / "csv_landing"
CSV_QUARANTINE_DIR = DATA_DIR / "csv_quarantine"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
WAREHOUSE_DB_PATH = WAREHOUSE_DIR / "lakehouse.duckdb"

# Ensure runtime directories exist
for path in [CSV_LANDING_DIR, CSV_QUARANTINE_DIR, WAREHOUSE_DIR]:
    path.mkdir(parents=True, exist_ok=True)

@dataclass
class APIConfig:
    """REST API Ingestion Settings (Open-Meteo Weather API)"""
    base_url: str = "https://archive-api.open-meteo.com/v1/archive"
    forecast_url: str = "https://api.open-meteo.com/v1/forecast"
    max_retries: int = 4
    backoff_factor: float = 1.5
    rate_limit_per_sec: float = 5.0
    timeout_sec: int = 15
    # Default monitoring regions for e-commerce hubs
    locations: list[dict[str, str | float]] = None

    def __post_init__(self):
        if self.locations is None:
            self.locations = [
                {"city": "New York", "region_code": "US-NY", "latitude": 40.7128, "longitude": -74.0060},
                {"city": "Los Angeles", "region_code": "US-CA", "latitude": 34.0522, "longitude": -118.2437},
                {"city": "Chicago", "region_code": "US-IL", "latitude": 41.8781, "longitude": -87.6298},
                {"city": "Austin", "region_code": "US-TX", "latitude": 30.2672, "longitude": -97.7431},
                {"city": "Seattle", "region_code": "US-WA", "latitude": 47.6062, "longitude": -122.3321},
            ]

@dataclass
class PostgresOLTPConfig:
    """Postgres OLTP Source Database Configuration"""
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "ecommerce_oltp")
    user: str = os.getenv("POSTGRES_USER", "postgres")
    password: str = os.getenv("POSTGRES_PASSWORD", "postgres")

@dataclass
class KafkaCDCConfig:
    """Kafka / Debezium CDC Settings"""
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topics: list[str] = None
    group_id: str = "medallion-bronze-consumer-group"
    auto_offset_reset: str = "earliest"

    def __post_init__(self):
        if self.topics is None:
            self.topics = [
                "oltp.public.customers",
                "oltp.public.products",
                "oltp.public.orders",
                "oltp.public.order_items",
            ]
