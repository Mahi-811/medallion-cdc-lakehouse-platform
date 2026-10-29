"""
Kafka / Debezium CDC Consumer and Bronze Lakehouse Ingestion Engine.
Consumes CDC mutation streams (creates, updates, deletes) and lands them append-only into Bronze.
Enriches every record with ingestion lineage: _loaded_at, _source, _batch_id, _cdc_op, _cdc_ts_ms.
"""
from __future__ import annotations
import json
import uuid
import logging
from datetime import datetime
from typing import Any
from pathlib import Path
import duckdb
import pandas as pd

from ingestion.config import WAREHOUSE_DB_PATH, KafkaCDCConfig
from ingestion.oltp_generator import OLTPDataSimulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("CDCConsumer")

class CDCKafkaConsumer:
    """Consumes Debezium CDC messages and appends raw events to Bronze layer."""
    def __init__(self, db_path: Path | str = WAREHOUSE_DB_PATH, config: KafkaCDCConfig | None = None):
        self.db_path = str(db_path)
        self.config = config or KafkaCDCConfig()

    def process_cdc_events(self, events: list[dict[str, Any]]) -> dict[str, int]:
        """
        Parses a micro-batch of Debezium CDC events, enriches them with bronze metadata,
        and lands them append-only into target bronze tables.
        """
        batch_id = str(uuid.uuid4())
        loaded_at = datetime.utcnow().isoformat()

        # Categorize records by target table
        orders_batch: list[dict[str, Any]] = []
        customers_batch: list[dict[str, Any]] = []
        products_batch: list[dict[str, Any]] = []
        order_items_batch: list[dict[str, Any]] = []

        for evt in events:
            table = evt.get("source", {}).get("table")
            op = evt.get("op")
            ts_ms = evt.get("ts_ms")
            payload = evt.get("after") if op != "d" else evt.get("before")

            if not payload:
                continue

            record = {
                "_loaded_at": loaded_at,
                "_source": "debezium_cdc_postgres",
                "_batch_id": batch_id,
                "_cdc_op": op,
                "_cdc_ts_ms": ts_ms,
                **payload
            }

            if table == "orders":
                orders_batch.append(record)
            elif table == "customers":
                customers_batch.append(record)
            elif table == "products":
                products_batch.append(record)
            elif table == "order_items":
                order_items_batch.append(record)

        counts = {}
        con = duckdb.connect(self.db_path)
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

            # 1. Orders CDC Bronze Table
            if orders_batch:
                con.execute("""
                    CREATE TABLE IF NOT EXISTS bronze.bronze_orders_cdc (
                        _loaded_at VARCHAR,
                        _source VARCHAR,
                        _batch_id VARCHAR,
                        _cdc_op VARCHAR,
                        _cdc_ts_ms BIGINT,
                        order_id BIGINT,
                        customer_id BIGINT,
                        order_date VARCHAR,
                        order_timestamp VARCHAR,
                        order_status VARCHAR,
                        gross_amount DOUBLE,
                        discount_amount DOUBLE,
                        tax_amount DOUBLE,
                        net_amount DOUBLE,
                        created_at VARCHAR,
                        updated_at VARCHAR
                    );
                """)
                df_orders = pd.DataFrame(orders_batch)
                con.register("df_orders", df_orders)
                con.execute("INSERT INTO bronze.bronze_orders_cdc SELECT * FROM df_orders;")
                counts["orders"] = len(orders_batch)

            # 2. Customers CDC Bronze Table
            if customers_batch:
                con.execute("""
                    CREATE TABLE IF NOT EXISTS bronze.bronze_customers_cdc (
                        _loaded_at VARCHAR,
                        _source VARCHAR,
                        _batch_id VARCHAR,
                        _cdc_op VARCHAR,
                        _cdc_ts_ms BIGINT,
                        customer_id BIGINT,
                        first_name VARCHAR,
                        last_name VARCHAR,
                        email VARCHAR,
                        phone VARCHAR,
                        address VARCHAR,
                        city VARCHAR,
                        loyalty_tier VARCHAR,
                        created_at VARCHAR,
                        updated_at VARCHAR
                    );
                """)
                df_cust = pd.DataFrame(customers_batch)
                con.register("df_cust", df_cust)
                con.execute("INSERT INTO bronze.bronze_customers_cdc SELECT * FROM df_cust;")
                counts["customers"] = len(customers_batch)

            # 3. Products CDC Bronze Table
            if products_batch:
                con.execute("""
                    CREATE TABLE IF NOT EXISTS bronze.bronze_products_cdc (
                        _loaded_at VARCHAR,
                        _source VARCHAR,
                        _batch_id VARCHAR,
                        _cdc_op VARCHAR,
                        _cdc_ts_ms BIGINT,
                        product_id BIGINT,
                        sku VARCHAR,
                        product_name VARCHAR,
                        category VARCHAR,
                        unit_cost DOUBLE,
                        list_price DOUBLE,
                        stock_quantity BIGINT,
                        supplier_id BIGINT,
                        is_active BOOLEAN,
                        created_at VARCHAR,
                        updated_at VARCHAR
                    );
                """)
                df_prod = pd.DataFrame(products_batch)
                con.register("df_prod", df_prod)
                con.execute("INSERT INTO bronze.bronze_products_cdc SELECT * FROM df_prod;")
                counts["products"] = len(products_batch)

            # 4. Order Items CDC Bronze Table
            if order_items_batch:
                con.execute("""
                    CREATE TABLE IF NOT EXISTS bronze.bronze_order_items_cdc (
                        _loaded_at VARCHAR,
                        _source VARCHAR,
                        _batch_id VARCHAR,
                        _cdc_op VARCHAR,
                        _cdc_ts_ms BIGINT,
                        item_id BIGINT,
                        order_id BIGINT,
                        product_id BIGINT,
                        quantity BIGINT,
                        unit_price DOUBLE,
                        line_total DOUBLE,
                        created_at VARCHAR
                    );
                """)
                df_items = pd.DataFrame(order_items_batch)
                con.register("df_items", df_items)
                con.execute("INSERT INTO bronze.bronze_order_items_cdc SELECT * FROM df_items;")
                counts["order_items"] = len(order_items_batch)

            logger.info("Batch %s landed in bronze: %s", batch_id, counts)
            return counts
        finally:
            con.close()

if __name__ == "__main__":
    simulator = OLTPDataSimulator()
    seed_evts = simulator.seed(num_customers=20, num_products=10, num_orders=50)
    consumer = CDCKafkaConsumer()
    res = consumer.process_cdc_events(seed_evts)
    print("Bronze CDC landing result:", res)
