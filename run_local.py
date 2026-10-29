"""
One-Command Local Pipeline Setup & Verification Script.
Executes the full Medallion lakehouse platform end-to-end:
1. Ingests REST API weather data with rate limiting & retries
2. Validates and lands supplier CSV drops with schema contracts
3. Simulates Postgres OLTP database and lands Debezium CDC streams
4. Builds dbt models across Bronze, Silver, Gold, and Marts layers
5. Verifies all 79+ data tests and singular business integrity tests
"""
import sys
import subprocess
from pathlib import Path
from datetime import date, timedelta
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.api_extractor import APIExtractor
from ingestion.csv_validator import CSVValidator, generate_sample_csv
from ingestion.oltp_generator import OLTPDataSimulator
from ingestion.cdc_kafka_consumer import CDCKafkaConsumer
from ingestion.config import CSV_LANDING_DIR, WAREHOUSE_DB_PATH

def print_banner(text: str):
    print("\n" + "=" * 65)
    print(f"  {text}")
    print("=" * 65)

def main():
    print_banner("Medallion Batch + CDC Lakehouse Platform: Local Execution")

    # Step 1: REST API Weather Ingestion
    print("\n[Step 1/4] Ingesting REST API Weather Indicators...")
    api = APIExtractor()
    weather_records = api.extract_weather(start_date=date.today() - timedelta(days=7), end_date=date.today())
    api_cnt = api.land_to_bronze(weather_records)
    print(f"-> Landed {api_cnt} records into bronze.bronze_weather_api")

    # Step 2: CSV Drops Ingestion
    print("\n[Step 2/4] Validating & Landing Supplier Inventory CSV Drops...")
    csv_val = CSVValidator()
    drop_file = CSV_LANDING_DIR / "supplier_catalog_restock.csv"
    generate_sample_csv(drop_file, valid=True, include_schema_evolution=True)
    csv_res = csv_val.process_landing_file(drop_file)
    print(f"-> CSV Ingestion Status: {csv_res['status']} ({csv_res.get('records_count', 0)} rows)")

    # Step 3: Postgres OLTP CDC Simulation & Consumption
    print("\n[Step 3/4] Generating Postgres OLTP Transactions & CDC Streams...")
    sim = OLTPDataSimulator()
    seed_evts = sim.seed(num_customers=50, num_products=25, num_orders=150)
    mut_evts = sim.simulate_mutations(cycle_count=20)
    consumer = CDCKafkaConsumer()
    cdc_res = consumer.process_cdc_events(seed_evts + mut_evts)
    print(f"-> CDC Records Landed: {cdc_res}")

    # Step 4: Run dbt Build
    print("\n[Step 4/4] Executing dbt Build (Bronze -> Silver -> Gold -> Marts + Tests)...")
    dbt_dir = PROJECT_ROOT / "dbt"
    res = subprocess.run(["dbt", "build", "--profiles-dir", "."], cwd=str(dbt_dir), capture_output=True, text=True)
    if res.returncode != 0:
        print("dbt build failed!\nSTDOUT:\n", res.stdout, "\nSTDERR:\n", res.stderr)
        sys.exit(1)
    print("-> dbt build completed with 0 test failures!")

    # Summary Inspection
    print_banner("Warehouse Layer Metrics Summary")
    con = duckdb.connect(str(WAREHOUSE_DB_PATH))
    tables = [
        ("Bronze", "bronze.bronze_orders_cdc"),
        ("Bronze", "bronze.bronze_customers_cdc"),
        ("Bronze", "bronze.bronze_products_cdc"),
        ("Bronze", "bronze.bronze_supplier_inventory_csv"),
        ("Bronze", "bronze.bronze_weather_api"),
        ("Silver", "main_silver.silver_orders"),
        ("Silver", "main_silver.silver_customers"),
        ("Silver", "main_silver.silver_products"),
        ("Gold",   "main_gold.dim_customer"),
        ("Gold",   "main_gold.dim_product"),
        ("Gold",   "main_gold.dim_date"),
        ("Gold",   "main_gold.fact_orders"),
        ("Marts",  "main_marts.mart_daily_revenue"),
        ("Marts",  "main_marts.mart_customer_ltv"),
        ("Marts",  "main_marts.mart_product_performance")
    ]

    for layer, tbl in tables:
        try:
            cnt = con.execute(f"SELECT count(*) FROM {tbl};").fetchone()[0]
            print(f"  [{layer:<6}] {tbl:<42} : {cnt:>6} rows")
        except Exception as e:
            print(f"  [{layer:<6}] {tbl:<42} : Error ({e})")
    con.close()

    print_banner("Platform Pipeline Successfully Verified & Ready!")

if __name__ == "__main__":
    main()
