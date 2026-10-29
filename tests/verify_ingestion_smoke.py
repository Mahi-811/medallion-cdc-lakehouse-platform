"""
Smoke test to verify Phase 2 ingestion pipelines and Bronze layer landing.
"""
import sys
from pathlib import Path
from datetime import date, timedelta
import duckdb

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.api_extractor import APIExtractor
from ingestion.csv_validator import CSVValidator, generate_sample_csv
from ingestion.oltp_generator import OLTPDataSimulator
from ingestion.cdc_kafka_consumer import CDCKafkaConsumer
from ingestion.config import CSV_LANDING_DIR, WAREHOUSE_DB_PATH

def main():
    print("=== Testing Phase 2 Ingestion Pipelines ===")

    # 1. API Extractor
    print("[1/3] Extracting REST API Weather...")
    ext = APIExtractor()
    weather_data = ext.extract_weather(start_date=date.today() - timedelta(days=5), end_date=date.today())
    ext.land_to_bronze(weather_data)

    # 2. CSV Drops Validator
    print("[2/3] Validating & Landing Supplier CSV...")
    val = CSVValidator()
    csv_file = CSV_LANDING_DIR / "test_supplier_drop.csv"
    generate_sample_csv(csv_file, valid=True, include_schema_evolution=True)
    csv_res = val.process_landing_file(csv_file)
    print("CSV Processed:", csv_res)

    # 3. CDC Postgres Simulation & Kafka Consumer
    print("[3/3] Simulating Postgres OLTP & Landing CDC Stream...")
    sim = OLTPDataSimulator()
    seed_events = sim.seed(num_customers=40, num_products=20, num_orders=100)
    mut_events = sim.simulate_mutations(cycle_count=15)
    consumer = CDCKafkaConsumer()
    cdc_res = consumer.process_cdc_events(seed_events + mut_events)
    print("CDC Landed:", cdc_res)

    # Verification in Warehouse
    print("\n=== Warehouse Bronze Layer Inspection ===")
    con = duckdb.connect(str(WAREHOUSE_DB_PATH))
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='bronze' ORDER BY table_name;").fetchall()
    print("Bronze Tables:", [t[0] for t in tables])
    
    assert len(tables) >= 4, f"Expected at least 4 bronze tables, found {len(tables)}"
    for (t,) in tables:
        count = con.execute(f"SELECT count(*) FROM bronze.{t};").fetchone()[0]
        print(f"  -> bronze.{t}: {count} rows")
        assert count > 0, f"Table bronze.{t} should not be empty!"

    # Check ingestion metadata presence
    for (t,) in tables:
        cols = [c[1] for c in con.execute(f"PRAGMA table_info('bronze.{t}');").fetchall()]
        for req in ["_loaded_at", "_source", "_batch_id"]:
            assert req in cols, f"Metadata column {req} missing in bronze.{t}!"

    con.close()
    print("\n>>> Phase 2 Ingestion verified successfully! All Bronze tables populated with metadata.")

if __name__ == "__main__":
    main()
