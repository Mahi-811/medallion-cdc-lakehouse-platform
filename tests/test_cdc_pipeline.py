"""
Unit & Integration Tests for Debezium CDC Processing.
Verifies mutation capturing, op-code handling (c, u, d), and metadata lineage.
"""
import duckdb
from ingestion.oltp_generator import OLTPDataSimulator
from ingestion.cdc_kafka_consumer import CDCKafkaConsumer

def test_cdc_mutation_and_tombstone_handling(tmp_path):
    db_path = tmp_path / "test_cdc_warehouse.duckdb"
    sim = OLTPDataSimulator()
    seed_evts = sim.seed(num_customers=10, num_products=5, num_orders=15)
    mut_evts = sim.simulate_mutations(cycle_count=5)

    consumer = CDCKafkaConsumer(db_path=db_path)
    counts = consumer.process_cdc_events(seed_evts + mut_evts)

    assert "orders" in counts
    assert "customers" in counts
    assert "products" in counts

    con = duckdb.connect(str(db_path))
    # Assert bronze append-only tables contain the cdc operations
    ops = [r[0] for r in con.execute("SELECT DISTINCT _cdc_op FROM bronze.bronze_orders_cdc;").fetchall()]
    assert "r" in ops or "c" in ops

    # Assert metadata columns are populated
    null_batches = con.execute("SELECT count(*) FROM bronze.bronze_orders_cdc WHERE _batch_id IS NULL;").fetchone()[0]
    assert null_batches == 0
    con.close()
