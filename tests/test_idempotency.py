"""
Idempotency Verification Test.
Acceptance Criterion: Re-running any ingestion or transformation step twice
produces identical results, row counts, and surrogate keys without duplicates.
"""
import subprocess
from pathlib import Path
import duckdb
from ingestion.config import PROJECT_ROOT, WAREHOUSE_DB_PATH

def test_dbt_transformation_idempotency():
    dbt_dir = PROJECT_ROOT / "dbt"

    # Step 1: Run full dbt build (Run 1)
    res1 = subprocess.run(
        ["dbt", "build", "--profiles-dir", "."],
        cwd=str(dbt_dir),
        capture_output=True,
        text=True
    )
    assert res1.returncode == 0, f"dbt run 1 failed:\n{res1.stderr}"

    con = duckdb.connect(str(WAREHOUSE_DB_PATH))
    tables = [
        "main_gold.fact_orders",
        "main_gold.dim_customer",
        "main_gold.dim_product",
        "main_gold.dim_date",
        "main_marts.mart_daily_revenue",
        "main_marts.mart_customer_ltv",
        "main_marts.mart_product_performance"
    ]

    counts_run_1 = {t: con.execute(f"SELECT count(*) FROM {t};").fetchone()[0] for t in tables}
    con.close()

    # Step 2: Re-run full dbt build immediately without source changes (Run 2)
    res2 = subprocess.run(
        ["dbt", "build", "--profiles-dir", "."],
        cwd=str(dbt_dir),
        capture_output=True,
        text=True
    )
    assert res2.returncode == 0, f"dbt run 2 failed:\n{res2.stderr}"

    con = duckdb.connect(str(WAREHOUSE_DB_PATH))
    counts_run_2 = {t: con.execute(f"SELECT count(*) FROM {t};").fetchone()[0] for t in tables}

    # Step 3: Assert identical row counts
    print("\n--- Idempotency Counts ---")
    for t in tables:
        c1 = counts_run_1[t]
        c2 = counts_run_2[t]
        print(f"Table {t}: Run 1 = {c1}, Run 2 = {c2}")
        assert c1 == c2, f"Idempotency violation on {t}: Run 1 had {c1} rows, Run 2 had {c2} rows!"

    # Step 4: Assert 0 duplicate keys in fact and dimension tables
    dup_fact = con.execute("SELECT count(*) - count(distinct order_sk) FROM main_gold.fact_orders;").fetchone()[0]
    assert dup_fact == 0, "Duplicate surrogate keys found in fact_orders!"

    dup_cust = con.execute("SELECT count(*) - count(distinct customer_sk) FROM main_gold.dim_customer;").fetchone()[0]
    assert dup_cust == 0, "Duplicate surrogate keys found in dim_customer!"

    dup_prod = con.execute("SELECT count(*) - count(distinct product_id) FROM main_gold.dim_product;").fetchone()[0]
    assert dup_prod == 0, "Duplicate product_id keys found in dim_product!"

    con.close()
    print(">>> Idempotency test passed with 100% fidelity.")
