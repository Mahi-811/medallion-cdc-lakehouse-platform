"""
Schema Evolution Verification Test.
Acceptance Criterion: A simulated schema change in the source is detected
and handled without breaking the pipeline; documented how.
"""
import json
import duckdb
from pathlib import Path
from ingestion.csv_validator import CSVValidator, generate_sample_csv

def test_non_breaking_schema_evolution(tmp_path):
    landing_dir = tmp_path / "landing"
    quarantine_dir = tmp_path / "quarantine"
    db_path = tmp_path / "test_evolution.duckdb"

    validator = CSVValidator(landing_dir=landing_dir, quarantine_dir=quarantine_dir, db_path=db_path)

    # 1. Simulate source dropping a CSV with unexpected new columns:
    # 'supplier_quality_rating' and 'warehouse_bay_code'
    csv_file = landing_dir / "evolved_supplier_manifest.csv"
    generate_sample_csv(csv_file, valid=True, include_schema_evolution=True)

    # 2. Process file
    result = validator.process_landing_file(csv_file)
    assert result["status"] == "loaded", "Pipeline should not break on additive schema evolution!"
    assert result["records_count"] == 5

    # 3. Verify in Bronze database that extra fields are safely captured in JSON payload
    con = duckdb.connect(str(db_path))
    rows = con.execute("SELECT extra_attributes_json FROM bronze.bronze_supplier_inventory_csv;").fetchall()
    assert len(rows) == 5

    for (extra_str,) in rows:
        extra_dict = json.loads(extra_str)
        assert "supplier_quality_rating" in extra_dict
        assert "warehouse_bay_code" in extra_dict
        assert extra_dict["supplier_quality_rating"] in ("A+", "A", "B+", "A-")

    con.close()
    print(">>> Non-breaking schema evolution verified: new fields captured in semi-structured JSON.")

def test_breaking_schema_change_isolated_to_quarantine(tmp_path):
    landing_dir = tmp_path / "landing"
    quarantine_dir = tmp_path / "quarantine"
    db_path = tmp_path / "test_evolution_bad.duckdb"

    validator = CSVValidator(landing_dir=landing_dir, quarantine_dir=quarantine_dir, db_path=db_path)

    # Simulate breaking change: header renamed/missing required field
    bad_csv = landing_dir / "breaking_schema_manifest.csv"
    with open(bad_csv, "w", encoding="utf-8") as f:
        f.write("supplier_id,unknown_column\n101,foo\n")

    result = validator.process_landing_file(bad_csv)
    assert result["status"] == "quarantined", "Breaking schema change must be safely quarantined!"
    assert result["errors_count"] > 0

    # Assert Bronze layer remains completely uncorrupted
    con = duckdb.connect(str(db_path))
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='bronze';").fetchall()
    assert len(tables) == 0, "No tables should be created or contaminated by a quarantined file."
    con.close()
    print(">>> Breaking schema change isolated to quarantine without pipeline disruption.")
