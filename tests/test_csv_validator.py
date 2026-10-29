"""
Unit & Integration Tests for CSV Drops Validator.
Verifies schema contract enforcement, quarantine routing, and diagnostic logging.
"""
import json
import pytest
from pathlib import Path
from ingestion.csv_validator import CSVValidator, generate_sample_csv

def test_valid_csv_ingestion(tmp_path):
    landing_dir = tmp_path / "landing"
    quarantine_dir = tmp_path / "quarantine"
    db_path = tmp_path / "test_warehouse.duckdb"

    validator = CSVValidator(landing_dir=landing_dir, quarantine_dir=quarantine_dir, db_path=db_path)
    csv_file = landing_dir / "valid_supplier_manifest.csv"
    generate_sample_csv(csv_file, valid=True)

    result = validator.process_landing_file(csv_file)
    assert result["status"] == "loaded"
    assert result["records_count"] == 5
    assert not csv_file.exists(), "Processed file should be cleaned from landing directory"

def test_invalid_csv_quarantine_routing(tmp_path):
    landing_dir = tmp_path / "landing"
    quarantine_dir = tmp_path / "quarantine"
    db_path = tmp_path / "test_warehouse.duckdb"

    validator = CSVValidator(landing_dir=landing_dir, quarantine_dir=quarantine_dir, db_path=db_path)
    csv_file = landing_dir / "bad_supplier_manifest.csv"
    generate_sample_csv(csv_file, valid=False)

    result = validator.process_landing_file(csv_file)
    assert result["status"] == "quarantined"
    assert result["errors_count"] > 0
    assert not csv_file.exists(), "Corrupted file must be moved out of landing"

    # Check diagnostic report in quarantine directory
    quarantined_files = list(quarantine_dir.glob("*_diagnostics.json"))
    assert len(quarantined_files) == 1
    with open(quarantined_files[0], "r", encoding="utf-8") as f:
        diag = json.load(f)
        assert len(diag["errors"]) > 0
        assert "bad_supplier_manifest.csv" in diag["source_file"]
