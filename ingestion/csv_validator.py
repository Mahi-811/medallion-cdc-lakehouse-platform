"""
CSV Drops Validator & Ingestor Module.
Validates arriving CSV files against strict schema contracts using Pydantic.
- Valid records land in Bronze layer (`bronze_supplier_inventory_csv`) with metadata.
- Corrupted/malformed files are routed to `csv_quarantine/` with diagnostic logs.
- Accommodates non-breaking schema evolution (additional columns safely captured).
"""
from __future__ import annotations
import os
import csv
import json
import uuid
import shutil
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator, ValidationError
import duckdb
import pandas as pd

from ingestion.config import CSV_LANDING_DIR, CSV_QUARANTINE_DIR, WAREHOUSE_DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("CSVValidator")

class SupplierInventoryRecord(BaseModel):
    """Pydantic schema definition for supplier inventory CSV drops."""
    supplier_id: int = Field(..., description="Unique supplier identifier", ge=1)
    supplier_name: str = Field(..., min_length=2, max_length=100)
    product_sku: str = Field(..., min_length=3, max_length=50)
    category: str = Field(..., min_length=2)
    unit_cost: float = Field(..., gt=0.0, description="Cost per unit must be strictly positive")
    available_stock: int = Field(..., ge=0, description="Stock cannot be negative")
    replenishment_lead_days: int = Field(..., ge=1)
    restock_date: str = Field(..., description="ISO Date string YYYY-MM-DD")
    
    # Catch-all for schema evolution (allows upstream suppliers to add new columns without pipeline failure)
    extra_attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("restock_date")
    @classmethod
    def validate_date(cls, v: str) -> str:
        try:
            date.fromisoformat(v)
            return v
        except ValueError as err:
            raise ValueError(f"restock_date must be in ISO format YYYY-MM-DD, got {v}") from err

class CSVValidator:
    """Validates files landing in object store folders and routes to bronze or quarantine."""
    def __init__(
        self,
        landing_dir: Path | str = CSV_LANDING_DIR,
        quarantine_dir: Path | str = CSV_QUARANTINE_DIR,
        db_path: Path | str = WAREHOUSE_DB_PATH
    ):
        self.landing_dir = Path(landing_dir)
        self.quarantine_dir = Path(quarantine_dir)
        self.db_path = str(db_path)
        self.landing_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

    def validate_file(self, file_path: Path) -> tuple[bool, list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Parses and validates a single CSV file against the schema contract.
        Returns: (is_valid, validated_records, error_diagnostics)
        """
        valid_records: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        standard_fields = {
            "supplier_id", "supplier_name", "product_sku", "category",
            "unit_cost", "available_stock", "replenishment_lead_days", "restock_date"
        }

        try:
            with open(file_path, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []
                
                # Check for critical missing headers
                missing_required = standard_fields - set(headers)
                if missing_required:
                    errors.append({
                        "file": file_path.name,
                        "row_num": 0,
                        "error_type": "SchemaContractViolation",
                        "message": f"Missing required column headers: {sorted(list(missing_required))}"
                    })
                    return False, [], errors

                for row_idx, row in enumerate(reader, start=1):
                    row_data = {}
                    extras = {}
                    for k, v in row.items():
                        if k in standard_fields:
                            row_data[k] = v
                        else:
                            extras[k] = v
                    row_data["extra_attributes"] = extras

                    try:
                        record = SupplierInventoryRecord(**row_data)
                        valid_records.append(record.model_dump())
                    except ValidationError as ve:
                        errors.append({
                            "file": file_path.name,
                            "row_num": row_idx,
                            "raw_record": row,
                            "error_type": "RowValidationError",
                            "message": ve.errors()
                        })

        except Exception as ex:
            errors.append({
                "file": file_path.name,
                "row_num": 0,
                "error_type": "FileReadError",
                "message": str(ex)
            })
            return False, [], errors

        # If any validation errors exist, file is deemed invalid (fail-closed contract)
        is_valid = len(errors) == 0
        return is_valid, valid_records, errors

    def process_landing_file(self, file_path: Path) -> dict[str, Any]:
        """
        Validates a file on arrival.
        - If valid: lands into Bronze layer with metadata and removes from landing.
        - If invalid: routes to quarantine folder with diagnostic report.
        """
        batch_id = str(uuid.uuid4())
        loaded_at = datetime.utcnow().isoformat()
        is_valid, records, errors = self.validate_file(file_path)

        if not is_valid:
            logger.warning("Validation failed for '%s' with %d errors. Quarantining file.", file_path.name, len(errors))
            timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            quarantined_file = self.quarantine_dir / f"{timestamp_str}_{file_path.name}"
            diagnostic_file = self.quarantine_dir / f"{timestamp_str}_{file_path.stem}_diagnostics.json"

            shutil.move(str(file_path), str(quarantined_file))
            with open(diagnostic_file, "w", encoding="utf-8") as diag_f:
                json.dump({
                    "batch_id": batch_id,
                    "quarantined_at": loaded_at,
                    "source_file": file_path.name,
                    "errors": errors
                }, diag_f, indent=2, default=str)

            return {
                "status": "quarantined",
                "file": file_path.name,
                "errors_count": len(errors),
                "quarantine_path": str(quarantined_file)
            }

        # Valid file: enrich records with bronze metadata
        bronze_records = []
        for r in records:
            extra_json = json.dumps(r.get("extra_attributes", {}))
            bronze_records.append({
                "_loaded_at": loaded_at,
                "_source": "csv_drops_supplier_inventory",
                "_batch_id": batch_id,
                "_filename": file_path.name,
                "supplier_id": r["supplier_id"],
                "supplier_name": r["supplier_name"],
                "product_sku": r["product_sku"],
                "category": r["category"],
                "unit_cost": float(r["unit_cost"]),
                "available_stock": int(r["available_stock"]),
                "replenishment_lead_days": int(r["replenishment_lead_days"]),
                "restock_date": str(r["restock_date"]),
                "extra_attributes_json": extra_json
            })

        self.land_to_bronze(bronze_records)
        # Clean up processed file from landing
        file_path.unlink()

        return {
            "status": "loaded",
            "file": file_path.name,
            "records_count": len(bronze_records),
            "batch_id": batch_id
        }

    def land_to_bronze(self, records: list[dict[str, Any]]) -> int:
        """Appends validated records into Bronze layer table `bronze_supplier_inventory_csv`."""
        if not records:
            return 0

        con = duckdb.connect(self.db_path)
        try:
            con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
            con.execute("""
                CREATE TABLE IF NOT EXISTS bronze.bronze_supplier_inventory_csv (
                    _loaded_at VARCHAR,
                    _source VARCHAR,
                    _batch_id VARCHAR,
                    _filename VARCHAR,
                    supplier_id BIGINT,
                    supplier_name VARCHAR,
                    product_sku VARCHAR,
                    category VARCHAR,
                    unit_cost DOUBLE,
                    available_stock BIGINT,
                    replenishment_lead_days BIGINT,
                    restock_date VARCHAR,
                    extra_attributes_json VARCHAR
                );
            """)

            df = pd.DataFrame(records)
            con.register("df_csv_incoming", df)
            con.execute("INSERT INTO bronze.bronze_supplier_inventory_csv SELECT * FROM df_csv_incoming;")
            total_count = con.execute("SELECT count(*) FROM bronze.bronze_supplier_inventory_csv;").fetchone()[0]
            logger.info("Landed %d rows into bronze.bronze_supplier_inventory_csv (Total: %d)", len(records), total_count)
            return len(records)
        finally:
            con.close()

def generate_sample_csv(
    target_path: Path,
    valid: bool = True,
    include_schema_evolution: bool = False
) -> Path:
    """Generates synthetic CSV files for testing validation and schema evolution."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    today_str = date.today().isoformat()

    if valid:
        headers = [
            "supplier_id", "supplier_name", "product_sku", "category",
            "unit_cost", "available_stock", "replenishment_lead_days", "restock_date"
        ]
        if include_schema_evolution:
            # Simulate upstream supplier adding non-breaking metadata columns
            headers.extend(["supplier_quality_rating", "warehouse_bay_code"])

        rows = [
            [101, "Apex Logistics", "SKU-TECH-001", "Electronics", 299.50, 150, 5, today_str],
            [101, "Apex Logistics", "SKU-TECH-002", "Electronics", 89.99, 420, 3, today_str],
            [102, "Nordic Supply", "SKU-FURN-001", "Furniture", 145.00, 60, 14, today_str],
            [103, "Pacific Apparel", "SKU-CLOTH-001", "Apparel", 24.50, 850, 7, today_str],
            [103, "Pacific Apparel", "SKU-CLOTH-002", "Apparel", 39.00, 310, 4, today_str]
        ]
        if include_schema_evolution:
            rows[0].extend(["A+", "BAY-W1"])
            rows[1].extend(["A", "BAY-W2"])
            rows[2].extend(["B+", "BAY-E1"])
            rows[3].extend(["A-", "BAY-S3"])
            rows[4].extend(["A", "BAY-S4"])
    else:
        # Invalid CSV: negative unit_cost and invalid restock_date format
        headers = [
            "supplier_id", "supplier_name", "product_sku", "category",
            "unit_cost", "available_stock", "replenishment_lead_days", "restock_date"
        ]
        rows = [
            [101, "Apex Logistics", "SKU-TECH-001", "Electronics", -50.00, 150, 5, "invalid-date-format"],
            [102, "Bad Supplier", "SKU-BAD-001", "Toys", 12.00, -10, 0, today_str]
        ]

    with open(target_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    return target_path

if __name__ == "__main__":
    validator = CSVValidator()
    # Generate and test valid sample
    sample_file = CSV_LANDING_DIR / "sample_supplier_drop.csv"
    generate_sample_csv(sample_file, valid=True, include_schema_evolution=True)
    result = validator.process_landing_file(sample_file)
    print("Process result:", result)
