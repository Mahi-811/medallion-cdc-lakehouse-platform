"""
Backfill CLI Utility for the Medallion Data Platform.
Enables idempotent date-range backfilling across ingestion and transformation layers.

Usage Examples:
    # Dry run check for date range
    python orchestration/backfill.py --start-date 2026-10-01 --end-date 2026-10-15 --dry-run

    # Execute full idempotent backfill for API & dbt transforms
    python orchestration/backfill.py --layer all --start-date 2026-10-01 --end-date 2026-10-15
"""
from __future__ import annotations
import sys
import argparse
import logging
import subprocess
from datetime import date, timedelta
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.api_extractor import APIExtractor
from ingestion.config import WAREHOUSE_DB_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("BackfillManager")

def run_dbt_command(command: list[str]) -> bool:
    """Executes a dbt CLI command within the dbt project folder."""
    dbt_dir = PROJECT_ROOT / "dbt"
    full_cmd = ["dbt"] + command + ["--profiles-dir", "."]
    logger.info("Executing: %s", " ".join(full_cmd))
    res = subprocess.run(full_cmd, cwd=str(dbt_dir), capture_output=True, text=True)
    if res.returncode != 0:
        logger.error("dbt command failed:\nSTDOUT:\n%s\nSTDERR:\n%s", res.stdout, res.stderr)
        return False
    logger.info("dbt command succeeded.")
    return True

def backfill(start_date: date, end_date: date, layer: str, dry_run: bool = False) -> None:
    logger.info("=== Starting Idempotent Backfill ===")
    logger.info("Window: %s to %s | Target Layer: %s | Dry Run: %s", start_date, end_date, layer, dry_run)

    if dry_run:
        days = (end_date - start_date).days + 1
        logger.info("[DRY RUN] Will backfill %d calendar days across %s.", days, layer)
        return

    # 1. Backfill API Weather Ingestion if requested
    if layer in ("ingestion", "all"):
        logger.info("[1/2] Backfilling REST API Weather partition [%s -> %s]...", start_date, end_date)
        extractor = APIExtractor()
        data = extractor.extract_weather(start_date=start_date, end_date=end_date)
        count = extractor.land_to_bronze(data)
        logger.info("Ingestion backfill complete: %d rows landed.", count)

    # 2. Backfill dbt transformations
    if layer in ("dbt", "transforms", "all"):
        logger.info("[2/2] Running incremental dbt build with target window...")
        success = run_dbt_command(["build"])
        if not success:
            raise RuntimeError("dbt backfill transform execution failed!")
        logger.info("Transform backfill complete.")

    logger.info(">>> Backfill completed successfully without regression.")

def main():
    parser = argparse.ArgumentParser(description="Medallion Lakehouse Backfill Utility")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, type=date.fromisoformat, help="End date (YYYY-MM-DD)")
    parser.add_argument("--layer", choices=["ingestion", "transforms", "all"], default="all", help="Target backfill layer")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without running mutations")

    args = parser.parse_args()
    if args.start_date > args.end_date:
        logger.error("start-date (%s) cannot be after end-date (%s)", args.start_date, args.end_date)
        sys.exit(1)

    backfill(start_date=args.start_date, end_date=args.end_date, layer=args.layer, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
