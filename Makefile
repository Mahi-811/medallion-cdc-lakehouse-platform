.PHONY: help start test lint dbt-build docs backfill docker-up docker-down

help:
	@echo "Medallion Data Platform - Command Center"
	@echo "  make start        Run the end-to-end pipeline locally (Ingestion + dbt build)"
	@echo "  make test         Execute full pytest test suite"
	@echo "  make lint         Run sqlfluff lint on SQL and dbt models"
	@echo "  make dbt-build    Execute dbt build against lakehouse"
	@echo "  make docs         Generate and view dbt documentation"
	@echo "  make backfill     Run the backfill utility"
	@echo "  make docker-up    Start Docker services (Postgres, Kafka, Debezium, MinIO, Airflow)"
	@echo "  make docker-down  Stop Docker services"

start:
	python run_local.py

test:
	pytest tests/ -v

lint:
	sqlfluff lint dbt/models/ --dialect duckdb

dbt-build:
	cd dbt && dbt build --profiles-dir .

docs:
	cd dbt && dbt docs generate --profiles-dir .

backfill:
	python orchestration/backfill.py --layer all --start-date 2026-10-01 --end-date 2026-10-15

docker-up:
	docker compose up -d

docker-down:
	docker compose down
