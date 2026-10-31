.DEFAULT_GOAL := help
PY ?= python3
VENV ?= .venv
BIN := $(VENV)/bin
PROFILE ?= default   # synthetic profile: small | default | scale

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

setup:  ## Create a virtualenv and install CardPulse with dev tools
	$(PY) -m venv $(VENV)
	$(BIN)/pip install -U pip
	$(BIN)/pip install -e ".[dev]"

synth:  ## Generate the synthetic portfolio into .lakehouse/landing (PROFILE=small|default|scale)
	$(BIN)/cardpulse-synth --config synthetic-data/config/$(strip $(PROFILE)).yaml --env local --clean

ingest:  ## Load every landed file into Bronze (local profile)
	$(BIN)/cardpulse-ingest run --env local --all

report:  ## Show Bronze, Silver and Gold reports: row counts, outcomes, DQ, KPIs, trigger preview
	$(BIN)/cardpulse-ingest report --env local
	$(BIN)/cardpulse-silver report --env local
	$(BIN)/cardpulse-gold report --env local

silver:  ## Conform Bronze into Silver (typing, dedup, SCD2 history, DQ gates)
	$(BIN)/cardpulse-silver run --env local

gold:  ## Build Gold with dbt: star schema, Customer 360, Card Account Snapshot (models + tests)
	$(BIN)/cardpulse-gold run --env local

dbt-docs:  ## Generate and serve the dbt docs site (lineage graph, model and column docs)
	cd dbt && CARDPULSE_GOLD_PATH=../.lakehouse/gold/warehouse.duckdb ../$(BIN)/dbt docs generate --profiles-dir . --target-path ../.lakehouse/gold/dbt/target && ../$(BIN)/dbt docs serve --profiles-dir . --target-path ../.lakehouse/gold/dbt/target

demo: reset synth ingest silver gold report  ## Full local demo: reset, generate, Bronze, Silver, Gold, report

reset:  ## Delete the local lakehouse (landing, tables, catalog, lineage)
	rm -rf .lakehouse

test:  ## Run the test suite
	$(BIN)/pytest

lint:  ## Lint, format check and card-number scan
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .
	$(BIN)/python tools/scan_repo_for_pans.py

fmt:  ## Auto-format
	$(BIN)/ruff format .
	$(BIN)/ruff check . --fix

up:  ## Start the docker profile services (SeaweedFS as S3, Iceberg REST, Marquez)
	docker compose up -d seaweedfs iceberg-rest marquez-db marquez marquez-web

docker-build:  ## Build the CardPulse image locally (CI publishes ghcr.io/nabatripathy/cardpulse)
	docker build -f docker/Dockerfile -t cardpulse:local .

demo-docker:  ## Generate, Bronze, Silver, Gold and verify inside docker against SeaweedFS (S3), Iceberg REST and Marquez
	CARDPULSE_PROFILE=$(strip $(PROFILE)) docker compose --profile run run --rm --build cardpulse

down:  ## Stop the docker profile
	docker compose down

.PHONY: help setup synth ingest silver gold dbt-docs report demo reset test lint fmt up docker-build demo-docker down
