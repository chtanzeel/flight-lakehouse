.PHONY: setup lint test test-delta ingest run clean

PYTHON ?= python3

setup:  ## create a virtualenv and install the project with dev tools
	$(PYTHON) -m venv .venv
	.venv/bin/pip install -e ".[dev]"

lint:
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

test:  ## run the suite against Parquet tables (no extra downloads)
	TEST_TABLE_FORMAT=parquet .venv/bin/pytest

test-delta:  ## run the suite against Delta Lake tables (downloads the Delta jars on first run)
	TEST_TABLE_FORMAT=delta .venv/bin/pytest

ingest:  ## download the source files into data/landing
	.venv/bin/python -m flight_lakehouse.ingest

run: ingest  ## download the data and run bronze -> silver -> gold
	.venv/bin/python -m flight_lakehouse.pipeline

clean:
	rm -rf data/lake spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
