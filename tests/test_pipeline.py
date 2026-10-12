from dataclasses import replace

import pytest

from flight_lakehouse.pipeline import run_pipeline
from flight_lakehouse.tables import read_table

from .conftest import AIRPORTS, FLIGHTS


def _read(spark, settings, layer, name):
    return read_table(spark, settings.table_path(layer, name), settings.table_format)


@pytest.fixture
def lenient(settings):
    return replace(settings, max_quarantine_ratio=1.0)


def test_full_run_builds_every_layer(spark, lenient, landing_writer):
    landing_writer(FLIGHTS, AIRPORTS)

    metrics = run_pipeline(lenient, spark=spark)

    assert set(metrics["stages"]) == {"bronze", "silver", "gold"}
    assert metrics["stages"]["bronze"]["result"] == {"flights": 8, "airports": 4}
    silver = metrics["stages"]["silver"]["result"]
    assert silver["silver_rows"] + silver["quarantined_rows"] == silver["bronze_rows"] == 8

    for layer, name in [
        ("bronze", "flights"),
        ("silver", "flights"),
        ("silver", "flights_quarantine"),
        ("gold", "route_performance_monthly"),
        ("gold", "airport_daily"),
        ("gold", "hourly_delay_profile"),
        ("gold", "origin_state_monthly"),
    ]:
        assert _read(spark, lenient, layer, name).count() > 0, f"{layer}.{name} is empty"


def test_bronze_keeps_raw_rows_and_adds_lineage_columns(spark, lenient, landing_writer):
    landing_writer(FLIGHTS, AIRPORTS)

    run_pipeline(lenient, stages=("bronze",), spark=spark)
    bronze = _read(spark, lenient, "bronze", "flights")

    assert {"_source_file", "_ingested_at", "_batch_id"} <= set(bronze.columns)
    # raw means raw: the whitespace/lowercase codes and the bad rows are all still there
    assert bronze.filter("origin = ' lax '").count() == 1
    assert bronze.count() == len(FLIGHTS)
    assert "flights-3m.parquet" in bronze.select("_source_file").first()[0]


def test_rerunning_is_idempotent(spark, lenient, landing_writer):
    landing_writer(FLIGHTS, AIRPORTS)

    first = run_pipeline(lenient, spark=spark)
    counts_after_first = {
        "silver": _read(spark, lenient, "silver", "flights").count(),
        "gold": _read(spark, lenient, "gold", "airport_daily").count(),
    }
    second = run_pipeline(lenient, spark=spark)

    assert counts_after_first == {
        "silver": _read(spark, lenient, "silver", "flights").count(),
        "gold": _read(spark, lenient, "gold", "airport_daily").count(),
    }
    assert first["stages"]["silver"]["result"] == second["stages"]["silver"]["result"]
    assert first["batch_id"] != second["batch_id"]


def test_every_run_is_logged(spark, lenient, landing_writer):
    landing_writer(FLIGHTS, AIRPORTS)

    run_pipeline(lenient, spark=spark)
    run_pipeline(lenient, spark=spark)

    log = _read(spark, lenient, "ops", "run_log")
    assert log.count() == 2
    assert {r.status for r in log.collect()} == {"SUCCEEDED"}


def test_failed_run_is_logged_and_raises(spark, settings, landing_writer):
    landing_writer(FLIGHTS, AIRPORTS)  # too dirty for the default 1% quarantine limit

    with pytest.raises(Exception, match="quarantined"):
        run_pipeline(settings, spark=spark)

    log = _read(spark, settings, "ops", "run_log").collect()
    assert [r.status for r in log] == ["FAILED"]
    assert "DataQualityError" in log[0].metrics_json


def test_unknown_stage_is_rejected(spark, settings):
    with pytest.raises(ValueError, match="unknown stages"):
        run_pipeline(settings, stages=("bronze", "platinum"), spark=spark)
