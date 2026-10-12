from dataclasses import replace

import pytest

from flight_lakehouse.bronze import build_bronze
from flight_lakehouse.quality import DataQualityError
from flight_lakehouse.silver import build_silver
from flight_lakehouse.tables import read_table

from .conftest import AIRPORTS, FLIGHTS


@pytest.fixture
def lenient(settings):
    # The fixture data is deliberately dirty, so allow a high quarantine ratio.
    return replace(settings, max_quarantine_ratio=1.0)


@pytest.fixture
def silver_run(spark, lenient, landing_writer):
    landing_writer(FLIGHTS, AIRPORTS)
    build_bronze(spark, lenient, batch_id="test")
    metrics = build_silver(spark, lenient)
    return lenient, metrics


def _table(spark, settings, layer, name):
    return read_table(spark, settings.table_path(layer, name), settings.table_format)


def test_rows_are_split_between_silver_and_quarantine(spark, silver_run):
    settings, metrics = silver_run

    assert metrics["bronze_rows"] == 8
    assert metrics["silver_rows"] == 3
    assert metrics["quarantined_rows"] == 5
    assert _table(spark, settings, "silver", "flights").count() == 3
    assert _table(spark, settings, "silver", "flights_quarantine").count() == 5


def test_quarantine_records_why_each_row_failed(silver_run):
    _, metrics = silver_run

    assert metrics["quarantined_by_rule"] == {
        "delay_within_bounds": 2,
        "origin_differs_from_destination": 1,
        "distance_is_positive": 2,
        "departure_ts_not_null": 2,
        "origin_is_iata_code": 1,
    }


def test_codes_are_normalised_and_flights_derived(spark, silver_run):
    settings, _ = silver_run
    silver = _table(spark, settings, "silver", "flights")

    row = silver.filter("origin = 'LAX' and destination = 'JFK'").collect()
    assert len(row) == 1
    row = row[0]
    assert str(row.flight_date) == "2001-01-01"
    assert row.flight_month == "2001-01"
    assert row.departure_hour == 8
    assert row.is_delayed is True
    assert row.origin_city == "Los Angeles"
    assert row.dest_state == "NY"


def test_on_time_flight_is_not_delayed(spark, silver_run):
    settings, _ = silver_run
    silver = _table(spark, settings, "silver", "flights")

    on_time = silver.filter("origin = 'ATL' and destination = 'LAX'").collect()[0]
    assert on_time.is_delayed is False


def test_unknown_airport_is_kept_and_flagged(spark, silver_run):
    settings, _ = silver_run
    silver = _table(spark, settings, "silver", "flights")

    row = silver.filter("origin = 'XXX'").collect()[0]
    assert row.origin_in_airport_dim is False
    assert row.origin_state is None
    assert row.dest_in_airport_dim is True


def test_airport_dimension_has_one_row_per_code(spark, silver_run):
    settings, metrics = silver_run
    airports = _table(spark, settings, "silver", "airports")

    assert metrics["airports"] == 3
    assert airports.filter("iata = 'JFK'").count() == 1
    assert airports.schema["latitude"].dataType.simpleString() == "double"


def test_silver_is_partitioned_by_month(silver_run):
    settings, _ = silver_run
    from pathlib import Path

    partitions = sorted(
        p.name for p in Path(settings.table_path("silver", "flights")).glob("flight_month=*")
    )
    assert partitions == ["flight_month=2001-01", "flight_month=2001-02"]


def test_quality_gate_stops_the_run_when_too_many_rows_are_rejected(
    spark, settings, landing_writer
):
    landing_writer(FLIGHTS, AIRPORTS)  # 5 of 8 rows are bad: far above the default 1%
    build_bronze(spark, settings, batch_id="test")

    with pytest.raises(DataQualityError, match="quarantined"):
        build_silver(spark, settings)
