"""Shared fixtures.

Set TEST_TABLE_FORMAT=delta to run the whole suite against Delta Lake tables instead of
Parquet (CI runs both).
"""

from __future__ import annotations

import csv
import os
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from flight_lakehouse.config import AIRPORTS_FILE, FLIGHTS_FILE, Settings
from flight_lakehouse.spark_session import get_spark

TABLE_FORMAT = os.getenv("TEST_TABLE_FORMAT", "parquet")

FLIGHT_SCHEMA = StructType(
    [
        StructField("date", TimestampType(), True),
        StructField("delay", LongType(), True),
        StructField("distance", LongType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True),
    ]
)

AIRPORT_HEADER = ["iata", "name", "city", "state", "country", "latitude", "longitude"]


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = get_spark("flight-lakehouse-tests", table_format=TABLE_FORMAT, shuffle_partitions=2)
    yield session
    session.stop()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        root=str(tmp_path / "lake"),
        landing=str(tmp_path / "landing"),
        table_format=TABLE_FORMAT,
        shuffle_partitions=2,
    )


@pytest.fixture
def landing_writer(spark: SparkSession, settings: Settings) -> Callable[[list, list], None]:
    """Write small flights/airports fixtures into the landing zone, in the source formats."""

    def write(flights: list[tuple], airports: list[tuple]) -> None:
        Path(settings.landing).mkdir(parents=True, exist_ok=True)
        (
            spark.createDataFrame(flights, FLIGHT_SCHEMA)
            .coalesce(1)
            .write.mode("overwrite")
            .parquet(settings.landing_path(FLIGHTS_FILE))
        )
        with open(settings.landing_path(AIRPORTS_FILE), "w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(AIRPORT_HEADER)
            writer.writerows(airports)

    return write


def utc(*args: int) -> datetime:
    """A timezone-aware UTC datetime, so fixtures mean the same thing on every machine."""
    return datetime(*args, tzinfo=timezone.utc)


# A small, hand-checked dataset: 3 good flights and 5 that each break one or more rules.
AIRPORTS = [
    ("LAX", "Los Angeles Intl", "Los Angeles", "CA", "USA", "33.94", "-118.40"),
    ("JFK", "John F Kennedy Intl", "New York", "NY", "USA", "40.63", "-73.77"),
    ("ATL", "Hartsfield-Jackson", "Atlanta", "GA", "USA", "33.64", "-84.42"),
    ("JFK", "Duplicate JFK row", "New York", "NY", "USA", "40.63", "-73.77"),
]

FLIGHTS = [
    # good: lowercase/whitespace codes must be normalised; 20 min late counts as delayed
    (utc(2001, 1, 1, 8, 30), 20, 2475, " lax ", "jfk"),
    # good: on time
    (utc(2001, 1, 1, 9, 0), 5, 761, "ATL", "LAX"),
    # good: airport missing from the dimension is kept but flagged
    (utc(2001, 2, 3, 10, 0), 0, 300, "XXX", "ATL"),
    # bad: implausible delay
    (utc(2001, 1, 2, 10, 0), -500, 300, "ATL", "JFK"),
    # bad: origin equals destination
    (utc(2001, 1, 2, 11, 0), 10, 100, "ATL", "ATL"),
    # bad: zero distance
    (utc(2001, 1, 2, 12, 0), 10, 0, "ATL", "JFK"),
    # bad: missing timestamp
    (None, 10, 300, "ATL", "JFK"),
    # bad: breaks several rules at once
    (None, 5000, -5, "AT", "ATL"),
]
