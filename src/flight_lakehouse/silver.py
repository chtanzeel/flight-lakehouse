"""Silver layer: typed, validated, de-noised and enriched data.

Rows that break a quality rule are not dropped silently: they go to a quarantine table
together with the names of the rules they failed, so they can be inspected and replayed.
"""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from flight_lakehouse.config import Settings
from flight_lakehouse.quality import (
    Rule,
    apply_rules,
    check_reconciliation,
    failure_counts,
)
from flight_lakehouse.tables import read_table, write_table

log = logging.getLogger(__name__)

_IATA_PATTERN = r"^[A-Z0-9]{3}$"


def _blank_to_null(column: str) -> Column:
    trimmed = F.trim(F.col(column))
    return F.when(trimmed == "", None).otherwise(trimmed)


def flight_rules(settings: Settings) -> list[Rule]:
    """The quality contract for a flight record."""
    return [
        Rule("departure_ts_not_null", F.col("departure_ts").isNotNull()),
        Rule("origin_is_iata_code", F.col("origin").rlike(_IATA_PATTERN)),
        Rule("destination_is_iata_code", F.col("destination").rlike(_IATA_PATTERN)),
        Rule("origin_differs_from_destination", F.col("origin") != F.col("destination")),
        Rule("distance_is_positive", F.col("distance_miles") > 0),
        Rule(
            "delay_within_bounds",
            F.col("delay_minutes").between(settings.min_delay_minutes, settings.max_delay_minutes),
        ),
    ]


def clean_airports(raw: DataFrame) -> DataFrame:
    """Type the airport dimension, normalise codes and keep one row per IATA code."""
    return (
        raw.select(
            F.upper(F.trim(F.col("iata"))).alias("iata"),
            _blank_to_null("name").alias("airport_name"),
            _blank_to_null("city").alias("city"),
            _blank_to_null("state").alias("state"),
            _blank_to_null("country").alias("country"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
        )
        .filter(F.col("iata").isNotNull() & (F.col("iata") != ""))
        .dropDuplicates(["iata"])
    )


def _airport_lookup(airports: DataFrame, prefix: str, join_key: str) -> DataFrame:
    return airports.select(
        F.col("iata").alias(join_key),
        F.col("airport_name").alias(f"{prefix}_airport_name"),
        F.col("city").alias(f"{prefix}_city"),
        F.col("state").alias(f"{prefix}_state"),
        F.lit(True).alias(f"{prefix}_in_airport_dim"),
    )


def clean_flights(
    raw: DataFrame, airports: DataFrame, settings: Settings
) -> tuple[DataFrame, DataFrame]:
    """Return (silver flights, quarantined flights) from raw bronze flights."""
    typed = raw.select(
        F.col("date").cast("timestamp").alias("departure_ts"),
        F.col("delay").cast("int").alias("delay_minutes"),
        F.col("distance").cast("int").alias("distance_miles"),
        F.upper(F.trim(F.col("origin"))).alias("origin"),
        F.upper(F.trim(F.col("destination"))).alias("destination"),
        F.col("_source_file"),
        F.col("_ingested_at"),
        F.col("_batch_id"),
    )

    valid, quarantined = apply_rules(typed, flight_rules(settings))

    derived = valid.withColumns(
        {
            "flight_date": F.to_date("departure_ts"),
            "flight_month": F.date_format("departure_ts", "yyyy-MM"),
            "departure_hour": F.hour("departure_ts"),
            "is_delayed": F.col("delay_minutes") >= settings.delayed_threshold_minutes,
        }
    )

    # Airports are small, so broadcast them instead of shuffling 3M flight rows.
    enriched = derived.join(
        F.broadcast(_airport_lookup(airports, "origin", "origin")), "origin", "left"
    ).join(F.broadcast(_airport_lookup(airports, "dest", "destination")), "destination", "left")

    silver = enriched.select(
        "departure_ts",
        "flight_date",
        "flight_month",
        "departure_hour",
        "origin",
        "origin_airport_name",
        "origin_city",
        "origin_state",
        F.coalesce("origin_in_airport_dim", F.lit(False)).alias("origin_in_airport_dim"),
        "destination",
        "dest_airport_name",
        "dest_city",
        "dest_state",
        F.coalesce("dest_in_airport_dim", F.lit(False)).alias("dest_in_airport_dim"),
        "distance_miles",
        "delay_minutes",
        "is_delayed",
        "_source_file",
        "_ingested_at",
        "_batch_id",
    )
    return silver, quarantined


def build_silver(spark: SparkSession, settings: Settings) -> dict[str, object]:
    """Build silver tables from bronze. Returns metrics, raising if a quality gate fails."""
    fmt = settings.table_format

    bronze_flights = read_table(spark, settings.table_path("bronze", "flights"), fmt)
    bronze_airports = read_table(spark, settings.table_path("bronze", "airports"), fmt)

    airports = clean_airports(bronze_airports)
    silver, quarantined = clean_flights(bronze_flights, airports, settings)

    silver_path = settings.table_path("silver", "flights")
    quarantine_path = settings.table_path("silver", "flights_quarantine")
    airports_path = settings.table_path("silver", "airports")

    write_table(airports, airports_path, fmt)
    write_table(silver, silver_path, fmt, partition_by=["flight_month"])
    write_table(quarantined, quarantine_path, fmt)

    source_rows = bronze_flights.count()
    silver_rows = read_table(spark, silver_path, fmt).count()
    quarantine_df = read_table(spark, quarantine_path, fmt)
    quarantine_rows = quarantine_df.count()
    by_rule = failure_counts(quarantine_df) if quarantine_rows else {}

    check_reconciliation(
        source_rows,
        silver_rows,
        quarantine_rows,
        max_quarantine_ratio=settings.max_quarantine_ratio,
    )

    metrics = {
        "bronze_rows": source_rows,
        "silver_rows": silver_rows,
        "quarantined_rows": quarantine_rows,
        "quarantine_ratio": round(quarantine_rows / source_rows, 6) if source_rows else 0.0,
        "quarantined_by_rule": by_rule,
        "airports": read_table(spark, airports_path, fmt).count(),
    }
    log.info("silver written: %s", metrics)
    return metrics
