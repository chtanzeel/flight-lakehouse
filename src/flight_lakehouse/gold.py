"""Gold layer: business-ready aggregates built from silver."""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from flight_lakehouse.config import Settings
from flight_lakehouse.tables import read_table, write_table

log = logging.getLogger(__name__)


def _delayed() -> Column:
    """1 for a delayed flight, 0 otherwise, so averaging it gives the delayed share."""
    return F.col("is_delayed").cast("int")


def route_performance_monthly(silver: DataFrame) -> DataFrame:
    """Delay statistics per route and month."""
    return silver.groupBy("flight_month", "origin", "destination").agg(
        F.count("*").alias("flights"),
        F.round(F.avg("delay_minutes"), 2).alias("avg_delay_minutes"),
        F.percentile_approx("delay_minutes", 0.5).alias("median_delay_minutes"),
        F.percentile_approx("delay_minutes", 0.9).alias("p90_delay_minutes"),
        F.round(F.avg(_delayed()), 4).alias("delayed_share"),
        F.round(F.avg("distance_miles"), 1).alias("avg_distance_miles"),
    )


def airport_daily(silver: DataFrame) -> DataFrame:
    """Daily departures per origin airport with a 7-day rolling average delay.

    The rolling average is computed over calendar days (a range window on the day number), so
    days with no departures don't pull in older data, and it is weighted by flight count
    (total delay / total flights) rather than averaging daily averages.
    """
    daily = silver.groupBy("flight_date", "origin").agg(
        F.count("*").alias("flights"),
        F.sum("delay_minutes").alias("total_delay_minutes"),
        F.round(F.avg("delay_minutes"), 2).alias("avg_delay_minutes"),
        F.round(F.avg(_delayed()), 4).alias("delayed_share"),
    )
    day_number = F.datediff(F.col("flight_date"), F.lit("1970-01-01").cast("date"))
    last_7_days = Window.partitionBy("origin").orderBy(day_number).rangeBetween(-6, 0)
    return daily.withColumn(
        "rolling_7d_avg_delay_minutes",
        F.round(
            F.sum("total_delay_minutes").over(last_7_days) / F.sum("flights").over(last_7_days), 2
        ),
    )


def hourly_delay_profile(silver: DataFrame) -> DataFrame:
    """How delays build up through the day (hour of the flight timestamp)."""
    return (
        silver.groupBy("departure_hour")
        .agg(
            F.count("*").alias("flights"),
            F.round(F.avg("delay_minutes"), 2).alias("avg_delay_minutes"),
            F.percentile_approx("delay_minutes", 0.5).alias("median_delay_minutes"),
            F.percentile_approx("delay_minutes", 0.9).alias("p90_delay_minutes"),
            F.round(F.avg(_delayed()), 4).alias("delayed_share"),
        )
        .orderBy("departure_hour")
    )


def origin_state_monthly(silver: DataFrame) -> DataFrame:
    """Monthly departures and delays by origin state (uses the airport dimension)."""
    return silver.groupBy(
        F.coalesce("origin_state", F.lit("UNKNOWN")).alias("origin_state"), "flight_month"
    ).agg(
        F.count("*").alias("flights"),
        F.round(F.avg("delay_minutes"), 2).alias("avg_delay_minutes"),
        F.round(F.avg(_delayed()), 4).alias("delayed_share"),
        F.countDistinct("origin").alias("airports"),
    )


def build_gold(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Build every gold table from silver. Returns row counts per table."""
    fmt = settings.table_format
    silver = read_table(spark, settings.table_path("silver", "flights"), fmt)

    tables = {
        "route_performance_monthly": route_performance_monthly(silver),
        "airport_daily": airport_daily(silver),
        "hourly_delay_profile": hourly_delay_profile(silver),
        "origin_state_monthly": origin_state_monthly(silver),
    }

    counts: dict[str, int] = {}
    for name, df in tables.items():
        path = settings.table_path("gold", name)
        write_table(df, path, fmt)
        counts[name] = read_table(spark, path, fmt).count()
    log.info("gold written: %s", counts)
    return counts
