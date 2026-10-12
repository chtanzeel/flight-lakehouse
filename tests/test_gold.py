from datetime import date

import pytest

from flight_lakehouse.gold import (
    airport_daily,
    hourly_delay_profile,
    origin_state_monthly,
    route_performance_monthly,
)

SILVER_SCHEMA = (
    "flight_month string, flight_date date, departure_hour int, origin string, "
    "destination string, origin_state string, delay_minutes int, distance_miles int, "
    "is_delayed boolean"
)


def _silver(spark, rows):
    """Rows are (month, date, hour, origin, dest, state, delay, distance); is_delayed is derived."""
    full = [(*r, r[6] >= 15) for r in rows]
    return spark.createDataFrame(full, SILVER_SCHEMA)


def test_route_performance_monthly(spark):
    rows = [("2001-01", date(2001, 1, 1), 8, "A", "B", "CA", d, 100) for d in (10, 20, 30, 40)] + [
        ("2001-02", date(2001, 2, 1), 8, "A", "B", "CA", 0, 100)
    ]

    result = {r.flight_month: r for r in route_performance_monthly(_silver(spark, rows)).collect()}

    jan = result["2001-01"]
    assert jan.flights == 4
    assert jan.avg_delay_minutes == 25.0
    assert jan.delayed_share == 0.75  # 20, 30 and 40 minutes are >= 15
    assert jan.avg_distance_miles == 100.0
    assert 20 <= jan.median_delay_minutes <= 30
    assert 30 <= jan.p90_delay_minutes <= 40
    assert result["2001-02"].flights == 1


def test_airport_daily_rolling_average_is_flight_weighted_over_calendar_days(spark):
    rows = [
        ("2001-01", date(2001, 1, 1), 8, "ATL", "B", "GA", 10, 100),
        ("2001-01", date(2001, 1, 1), 9, "ATL", "B", "GA", 30, 100),
        ("2001-01", date(2001, 1, 2), 8, "ATL", "B", "GA", 20, 100),
        ("2001-01", date(2001, 1, 10), 8, "ATL", "B", "GA", 60, 100),
        ("2001-01", date(2001, 1, 1), 8, "JFK", "B", "NY", 100, 100),
    ]

    result = {(r.origin, r.flight_date): r for r in airport_daily(_silver(spark, rows)).collect()}

    day1 = result[("ATL", date(2001, 1, 1))]
    assert day1.flights == 2
    assert day1.total_delay_minutes == 40
    assert day1.avg_delay_minutes == 20.0
    assert day1.rolling_7d_avg_delay_minutes == 20.0

    # (40 + 20) / (2 + 1) flights: weighted, not the mean of 20.0 and 20.0 daily averages
    assert result[("ATL", date(2001, 1, 2))].rolling_7d_avg_delay_minutes == 20.0

    # Jan 10 is 9 days after Jan 1, so the 7-day window only contains Jan 10 itself
    assert result[("ATL", date(2001, 1, 10))].rolling_7d_avg_delay_minutes == 60.0

    # Windows never mix airports
    assert result[("JFK", date(2001, 1, 1))].rolling_7d_avg_delay_minutes == 100.0


def test_rolling_average_weights_by_flights(spark):
    rows = [
        # day 1: 3 flights averaging 10 -> 30 minutes in total
        *[("2001-01", date(2001, 1, 1), 8, "ATL", "B", "GA", 10, 100) for _ in range(3)],
        # day 2: 1 flight of 50 minutes
        ("2001-01", date(2001, 1, 2), 8, "ATL", "B", "GA", 50, 100),
    ]

    result = {r.flight_date: r for r in airport_daily(_silver(spark, rows)).collect()}

    # weighted: (30 + 50) / 4 = 20.0 ; the unweighted mean of daily averages would be 30.0
    assert result[date(2001, 1, 2)].rolling_7d_avg_delay_minutes == pytest.approx(20.0)


def test_hourly_delay_profile_is_ordered_by_hour(spark):
    rows = [
        ("2001-01", date(2001, 1, 1), 7, "A", "B", "CA", 30, 100),
        ("2001-01", date(2001, 1, 1), 6, "A", "B", "CA", 0, 100),
        ("2001-01", date(2001, 1, 1), 6, "A", "B", "CA", 10, 100),
    ]

    result = hourly_delay_profile(_silver(spark, rows)).collect()

    assert [r.departure_hour for r in result] == [6, 7]
    assert [r.avg_delay_minutes for r in result] == [5.0, 30.0]
    assert [r.flights for r in result] == [2, 1]


def test_origin_state_monthly_buckets_unknown_states(spark):
    rows = [
        ("2001-01", date(2001, 1, 1), 8, "LAX", "B", "CA", 10, 100),
        ("2001-01", date(2001, 1, 1), 9, "SFO", "B", "CA", 30, 100),
        ("2001-01", date(2001, 1, 1), 9, "XXX", "B", None, 5, 100),
    ]

    result = {r.origin_state: r for r in origin_state_monthly(_silver(spark, rows)).collect()}

    assert set(result) == {"CA", "UNKNOWN"}
    assert result["CA"].flights == 2
    assert result["CA"].airports == 2
    assert result["CA"].avg_delay_minutes == 20.0
    assert result["UNKNOWN"].flights == 1
