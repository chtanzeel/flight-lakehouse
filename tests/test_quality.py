import pytest
from pyspark.sql import functions as F

from flight_lakehouse.quality import (
    FAILED_RULES_COL,
    DataQualityError,
    Rule,
    apply_rules,
    check_reconciliation,
    failure_counts,
)


def _frame(spark):
    return spark.createDataFrame(
        [(1, "a"), (-1, "b"), (2, None), (None, "d"), (-3, None)],
        "x int, y string",
    )


def _rules():
    return [
        Rule("x_positive", F.col("x") > 0),
        Rule("y_present", F.col("y").isNotNull()),
    ]


def test_apply_rules_splits_valid_and_quarantined(spark):
    valid, quarantined = apply_rules(_frame(spark), _rules())

    assert [r.x for r in valid.collect()] == [1]
    assert quarantined.count() == 4
    assert FAILED_RULES_COL in quarantined.columns
    assert FAILED_RULES_COL not in valid.columns


def test_null_counts_as_failure(spark):
    _, quarantined = apply_rules(_frame(spark), _rules())

    failed = {r.y: r[FAILED_RULES_COL] for r in quarantined.collect() if r.x is None}
    assert failed == {"d": ["x_positive"]}


def test_row_lists_every_rule_it_broke(spark):
    _, quarantined = apply_rules(_frame(spark), _rules())

    both = quarantined.filter(F.col("x") == -3).collect()[0][FAILED_RULES_COL]
    assert sorted(both) == ["x_positive", "y_present"]


def test_failure_counts(spark):
    _, quarantined = apply_rules(_frame(spark), _rules())

    # x_positive: -1, null, -3 ; y_present: 2/None, -3/None
    assert failure_counts(quarantined) == {"x_positive": 3, "y_present": 2}


def test_reconciliation_passes_when_rows_add_up():
    check_reconciliation(100, 99, 1, max_quarantine_ratio=0.05)


def test_reconciliation_fails_when_rows_go_missing():
    with pytest.raises(DataQualityError, match="row count mismatch"):
        check_reconciliation(100, 90, 5, max_quarantine_ratio=0.5)


def test_reconciliation_fails_when_too_many_rows_rejected():
    with pytest.raises(DataQualityError, match="quarantined"):
        check_reconciliation(100, 90, 10, max_quarantine_ratio=0.05)
