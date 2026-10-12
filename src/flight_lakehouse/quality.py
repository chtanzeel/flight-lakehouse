"""Declarative row-level data quality rules, with a quarantine for rows that fail them."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

FAILED_RULES_COL = "_failed_rules"


class DataQualityError(RuntimeError):
    """Raised when a pipeline-level quality gate fails."""


@dataclass(frozen=True)
class Rule:
    """A named boolean expression that must hold for a row to be considered valid.

    A NULL result counts as a failure, so a missing value can never slip through.
    """

    name: str
    passes: Column


def apply_rules(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    """Split ``df`` into (valid rows, quarantined rows).

    Quarantined rows keep every original column plus ``_failed_rules``: the names of every
    rule the row broke, so a single row can explain all of its problems at once.
    """
    failures = [F.when(~F.coalesce(rule.passes, F.lit(False)), F.lit(rule.name)) for rule in rules]
    checked = df.withColumn(FAILED_RULES_COL, F.array_compact(F.array(*failures)))
    valid = checked.filter(F.size(FAILED_RULES_COL) == 0).drop(FAILED_RULES_COL)
    quarantined = checked.filter(F.size(FAILED_RULES_COL) > 0)
    return valid, quarantined


def failure_counts(quarantined: DataFrame) -> dict[str, int]:
    """How many quarantined rows broke each rule (a row can count toward several)."""
    rows = (
        quarantined.select(F.explode(FAILED_RULES_COL).alias("rule"))
        .groupBy("rule")
        .count()
        .collect()
    )
    return {row["rule"]: row["count"] for row in rows}


def check_reconciliation(
    source_rows: int,
    valid_rows: int,
    quarantined_rows: int,
    *,
    max_quarantine_ratio: float,
) -> None:
    """Fail the run if rows went missing or too many were rejected.

    Every input row must land in exactly one place: silver or quarantine.
    """
    if source_rows != valid_rows + quarantined_rows:
        raise DataQualityError(
            f"row count mismatch: {source_rows} source rows but {valid_rows} valid + "
            f"{quarantined_rows} quarantined = {valid_rows + quarantined_rows}"
        )
    ratio = quarantined_rows / source_rows if source_rows else 0.0
    if ratio > max_quarantine_ratio:
        raise DataQualityError(
            f"{ratio:.2%} of rows were quarantined, above the allowed {max_quarantine_ratio:.2%}"
        )
