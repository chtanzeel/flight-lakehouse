"""Pipeline orchestration: run bronze, silver and gold and record what happened."""

from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from flight_lakehouse.bronze import build_bronze
from flight_lakehouse.config import Settings
from flight_lakehouse.gold import build_gold
from flight_lakehouse.silver import build_silver
from flight_lakehouse.spark_session import get_spark
from flight_lakehouse.tables import write_table

log = logging.getLogger(__name__)

STAGES = ("bronze", "silver", "gold")

_RUN_LOG_SCHEMA = StructType(
    [
        StructField("batch_id", StringType(), False),
        StructField("started_at", StringType(), False),
        StructField("finished_at", StringType(), False),
        StructField("status", StringType(), False),
        StructField("metrics_json", StringType(), False),
    ]
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _write_run_log(
    spark: SparkSession,
    settings: Settings,
    *,
    batch_id: str,
    started_at: str,
    status: str,
    metrics: dict,
) -> None:
    """Append one row per run to an ops table, so every run is auditable."""
    row = (batch_id, started_at, _now(), status, json.dumps(metrics, sort_keys=True, default=str))
    df = spark.createDataFrame([row], _RUN_LOG_SCHEMA)
    write_table(df, settings.table_path("ops", "run_log"), settings.table_format, mode="append")


def run_pipeline(
    settings: Settings,
    stages: tuple[str, ...] = STAGES,
    spark: SparkSession | None = None,
) -> dict:
    """Run the requested stages in order and return the run metrics.

    Stages are idempotent full refreshes, so re-running produces the same tables.
    """
    unknown = set(stages) - set(STAGES)
    if unknown:
        raise ValueError(f"unknown stages: {sorted(unknown)}; choose from {STAGES}")

    spark = spark or get_spark(
        table_format=settings.table_format, shuffle_partitions=settings.shuffle_partitions
    )
    batch_id = uuid.uuid4().hex[:12]
    started_at = _now()
    metrics: dict = {"batch_id": batch_id, "table_format": settings.table_format, "stages": {}}
    log.info("run %s starting (stages=%s, format=%s)", batch_id, stages, settings.table_format)

    runners = {
        "bronze": lambda: build_bronze(spark, settings, batch_id),
        "silver": lambda: build_silver(spark, settings),
        "gold": lambda: build_gold(spark, settings),
    }

    status = "SUCCEEDED"
    try:
        for stage in STAGES:
            if stage not in stages:
                continue
            t0 = time.perf_counter()
            result = runners[stage]()
            metrics["stages"][stage] = {
                "seconds": round(time.perf_counter() - t0, 1),
                "result": result,
            }
            log.info("stage %s finished in %.1fs", stage, time.perf_counter() - t0)
    except Exception as error:
        status = "FAILED"
        metrics["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        _write_run_log(
            spark,
            settings,
            batch_id=batch_id,
            started_at=started_at,
            status=status,
            metrics=metrics,
        )

    return metrics


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the flight lakehouse pipeline.")
    parser.add_argument(
        "--stages",
        nargs="+",
        choices=STAGES,
        default=list(STAGES),
        help="stages to run (default: all, in bronze -> silver -> gold order)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    metrics = run_pipeline(Settings.from_env(), tuple(args.stages))
    print(json.dumps(metrics, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
