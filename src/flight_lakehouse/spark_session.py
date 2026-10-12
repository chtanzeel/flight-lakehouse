"""SparkSession factory that works on a laptop, in CI and on Databricks."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(
    app_name: str = "flight-lakehouse",
    table_format: str = "delta",
    shuffle_partitions: int = 8,
) -> SparkSession:
    """Return a SparkSession.

    On Databricks the existing session is reused (Delta is built in). Elsewhere a local
    session is created, and for ``delta`` the Delta Lake jars are resolved through
    ``configure_spark_with_delta_pip``.
    """
    builder = SparkSession.builder.appName(app_name)

    if not on_databricks():
        builder = (
            builder.master("local[*]")
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.ui.enabled", "false")
        )

    if table_format == "delta":
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        if not on_databricks():
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)

    spark = builder.getOrCreate()
    # Timestamps in the source are timezone-naive; pin the session to UTC so hour-of-day and
    # date logic is identical on every machine.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark
