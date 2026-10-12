"""Thin read/write helpers so the layers don't care whether tables are Delta or Parquet."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession


def write_table(
    df: DataFrame,
    path: str,
    table_format: str,
    *,
    partition_by: Sequence[str] | None = None,
    mode: str = "overwrite",
) -> None:
    writer = df.write.format(table_format).mode(mode)
    if table_format == "delta" and mode == "overwrite":
        # Lets a re-run replace a table whose schema has evolved.
        writer = writer.option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def read_table(spark: SparkSession, path: str, table_format: str) -> DataFrame:
    return spark.read.format(table_format).load(path)
