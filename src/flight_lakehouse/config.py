"""Pipeline settings. Everything that differs between laptop and cloud lives here."""

from __future__ import annotations

import os
from dataclasses import dataclass

SOURCE_BASE_URL = "https://raw.githubusercontent.com/vega/vega-datasets/main/data"
FLIGHTS_FILE = "flights-3m.parquet"
AIRPORTS_FILE = "airports.csv"

SUPPORTED_FORMATS = ("delta", "parquet")


@dataclass(frozen=True)
class Settings:
    """Runtime configuration.

    ``root`` and ``landing`` accept local paths or any Spark-readable URI
    (``s3://bucket/prefix`` on Databricks), so the same code runs everywhere.
    """

    root: str = "data/lake"
    landing: str = "data/landing"
    table_format: str = "delta"
    flights_url: str = f"{SOURCE_BASE_URL}/{FLIGHTS_FILE}"
    airports_url: str = f"{SOURCE_BASE_URL}/{AIRPORTS_FILE}"

    # Data quality thresholds
    min_delay_minutes: int = -120
    max_delay_minutes: int = 1440
    max_quarantine_ratio: float = 0.01

    # FAA convention: a flight is "delayed" when it is 15+ minutes late.
    delayed_threshold_minutes: int = 15

    shuffle_partitions: int = 8

    def __post_init__(self) -> None:
        if self.table_format not in SUPPORTED_FORMATS:
            raise ValueError(
                f"table_format must be one of {SUPPORTED_FORMATS}, got {self.table_format!r}"
            )

    def table_path(self, layer: str, name: str) -> str:
        return f"{self.root.rstrip('/')}/{layer}/{name}"

    def landing_path(self, filename: str) -> str:
        return f"{self.landing.rstrip('/')}/{filename}"

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from environment variables, falling back to defaults."""
        defaults = cls()
        return cls(
            root=os.getenv("LAKEHOUSE_ROOT", defaults.root),
            landing=os.getenv("LAKEHOUSE_LANDING", defaults.landing),
            table_format=os.getenv("TABLE_FORMAT", defaults.table_format).lower(),
            flights_url=os.getenv("FLIGHTS_URL", defaults.flights_url),
            airports_url=os.getenv("AIRPORTS_URL", defaults.airports_url),
            min_delay_minutes=int(os.getenv("MIN_DELAY_MINUTES", defaults.min_delay_minutes)),
            max_delay_minutes=int(os.getenv("MAX_DELAY_MINUTES", defaults.max_delay_minutes)),
            max_quarantine_ratio=float(
                os.getenv("MAX_QUARANTINE_RATIO", defaults.max_quarantine_ratio)
            ),
            shuffle_partitions=int(os.getenv("SHUFFLE_PARTITIONS", defaults.shuffle_partitions)),
        )
