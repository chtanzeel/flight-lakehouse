# Databricks notebook source
# MAGIC %md
# MAGIC # Flight lakehouse pipeline
# MAGIC Runs bronze -> silver -> gold. Install the package on the cluster first
# MAGIC (`%pip install /Workspace/Repos/<you>/flight-lakehouse`), or attach the repo as a library.

# COMMAND ----------

dbutils.widgets.text("root", "s3://YOUR-BUCKET/flight-lakehouse/lake", "Lake root")
dbutils.widgets.text("landing", "s3://YOUR-BUCKET/flight-lakehouse/landing", "Landing folder")
dbutils.widgets.dropdown("table_format", "delta", ["delta", "parquet"], "Table format")

# COMMAND ----------

from flight_lakehouse.config import Settings
from flight_lakehouse.pipeline import run_pipeline

settings = Settings(
    root=dbutils.widgets.get("root"),
    landing=dbutils.widgets.get("landing"),
    table_format=dbutils.widgets.get("table_format"),
)

# On Databricks, get_spark() reuses the cluster's session and Delta is built in.
metrics = run_pipeline(settings)

# COMMAND ----------

import json

print(json.dumps(metrics, indent=2, default=str))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Peek at a gold table

# COMMAND ----------

display(spark.read.format(settings.table_format).load(settings.table_path("gold", "hourly_delay_profile")))
