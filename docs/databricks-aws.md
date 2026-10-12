# Running on Databricks and AWS

The pipeline has no Databricks-specific code paths beyond one check: if `DATABRICKS_RUNTIME_VERSION`
is set it reuses the cluster's Spark session (Delta is already built in) instead of creating a local
one. Everything else is driven by two settings, `root` and `landing`, which accept any path Spark can
read.

> This guide is a deployment recipe. The pipeline itself is tested locally and in CI; the Databricks
> and S3 steps below have not been run for this repo, so expect to adjust names and permissions to
> your workspace.

## 1. Storage on S3

Create a bucket (or a prefix in an existing one) with two folders:

```
s3://<bucket>/flight-lakehouse/landing/   # source files
s3://<bucket>/flight-lakehouse/lake/      # bronze / silver / gold / ops tables
```

Give the cluster an instance profile (or Unity Catalog external location) with read/write access to
that prefix. Prefer an IAM role over access keys.

## 2. Put the source files in the landing folder

Either download them on your laptop and upload with the AWS CLI:

```bash
make ingest
aws s3 cp data/landing/ s3://<bucket>/flight-lakehouse/landing/ --recursive
```

or, on a Databricks volume, run the ingest step from a notebook, because `ingest` writes to any
path that behaves like a local directory, including `/Volumes/<catalog>/<schema>/<volume>/landing`:

```python
from flight_lakehouse.config import Settings
from flight_lakehouse.ingest import ingest

ingest(Settings(landing="/Volumes/main/default/lakehouse/landing"))
```

## 3. Get the code onto the workspace

Add this repository under **Workspace → Repos** (Git folders), then install it on the cluster:

```python
%pip install /Workspace/Repos/<you>/flight-lakehouse
```

## 4. Run the notebook

Open `databricks/run_pipeline.py`, set the `root`, `landing` and `table_format` widgets, and run it.
Use a Databricks Runtime built on Spark 4 to match the versions the tests use. A small cluster is
plenty for 3M rows.

## 5. Schedule it

Create a **Workflow (Job)** with one notebook task pointing at `databricks/run_pipeline.py`, pass the
three widgets as parameters, and add a schedule. Every run appends a row to `ops/run_log`, including
failed runs, so you can alert on `status = 'FAILED'`.

## What changes at larger scale

- Replace the full refresh in `bronze` with Auto Loader and an incremental `MERGE` into silver.
- Move `quality.py` rules into Delta Live Tables expectations or keep them as they are; the
  quarantine pattern carries over unchanged.
- Partition silver by month as it is now, then add `OPTIMIZE ... ZORDER BY (origin)` for point lookups.
