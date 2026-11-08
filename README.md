# earthquake_medallion_pipeline

A Medallion (Bronze -> Silver -> Gold) data pipeline in plain Python, reading the
USGS Earthquake API into an Azure Data Lake (ADLS Gen2), ending in a Tableau dashboard.
Built for DATA 5330 / 6330 at Utah State University.

## What each layer does

**Bronze** (`bronze.py`)
- Reads: USGS API, everything added or changed since the last run
- Writes: `bronze/usgs/load_date=YYYY-MM-DD/load_hr=HH/events_HHMMSS.jsonl` (raw JSON, one event per line)
- Partitioned by: load time
- Strategy: append only, watermark `usgs_to_bronze`

**Silver** (`silver.py`)
- Reads: new Bronze partitions
- Writes: `silver/earthquakes/event_date=YYYY-MM-DD/event_hr=HH/part-0.parquet` (clean, typed, deduped)
- Partitioned by: event time
- Strategy: overwrite touched partitions, watermark `bronze_to_silver`

**Gold** (`gold.py`)
- Reads: Silver, last 3 days
- Writes: `gold/<table>/event_date=YYYY-MM-DD/part-0.parquet` plus `gold/tableau/<table>.csv`
- Partitioned by: event date
- Strategy: rebuild the last 3 days every run, no watermark

Gold tables: `events`, `hourly_summary`, `region_daily`, `alerts`.

## Files

```
pipeline/
  config.py          all settings, split into GENERIC / BRONZE / SILVER / GOLD sections
  utils.py           generic helpers: logger, lake connection, watermarks, read/write files, run wrapper
  bronze_utils.py    functions used only by Bronze
  silver_utils.py    functions used only by Silver
  gold_utils.py      functions used only by Gold
  bronze.py          entry point, step by step
  silver.py          entry point, step by step
  gold.py            entry point, step by step
requirements.txt
.env.example         template for the two secrets
```

## Run it on your laptop

1. In the Azure portal create a Storage Account with **hierarchical namespace enabled**
   and four containers: `bronze`, `silver`, `gold`, `meta`.
2. Copy `.env.example` to `pipeline/.env` and fill in the account name and an access key.
3. Install and run:

```
pip install -r requirements.txt
cd pipeline
python bronze.py
python silver.py
python gold.py
```

Run `bronze.py` twice a minute apart. The second run should say "Watermark found" and
return far fewer events: that is the watermark doing its job.

## Run it on Airflow

Each layer has its own DAG in the `airflow-dags` repo. Each DAG creates a VM, runs
`pip install -r requirements.txt && cd pipeline && python <layer>.py`, deletes the VM,
and triggers the next layer's DAG. The VM gets `STORAGE_ACCOUNT` and `STORAGE_KEY`
as environment variables from the DAG.

## How failures are handled

- Every entry point ends with `utils.run(main, "<layer>")`. On any error it prints the
  full traceback to the console (which Airflow shows in the task log UI) and exits
  non-zero, so Airflow marks the task failed and retries it.
- A watermark is only written **after** the data write succeeds. A failed run therefore
  re-reads exactly the same window on retry, and a missed run is backfilled by the next one.
