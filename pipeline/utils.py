"""
utils.py
Generic helpers used by every layer: logging, lake connection, time,
watermarks, reading and writing files in the lake, and the run wrapper.
Nothing in here knows about earthquakes.
"""

import json
import logging
from datetime import datetime, timezone

import pandas as pd
from adlfs import AzureBlobFileSystem

import config


# ---------- Logging ----------
def get_logger(name):
    """Console logger. Airflow captures console output into its task log UI."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("azure").setLevel(logging.WARNING)   # hide the Azure SDK's HTTP chatter
    return logging.getLogger(name)


# ---------- Lake connection ----------
def get_fs():
    """Filesystem object for the data lake. Paths look like 'bronze/usgs/file.jsonl'."""
    return AzureBlobFileSystem(
        account_name=config.STORAGE_ACCOUNT,
        account_key=config.STORAGE_KEY,
    )


# ---------- Time ----------
def now_utc():
    return datetime.now(timezone.utc)


def to_text(dt):
    """datetime -> '2026-11-01T13:00:08Z'"""
    return dt.strftime(config.TIME_FORMAT)


def from_text(text):
    """'2026-11-01T13:00:08Z' -> datetime (UTC)"""
    return datetime.strptime(text, config.TIME_FORMAT).replace(tzinfo=timezone.utc)


# ---------- Watermarks ----------
def read_watermark(fs, name):
    """Datetime stored in meta/watermarks/<name>.json, or None if the file is missing."""
    path = f"{config.WATERMARK_PATH}/{name}.json"
    if not fs.exists(path):
        return None
    with fs.open(path, "r") as f:
        return from_text(json.load(f)["value"])


def write_watermark(fs, name, value):
    """Save a datetime to meta/watermarks/<name>.json. Call only AFTER data is written."""
    path = f"{config.WATERMARK_PATH}/{name}.json"
    with fs.open(path, "w") as f:
        json.dump({"value": to_text(value)}, f)


# ---------- Reading and writing the lake ----------
def write_jsonl(fs, path, records):
    """Write a list of dicts as JSON Lines (one record per line)."""
    with fs.open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def read_jsonl(fs, path):
    """Read a JSON Lines file into a list of dicts."""
    with fs.open(path, "r") as f:
        return [json.loads(line) for line in f]


def write_parquet(fs, path, df):
    with fs.open(path, "wb") as f:
        df.to_parquet(f, index=False)


def read_parquet(fs, path):
    with fs.open(path, "rb") as f:
        return pd.read_parquet(f)


def read_parquet_folder(fs, pattern):
    """Read every parquet file matching a glob pattern into one DataFrame.
    Returns an empty DataFrame if nothing matches."""
    frames = [read_parquet(fs, p) for p in fs.glob(pattern)]
    return pd.concat(frames) if frames else pd.DataFrame()


def write_csv(fs, path, df):
    with fs.open(path, "w") as f:
        df.to_csv(f, index=False)


# ---------- Run wrapper ----------
def run(main, name):
    """Run a layer's main(). On any error, print the full traceback to the log
    (so it shows in the Airflow log UI) and re-raise so the task is marked failed."""
    log = get_logger(name)
    try:
        main()
    except Exception:
        log.exception("%s FAILED", name.upper())
        raise
