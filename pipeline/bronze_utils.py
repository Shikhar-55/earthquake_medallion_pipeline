"""
bronze_utils.py
Functions used only by the Bronze layer.
"""

from datetime import timedelta

import requests

import config
import utils

log = utils.get_logger("bronze")


def get_start_time(fs):
    """Where the last successful run stopped. On the very first run there is
    no watermark, so look back a few hours instead."""
    start = utils.read_watermark(fs, config.BRONZE_WATERMARK)
    if start is None:
        start = utils.now_utc() - timedelta(hours=config.FIRST_RUN_LOOKBACK_HOURS)
        log.info("First run, no watermark. Looking back %d hours.", config.FIRST_RUN_LOOKBACK_HOURS)
    log.info("Start time: %s", start)
    return start


def fetch_events(start):
    """Ask USGS for every earthquake added OR changed since 'start'.
    'updatedafter' is what makes revisions of old events show up too."""
    params = {
        "format": "geojson",
        "updatedafter": utils.to_text(start),
        "orderby": "time",
    }
    response = requests.get(config.USGS_URL, params=params, timeout=60)
    response.raise_for_status()                 # non-200 -> exception -> Airflow retry
    events = response.json()["features"]
    log.info("USGS returned %d events", len(events))
    return events


def write_events(fs, events, run_ts):
    """Write the events, untouched except for a load_ts stamp, into this run's
    partition folder:  bronze/usgs/load_date=YYYY-MM-DD/load_hr=HH/events_HHMMSS.jsonl
    The file name carries the run time so two runs in the same hour never overwrite each other."""
    for event in events:
        event["load_ts"] = utils.to_text(run_ts)
    path = (f"{config.BRONZE_PATH}/load_date={run_ts:%Y-%m-%d}/load_hr={run_ts:%H}"
            f"/events_{run_ts:%H%M%S}.jsonl")
    utils.write_jsonl(fs, path, events)
    log.info("Wrote %d lines to %s", len(events), path)
