"""
gold_utils.py
Functions used only by the Gold layer.
"""

from datetime import timedelta

import pandas as pd

import config
import utils

log = utils.get_logger("gold")

# Columns kept in the event-level Gold tables (events and alerts)
EVENT_COLUMNS = [
    "id", "event_ts", "event_date", "event_hr", "event_dow", "mag", "mag_type", "mag_class",
    "depth_km", "depth_class", "latitude", "longitude", "place", "nearest_place", "region",
    "distance_km", "direction", "type", "status", "is_reviewed", "felt", "cdi", "mmi",
    "alert", "tsunami", "sig", "is_felt", "is_significant", "nst", "gap", "rms", "dmin",
    "net", "hours_to_update", "url", "load_ts",
]


def days_to_rebuild():
    """Today and the previous GOLD_REBUILD_DAYS-1 days, as 'YYYY-MM-DD' strings."""
    today = utils.now_utc().date()
    days = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(config.GOLD_REBUILD_DAYS)]
    log.info("Rebuilding Gold for %s", days)
    return days


def read_silver(fs, days):
    """Every Silver partition for the given days, as one DataFrame."""
    frames = [utils.read_parquet_folder(fs, f"{config.SILVER_PATH}/event_date={day}/event_hr=*/*.parquet")
              for day in days]
    silver = pd.concat(frames)
    log.info("Read %d Silver rows", len(silver))
    return silver


# ---------- One function per Gold table. Each takes one day of Silver, returns one DataFrame ----------
def build_events(s):
    """Cleaned event-level rows for the map and detail views."""
    return s[EVENT_COLUMNS]


def build_hourly_summary(s):
    """One row per hour."""
    return s.groupby(["event_date", "event_hr"]).agg(
        event_count=("id", "count"),
        max_mag=("mag", "max"),
        avg_mag=("mag", "mean"),
        avg_depth_km=("depth_km", "mean"),
        felt_reports=("felt", "sum"),
        reviewed_count=("is_reviewed", "sum"),
        significant_count=("is_significant", "sum"),
    ).reset_index()


def build_region_daily(s):
    """One row per region per day."""
    return s.groupby(["event_date", "region"]).agg(
        event_count=("id", "count"),
        max_mag=("mag", "max"),
        avg_mag=("mag", "mean"),
        avg_depth_km=("depth_km", "mean"),
        felt_reports=("felt", "sum"),
    ).reset_index()


def build_alerts(s):
    """Events worth a second look: PAGER alert, tsunami flag, or high significance."""
    mask = s["alert"].notna() | (s["tsunami"] == 1) | s["is_significant"]
    return s[mask][EVENT_COLUMNS]


BUILDERS = {
    "events": build_events,
    "hourly_summary": build_hourly_summary,
    "region_daily": build_region_daily,
    "alerts": build_alerts,
}


def write_day(fs, day, s):
    """Build every Gold table for one day and overwrite that day's partition."""
    for table in config.GOLD_TABLES:
        df = BUILDERS[table](s)
        path = f"{config.GOLD_PATH}/{table}/event_date={day}/part-0.parquet"
        utils.write_parquet(fs, path, df)
        log.info("Wrote %d rows to %s", len(df), path)


def export_csvs(fs):
    """Concatenate every day partition of each table into one CSV for Tableau."""
    for table in config.GOLD_TABLES:
        full = utils.read_parquet_folder(fs, f"{config.GOLD_PATH}/{table}/event_date=*/*.parquet")
        path = f"{config.GOLD_PATH}/tableau/{table}.csv"
        utils.write_csv(fs, path, full)
        log.info("Exported %d rows to %s", len(full), path)
