"""
silver_utils.py
Functions used only by the Silver layer.
"""

from datetime import datetime, timezone

import pandas as pd

import config
import utils

log = utils.get_logger("silver")


def get_watermark(fs):
    """Last Bronze load_ts that Silver already processed. First run: take everything."""
    watermark = utils.read_watermark(fs, config.SILVER_WATERMARK)
    if watermark is None:
        watermark = datetime(2000, 1, 1, tzinfo=timezone.utc)
    log.info("Processing Bronze lines with load_ts > %s", watermark)
    return watermark


def read_new_bronze(fs, watermark):
    """Read only the Bronze lines newer than the watermark.
    Two filters: skip whole partition folders that are too old (partition
    pruning), then keep only rows whose load_ts is newer than the watermark."""
    watermark_hour = watermark.replace(minute=0, second=0)
    rows = []
    for path in fs.glob(f"{config.BRONZE_PATH}/load_date=*/load_hr=*/events.jsonl"):
        date_part = path.split("load_date=")[1][:10]            # 2026-11-01
        hr_part = path.split("load_hr=")[1][:2]                 # 13
        folder_time = datetime.strptime(f"{date_part}T{hr_part}", "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
        if folder_time < watermark_hour:
            continue                                             # too old, skip the whole folder
        rows.extend(utils.read_jsonl(fs, path))

    raw = pd.DataFrame(rows)
    if raw.empty:
        return raw
    raw["load_ts"] = pd.to_datetime(raw["load_ts"], utc=True)
    raw = raw[raw["load_ts"] > watermark].reset_index(drop=True)   # reset so flatten() lines up
    log.info("Read %d new Bronze lines", len(raw))
    return raw


def flatten(raw):
    """Nested GeoJSON -> flat columns, correct types."""
    props = pd.json_normalize(raw["properties"].tolist())
    coords = pd.DataFrame(raw["geometry"].apply(lambda g: g["coordinates"]).tolist(),
                          columns=["longitude", "latitude", "depth_km"])
    df = pd.concat([raw[["id", "load_ts"]], props, coords], axis=1)   # all three share index 0..n-1

    df["event_ts"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    df["updated_ts"] = pd.to_datetime(df["updated"], unit="ms", utc=True)
    df = df.drop(columns=["time", "updated", "detail"], errors="ignore")
    df = df.rename(columns={"magType": "mag_type"})
    return df


def dedupe(df):
    """Keep only the newest version of each earthquake."""
    return df.sort_values("updated_ts").drop_duplicates("id", keep="last")


def mag_class(m):
    if pd.isna(m):   return "unknown"
    if m < 2:        return "micro"
    if m < 4:        return "minor"
    if m < 5:        return "light"
    if m < 6:        return "moderate"
    if m < 7:        return "strong"
    if m < 8:        return "major"
    return "great"


def depth_class(d):
    if pd.isna(d):   return "unknown"
    if d < 70:       return "shallow"
    if d < 300:      return "intermediate"
    return "deep"


def add_columns(df):
    """Partition keys plus the derived columns the dashboard needs."""
    # Partition keys come from the EVENT time, not the load time
    df["event_date"] = df["event_ts"].dt.strftime("%Y-%m-%d")
    df["event_hr"] = df["event_ts"].dt.strftime("%H")

    df["event_dow"] = df["event_ts"].dt.day_name()
    df["mag_class"] = df["mag"].apply(mag_class)
    df["depth_class"] = df["depth_km"].apply(depth_class)
    df["is_reviewed"] = df["status"] == "reviewed"
    df["is_felt"] = df["felt"].fillna(0) > 0
    df["is_significant"] = df["sig"].fillna(0) >= 600
    df["hours_to_update"] = (df["updated_ts"] - df["event_ts"]).dt.total_seconds() / 3600

    # "12 km NE of Ridgecrest, CA"  ->  12 | NE | Ridgecrest, CA | CA
    parts = df["place"].fillna("").str.extract(r"^(?:(\d+) km ([NSEW]{1,3}) of )?(.*)$")
    df["distance_km"] = pd.to_numeric(parts[0], errors="coerce")
    df["direction"] = parts[1]
    df["nearest_place"] = parts[2]
    df["region"] = df["nearest_place"].str.split(",").str[-1].str.strip()
    return df


def write_partitions(fs, df):
    """For every event_date/event_hr in df: merge with the rows already in that
    Silver partition, keep the newest version of each event, OVERWRITE the file."""
    for (event_date, event_hr), new_part in df.groupby(["event_date", "event_hr"]):
        path = f"{config.SILVER_PATH}/event_date={event_date}/event_hr={event_hr}/part-0.parquet"
        if fs.exists(path):
            merged = dedupe(pd.concat([utils.read_parquet(fs, path), new_part]))
        else:
            merged = new_part
        utils.write_parquet(fs, path, merged)
        log.info("Wrote %d rows to %s", len(merged), path)
