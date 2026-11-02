"""
gold.py  (Layer 3 of 3)

Build the tables Tableau reads, partitioned by event_date.

Gold has no watermark. USGS keeps revising earthquakes for a few days, so
every run simply REBUILDS the last GOLD_REBUILD_DAYS days from Silver and
overwrites those day partitions. Older days never change.
"""

import utils
import gold_utils

log = utils.get_logger("gold")


def main():
    fs = utils.get_fs()

    days = gold_utils.days_to_rebuild()                # 1. which days to rebuild
    silver = gold_utils.read_silver(fs, days)          # 2. read those days from Silver
    if silver.empty:
        log.info("No Silver data for these days. Gold done.")
        return

    for day, s in silver.groupby("event_date"):        # 3. build + overwrite each day
        gold_utils.write_day(fs, day, s)

    gold_utils.export_csvs(fs)                         # 4. flat CSVs for Tableau
    log.info("Gold done.")


if __name__ == "__main__":
    utils.run(main, "gold")
