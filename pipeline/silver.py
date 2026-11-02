"""
silver.py  (Layer 2 of 3)

Turn raw Bronze JSON into a clean, typed, deduplicated table partitioned
by the time the earthquake HAPPENED. Touched partitions are overwritten.
"""

import config
import utils
import silver_utils

log = utils.get_logger("silver")


def main():
    fs = utils.get_fs()

    watermark = silver_utils.get_watermark(fs)              # 1. what did we already process?
    raw = silver_utils.read_new_bronze(fs, watermark)       # 2. read only the new Bronze lines
    if raw.empty:
        log.info("No new Bronze data. Silver done.")
        return

    df = silver_utils.flatten(raw)                          # 3. nested JSON -> columns
    df = silver_utils.dedupe(df)                            #    newest version of each event
    df = silver_utils.add_columns(df)                       #    partition keys + derived columns
    log.info("Cleaned to %d unique events", len(df))

    silver_utils.write_partitions(fs, df)                   # 4. overwrite touched partitions

    newest = raw["load_ts"].max().to_pydatetime()
    utils.write_watermark(fs, config.SILVER_WATERMARK, newest)   # 5. only after step 4 worked
    log.info("Watermark moved to %s. Silver done.", newest)


if __name__ == "__main__":
    utils.run(main, "silver")
