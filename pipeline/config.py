"""
config.py
Every setting in one place. Secrets come from environment variables
(copy .env.example to pipeline/.env on your laptop; the DAG sets them on the VM).
"""

import os
from dotenv import load_dotenv

load_dotenv()

### GENERIC CONFIG ###########################################################

# Azure storage account with hierarchical namespace ON, and one of its access keys
STORAGE_ACCOUNT = os.environ["STORAGE_ACCOUNT"]
STORAGE_KEY = os.environ["STORAGE_KEY"]

# Where watermark files live. Format for every path is "container/folder".
WATERMARK_PATH = "meta/watermarks"

# One timestamp format for the whole pipeline, always UTC: 2026-11-01T13:00:08Z
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


### BRONZE CONFIG ############################################################

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
BRONZE_PATH = "bronze/usgs"
BRONZE_WATERMARK = "usgs_to_bronze"
FIRST_RUN_LOOKBACK_HOURS = 2        # no watermark yet? look back this far


### SILVER CONFIG ############################################################

SILVER_PATH = "silver/earthquakes"
SILVER_WATERMARK = "bronze_to_silver"


### GOLD CONFIG ##############################################################

GOLD_PATH = "gold"
GOLD_TABLES = ["events", "hourly_summary", "region_daily", "alerts"]
GOLD_REBUILD_DAYS = 3               # Gold rebuilds the last N days every run
