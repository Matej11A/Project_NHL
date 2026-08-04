# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7fa7e213-3f48-43a9-9854-240338af99f8",
# META       "default_lakehouse_name": "lh_Main",
# META       "default_lakehouse_workspace_id": "4dde6d65-494a-490e-895a-613e38da7758",
# META       "known_lakehouses": [
# META         {
# META           "id": "7fa7e213-3f48-43a9-9854-240338af99f8"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

ingestion_date = None
draft_years = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import requests
from datetime import date

if ingestion_date is None:
    ingestion_date = date.today().isoformat()

if draft_years is None:
    draft_years = "2026"

years_list = [y.strip() for y in draft_years.split(",")]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_dir = f"/lakehouse/default/Files/raw/draft_picks/{ingestion_date}"
os.makedirs(raw_dir, exist_ok=True)

for year in years_list:
    print(f"Fetching draft picks for {year}...")
    resp = requests.get(f"https://api-web.nhle.com/v1/draft/picks/{year}/all", timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    with open(f"{raw_dir}/{year}.json", "w") as f:
        json.dump(payload, f)

    n_picks = len(payload.get("picks", []))
    print(f" {year}: landed {n_picks} picks")

print(f"Done - landed {len(years_list)} year(s) of draft picks to {raw_dir}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
