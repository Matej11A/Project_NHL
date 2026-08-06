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
import time
from datetime import date
from pyspark.sql import functions as F

if ingestion_date is None:
    ingestion_date = date.today().isoformat()
if draft_years is None:
    draft_years = "2026"

years_list = [y.strip() for y in draft_years.split(",")]

draft_picks = (
    spark.read.table("gold.dim_draft_picks")
    .filter(F.col("draft_year").isin(years_list))
    .select("draft_year", "overall_pick", "first_name", "last_name")
    .collect()
)

print(f"Resolving search candidates for {len(draft_picks)} draft picks")

raw_dir = f"/lakehouse/default/Files/raw/prospect_search/{ingestion_date}"
os.makedirs(raw_dir, exist_ok=True)

failures = []

for pick in draft_picks:
    query_name = f"{pick.first_name} {pick.last_name}"
    file_path = f"{raw_dir}/{pick.draft_year}_{pick.overall_pick}.json"

    try:
        resp = requests.get(
            "https://search.d3.nhle.com/api/v1/search/player",
            params={"culture": "en-us", "limit": 20, "q": query_name},
            timeout=10
        )
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as e:
        print(f" FAILED pick #{pick.overall_pick} ({query_name}): {e}")
        failures.append((pick.draft_year, pick.overall_pick, query_name))
        continue
    
    with open(file_path, "w") as f:
        json.dump(payload, f)

    time.sleep(0.1)


print(f"Done - landed {len(draft_picks) - len(failures)} of {len(draft_picks)} picks to {raw_dir}")
if failures:
    print(f"Failed picks (retry these): {failures}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
