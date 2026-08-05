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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
import json
import os
import requests
import time
from datetime import date

if ingestion_date is None:
    ingestion_date = date.today().isoformat()

resolved_players = (
    spark.read.table("silver.bridge_prospect_player")
    .filter(F.col("player_id").isNotNull())
    .select("player_id")
    .distinct()
    .collect()
)

print(f"Fetching landing data for {len(resolved_players)} unique resolved players")

raw_dir = f"/lakehouse/default/Files/raw/prospect_landing/{ingestion_date}"
os.makedirs(raw_dir, exist_ok=True)

failures = []

for row in resolved_players:
    player_id = row.player_id
    file_path = f"{raw_dir}/{player_id}.json"

    try:
        resp = requests.get(f"https://api-web.nhle.com/v1/player/{player_id}/landing", timeout=10)
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as e:
        print(f"  FAILED player {player_id}: {e}")
        failures.append(player_id)
        continue

    with open(file_path, "w") as f:
        json.dump(payload, f)

    time.sleep(0.1)

print(f"Done - landed {len(resolved_players) - len(failures)} of {len(resolved_players)} players to {raw_dir}")
if failures:
    print(f"Failed player_ids (retry these): {failures}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
