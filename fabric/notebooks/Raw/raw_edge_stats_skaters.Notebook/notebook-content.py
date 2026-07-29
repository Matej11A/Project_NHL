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

# CELL ********************

%pip install nhl-api-py

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import os
import time
from datetime import date
from nhlpy import NHLClient
import notebookutils.mssparkutils as mssparkutils

client = NHLClient()

SEASON = "20242025"
TEAM_ABBR = "ANA"

ingestion_date = date.today().isoformat()

print(f"1. Fetching {SEASON} roster for {TEAM_ABBR}...")
roster = client.teams.team_roster(team_abbr=TEAM_ABBR, season=SEASON)

roster_dir = f"/lakehouse/default/Files/raw/edge_stats_skaters/roster/{ingestion_date}"
os.makedirs(roster_dir, exist_ok=True)
with open(f"{roster_dir}/{SEASON}.json", "w") as f:
    json.dump(roster, f)

skaters = roster.get("forwards", []) + roster.get("defensemen", [])
print(f"Found {len(skaters)} skaters on the roster.")

edge_dir = f"/lakehouse/default/Files/raw/edge_stats_skaters/player_edge/{SEASON}/{ingestion_date}"
os.makedirs(edge_dir, exist_ok=True)

print("\n2. Pulling live EDGE profiles...")
fetched_meta = []

for idx, skater in enumerate(skaters, start=1):
    player_id = skater.get("id")
    first_name = skater.get("firstName", {}).get("default", "")
    last_name = skater.get("lastName", {}).get("default", "")
    player_name = f"{first_name} {last_name}".strip()

    if not player_id:
        continue
    
    print(f" [{idx}/{len(skaters)}] Extracting: {player_name}")

    try:
        raw_edge_data = client.edge.skater_detail(player_id=player_id, season=SEASON)

        if raw_edge_data:
            with open(f"{edge_dir}/{player_id}.json", "w") as f:
                json.dump(raw_edge_data, f)

            fetched_meta.append({
                "player_id": player_id,
                "player_name": player_name,
                "position_code": skater.get("positionCode")
            })

    except Exception as player_err:
            print(f" Could not fetch data for {player_name}: {player_err}")

    time.sleep(0.2)

with open(f"{edge_dir}/_player_meta.json", "w") as f:
    json.dump(fetched_meta, f)

print(f"\nDone - {len(fetched_meta)} skater EDGE profiles fetched")

mssparkutils.notebook.run(
    "bronze_edge_stats_skaters",
    600,
    {"ingestion_date": ingestion_date, "season": SEASON}
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
