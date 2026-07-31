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
# META     },
# META     "environment": {
# META       "environmentId": "1c9c4c3b-612e-8da1-4689-71c8adda755d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
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

import json
import os
import time
from datetime import date
from nhlpy import NHLClient
import notebookutils.mssparkutils as mssparkutils

client = NHLClient()

SEASON = "20242025"
TEAM_ABBR = "ANA"

if ingestion_date is None:
    ingestion_date = date.today().isoformat()

print(f"1. Fetching {SEASON} roster for {TEAM_ABBR}...")
roster = client.teams.team_roster(team_abbr=TEAM_ABBR, season=SEASON)

roster_dir = f"/lakehouse/default/Files/raw/edge_stats_goalies/roster/{ingestion_date}"
os.makedirs(roster_dir, exist_ok=True)
with open(f"{roster_dir}/{SEASON}.json", "w") as f:
    json.dump(roster, f)

goalies = roster.get("goalies", [])
print(f"   Found {len(goalies)} goalies on the roster.")

edge_dir = f"/lakehouse/default/Files/raw/edge_stats_goalies/player_edge/{SEASON}/{ingestion_date}"
os.makedirs(edge_dir, exist_ok=True)

print("\n2. Pulling live Goalie EDGE profiles...")
fetched_meta = []

for idx, goalie in enumerate(goalies, start=1):
    player_id = goalie.get("id")
    first_name = goalie.get("firstName", {}).get("default", "")
    last_name = goalie.get("lastName", {}).get("default", "")
    player_name = f"{first_name} {last_name}".strip()

    if not player_id:
        continue

    print(f"   [{idx}/{len(goalies)}] Extracting Goalie: {player_name}")

    try:
        raw_edge_data = client.edge.goalie_detail(player_id=int(player_id), season=SEASON)

        if raw_edge_data:
            with open(f"{edge_dir}/{player_id}.json", "w") as f:
                json.dump(raw_edge_data, f)

            fetched_meta.append({
                "player_id": player_id,
                "player_name": player_name,
                "position_code": goalie.get("positionCode", "G"),
            })

    except Exception as player_err:
        print(f"    Could not fetch data for {player_name}: {player_err}")

    time.sleep(0.2)

with open(f"{edge_dir}/_player_meta.json", "w") as f:
    json.dump(fetched_meta, f)

print(f"\nDone - {len(fetched_meta)} goalie EDGE profiles fetched")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
