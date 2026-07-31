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
from datetime import date
from nhlpy import NHLClient
import notebookutils.mssparkutils as mssparkutils

client = NHLClient()

if ingestion_date is None:
    ingestion_date = date.today().isoformat()

TEAM_ABBR = "ANA"
SEASON = "20242025"

schedule = client.schedule.team_season_schedule(team_abbr=TEAM_ABBR, season=SEASON)

raw_path = f"/lakehouse/default/Files/raw/games/{SEASON}/{ingestion_date}/schedule.json"

os.makedirs(os.path.dirname(raw_path), exist_ok=True)
with open(raw_path, "w") as f:
    json.dump(schedule, f)

print(f"Wrote raw schedule JSON to {raw_path} ({len(schedule.get('games', []))} games, full season, unfiltered)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
