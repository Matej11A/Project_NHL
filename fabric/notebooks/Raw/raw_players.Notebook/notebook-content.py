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
from datetime import date
from nhlpy import NHLClient
import notebookutils.mssparkutils as mssparkutils

client = NHLClient()

TEAM_ABBR = "ANA"
seasons  = [
    "20202021",
    "20212022",
    "20222023",
    "20232024",
    "20242025"
]

ingestion_date = date.today().isoformat()

roster_dir = f"/lakehouse/default/Files/raw/players/roster/{ingestion_date}"
os.makedirs(roster_dir, exist_ok=True)

all_player_ids = set()

for season in seasons:
    print(f"Fethicng {TEAM_ABBR} roster for {season}...")
    roster = client.teams.team_roster(team_abbr=TEAM_ABBR, season=season)

    with open(f"{roster_dir}/{season}.json", "w") as f:
        json.dump(roster, f)

    for p in roster['forwards'] + roster['defensemen'] + roster['goalies']:
        all_player_ids.add(p['id'])

print(f"Unique players found across all seasons: {len(all_player_ids)}")


player_stats_dir = f"/lakehouse/default/Files/raw/players/player_stats/{ingestion_date}"
os.makedirs(player_stats_dir, exist_ok=True)

for player_id in all_player_ids:
    print(f"Fetching player {player_id}...")
    raw = client.stats.player_career_stats(player_id=str(player_id))
    with open (f"{player_stats_dir}/{player_id}.json", "w") as f:
        json.dump(raw, f)

print(f"Landed raw JSON for {len(all_player_ids)} players")

mssparkutils.notebook.run(
    "bronze_players",
    600,
    {"ingestion_date": ingestion_date, "team_abbr": TEAM_ABBR}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
