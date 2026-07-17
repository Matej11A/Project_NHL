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

# MARKDOWN ********************

# # notebook to pull raw data for Anaheim players at bronze layer

# CELL ********************

%pip install nhl-api-py

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from nhlpy import NHLClient
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

client = NHLClient()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

team_lookup_df = spark.read.table("bronze.dim_nhl_teams").select("abbr", "franchise_id").collect()
team_lookup = {row["abbr"]: row["franchise_id"] for row in team_lookup_df}

print(f"Team lookup built - {len(team_lookup)} teams")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

seasons = [
    "20202021",
    "20212022",
    "20222023",
    "20232024",
    "20242025"
]

TEAM_ABBR = "ANA"
TEAM_FRANCHISE_ID = team_lookup.get(TEAM_ABBR, None)
print(f"Franchise ID for {TEAM_ABBR}: {TEAM_FRANCHISE_ID}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_player_ids = set()

for season in seasons:
    print(f"Fetching {TEAM_ABBR} roster for {season}...")
    roster = client.teams.team_roster(team_abbr=TEAM_ABBR, season=season)
    for p in roster['forwards'] + roster['defensemen'] + roster['goalies']:
        all_player_ids.add(p['id'])

print(f"\nUnique players found across all season: {len(all_player_ids)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_rows = []

for player_id in all_player_ids:
    print(f"Fetching player {player_id}...")
    raw = client.stats.player_career_stats(player_id=str(player_id))
    raw_rows.append({
        "player_id": player_id,
        "franchise_id": TEAM_FRANCHISE_ID,
        "raw_json": json.dumps(raw)
    })

print(f"\nDone - {len(raw_rows)} players fetched")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_schema = StructType([
    StructField("player_id", StringType(), True),
    StructField("franchise_id", StringType(), True),
    StructField("raw_json", StringType(), True),
])

df_bronze = spark.createDataFrame(raw_rows, schema=bronze_schema)
df_bronze = df_bronze.withColumn("_ingested_at", F.current_timestamp())

df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze.dim_players")

print(f"Saved bronze.dim_players table with {df_bronze.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
