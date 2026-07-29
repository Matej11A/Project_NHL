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
team_abbr = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType
import notebookutils.mssparkutils as mssparkutils

if ingestion_date is None:
    ingestion_date = date.today().isoformat()
if team_abbr is None:
    team_abbr = "ANA"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

team_lookup_df = spark.read.table("bronze.dim_nhl_teams").select("abbr", "franchise_id").collect()
team_lookup = {row["abbr"]: row["franchise_id"] for row in team_lookup_df}
team_franchise_id = team_lookup.get(team_abbr, None)

print(f"Franchise ID for {team_abbr}: {team_franchise_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

player_stats_dir = f"/lakehouse/default/Files/raw/players/player_stats/{ingestion_date}"
raw_files = mssparkutils.fs.ls(f"file:{player_stats_dir}")

raw_rows = []
for file_info in raw_files:
    player_id = file_info.name.replace(".json", "")
    local_path = f"{player_stats_dir}/{file_info.name}"
    with open(local_path, "r") as f:
        raw = json.load(f)
    raw_rows.append({
        "player_id": player_id,
        "franchise_id": team_franchise_id,
        "raw_json": json.dumps(raw),
        "_raw_file_path": local_path
    })

print(f"Loaded raw career stats for {len(raw_rows)} players from raw files")

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
    StructField("_raw_file_path", StringType(), True)
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
