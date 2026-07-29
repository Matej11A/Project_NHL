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

# # notebook to pull raw edge stats data for ANA goalies

# PARAMETERS CELL ********************

ingestion_date = None
season = None 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import notebookutils.mssparkutils as mssparkutils

if ingestion_date is None:
    ingestion_date = date.today().isoformat()
if season is None:
    season = "20242025"

edge_dir = f"/lakehouse/default/Files/raw/edge_stats_goalies/player_edge/{season}/{ingestion_date}"

with open(f"{edge_dir}/_player_meta.json", "r") as f:
    player_meta = {row["player_id"]: row for row in json.load(f)}

raw_files = mssparkutils.fs.ls(f"file:{edge_dir}")

raw_edge_rows = []
for file_info in raw_files:
    if file_info.name == "_player_meta.json":
        continue

    player_id = int(file_info.name.replace(".json", ""))
    local_path = f"{edge_dir}/{file_info.name}"

    with open(local_path, "r") as f:
        raw_edge_data = json.load(f)

    meta = player_meta.get(player_id, {})
    raw_edge_rows.append({
        "player_id": player_id,
        "player_name": meta.get("player_name"),
        "position_code": meta.get("position_code", "G"),
        "season": season,
        "raw_json": json.dumps(raw_edge_data),
        "_raw_file_path": local_path,
    })

print(f"Loaded {len(raw_edge_rows)} goalie EDGE profiles from raw files")

bronze_schema = StructType([
    StructField("player_id", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("position_code", StringType(), True),
    StructField("season", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("_raw_file_path", StringType(), True)
])

df_bronze = spark.createDataFrame(raw_edge_rows, schema=bronze_schema)
df_bronze = df_bronze.withColumn("_ingested_at", F.current_timestamp())

df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze.fact_edge_stats_goalies")
print(f"Saved bronze.fact_edge_stats_goalies table with {df_bronze.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
