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

import json
from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


if ingestion_date is None:
    ingestion_date =date.today().isoformat()
if season is None:
    season = "20242025"
if team_abbr is None:
    team_abbr = "ANA"
if raw_path is None:
    raw_path = f"/lakehouse/default/Files/raw/games/{SEASON}/{ingestion_date}/schedule.json"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SOURCE_ENTITY = "fact_games"

with open(raw_path, "r") as f:
    schedule = json.load(f)

all_games = schedule.get("games", [])
print(f"Full season schedule loaded from raw file: {len(all_games)} games")

watermark_row = (
    spark.read.table("dbo.control_table")
    .filter(F.col("source_entity") == SOURCE_ENTITY)
    .orderBy(F.col("last_run_timestamp").desc())
    .limit(1)
    .collect()
)

if watermark_row:
    current_watermark = watermark_row[0]["last_watermark_value"]
    print(f"Found existing watermark: {current_watermark}")
else:
    current_watermark = "1900-01-01"
    print("No watermark found - this is the first run, will pull everything")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

new_or_updated_games = [
    game for game in all_games
    if game.get("gameDate", "") > current_watermark
]

print(f"{len(new_or_updated_games)} games are newer then watermark ({current_watermark})")

completed_new_games = [g for g in new_or_updated_games if g.get("gameState") == "OFF"]

print(f"{len(completed_new_games)} of those are completed (gameState == 'OFF')")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_game_rows = [
    {
        "game_id": str(game.get("id")),
        "game_date": game.get("gameDate"),
        "game_state": game.get("gameState"),
        "season": season,
        "team_abbr": team_abbr,
        "raw_json": json.dumps(game)
    }
    for game in new_or_updated_games
]

print(f"Prepared {len(raw_game_rows)} rows for Bronze write")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_schema = StructType([
    StructField("game_id", StringType(), False),
    StructField("game_date", StringType(), True),
    StructField("game_state", StringType(), True),
    StructField("season", StringType(), True),
    StructField("team_abbr", StringType(), True),
    StructField("raw_json", StringType(), True)
])

df_new =spark.createDataFrame(raw_game_rows, schema=bronze_schema)
df_new = (
    df_new
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_raw_file_path", F.lit(raw_path))
)

if not spark.catalog.tableExists("bronze.fact_games"):
    df_new.write.format("delta").saveAsTable("bronze.fact_games")
    print(f"Created bronze.fact_games with {df_new.count()} rows")
else:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    df_new.createOrReplaceTempView("new_games")
    spark.sql("""
        MERGE INTO bronze.fact_games AS target
        USING new_games AS source
        ON target.game_id = source.game_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merged {df_new.count()} rows into bronze.fact_games")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if completed_new_games:
    new_watermark_value = max(g.get("gameDate")for g in completed_new_games)

    watermark_schema = StructType([
        StructField("source_entity", StringType(), False),
        StructField("last_watermark_value", StringType(), True),
        StructField("last_run_timestamp", TimestampType(), True),
        StructField("rows_processed_last_run", IntegerType(), True)
    ])

    watermark_update_row = spark.createDataFrame([{
        "source_entity": SOURCE_ENTITY,
        "last_watermark_value": new_watermark_value,
        "last_run_timestamp": None,
        "rows_processed_last_run": len(raw_game_rows),
    }], schema=watermark_schema).withColumn("last_run_timestamp", F.current_timestamp())

    watermark_update_row.write.format("delta").mode("append").saveAsTable("dbo.control_table")

    print(f"Watermark advance to {new_watermark_value}")
else:
    print("No newly-completed games this run - watermark unchanged")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
