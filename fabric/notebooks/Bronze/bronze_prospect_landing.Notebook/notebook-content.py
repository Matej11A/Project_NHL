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
from datetime import date

if ingestion_date is None:
    ingestion_date = date.today().isoformat()

raw_dir = f"/lakehouse/default/Files/raw/prospect_landing/{ingestion_date}"

rows = []
for filename in os.listdir(raw_dir):
    player_id = filename.replace(".json", "")
    raw_path = f"{raw_dir}/{filename}"

    with open(raw_path, "r") as f:
        data = json.load(f)

    rows.append({
        "player_id": player_id,
        "raw_json": json.dumps(data),
        "_raw_file_path": raw_path,
    })

df_bronze = spark.createDataFrame(rows)
df_bronze = df_bronze.withColumn("_ingested_at", F.current_timestamp())
df_bronze.write.format("delta").mode("overwrite").saveAsTable("bronze.fact_prospect_season_totals")
print(f"Table bronze.fact_prospect_season_totals saved with {df_bronze.count()} rows!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
