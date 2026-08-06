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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_dir = f"/lakehouse/default/Files/raw/draft_picks/{ingestion_date}"

rows = []
for filename in os.listdir(raw_dir):
    year = filename.replace(".json", "")
    raw_path = f"{raw_dir}/{filename}"

    with open(raw_path, "r") as f:
        data = json.load(f)

    for pick in data["picks"]:
        rows.append({
            "draft_year": year,
            "overall_pick": pick.get("overallPick"),
            "raw_json": json.dumps(pick),
            "_raw_file_path": raw_path,
        })

df_bronze = spark.createDataFrame(rows)
df_bronze = df_bronze.withColumn("_ingested_at", F.current_timestamp())

if not spark.catalog.tableExists("bronze.dim_draft_picks"):
    df_bronze.write.format("delta").saveAsTable("bronze.dim_draft_picks")
    print(f"Created bronze.dim_draft_picks with {df_bronze.count()} rows")
else:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    df_bronze.createOrReplaceTempView("new_draft_picks")
    spark.sql("""
        MERGE INTO bronze.dim_draft_picks AS target
        USING new_draft_picks AS source
        ON target.draft_year = source.draft_year AND target.overall_pick = source.overall_pick
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merged {df_bronze.count()} rows into bronze.dim_draft_picks")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
