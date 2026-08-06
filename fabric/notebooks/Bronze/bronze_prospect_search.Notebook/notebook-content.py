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

raw_dir = f"/lakehouse/default/Files/raw/prospect_search/{ingestion_date}"

rows = []
for filename in os.listdir(raw_dir):
    draft_year, overall_pick = filename.replace(".json", "").split("_")
    raw_path = f"{raw_dir}/{filename}"

    with open(raw_path, "r") as f:
        candidates = json.load(f)

    rows.append({
        "draft_year": draft_year,
        "overall_pick": int(overall_pick),
        "raw_json": json.dumps(candidates),
        "_raw_file_path": raw_path,
    })

df_bronze = spark.createDataFrame(rows)
df_bronze = df_bronze.withColumn("_ingested_at", F.current_timestamp())

if not spark.catalog.tableExists("bronze.dim_prospect_search"):
    df_bronze.write.format("delta").saveAsTable("bronze.dim_prospect_search")
    print(f"Created bronze.dim_prospect_search with {df_bronze.count()} rows")
else:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    df_bronze.createOrReplaceTempView("new_prospect_search")
    spark.sql("""
        MERGE INTO bronze.dim_prospect_search AS target
        USING new_prospect_search AS source
        ON target.draft_year = source.draft_year AND target.overall_pick = source.overall_pick
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merged {df_bronze.count()} rows into bronze.dim_prospect_search")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
