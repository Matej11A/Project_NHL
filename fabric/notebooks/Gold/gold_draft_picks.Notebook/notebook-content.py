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

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver = spark.read.table("silver.dim_draft_picks")

df_gold = df_silver.withColumn(
    "draft_pick_sk",
    F.xxhash64(F.col("draft_year"), F.col("overall_pick"))
)

df_draft_picks = df_gold.select(
    F.col("draft_pick_sk"),
    F.col("draft_year"),
    F.col("first_name"),
    F.col("last_name"),
    F.col("amateur_club_name"),
    F.col("amateur_league"),
    F.col("country_code"),
    F.col("height"),
    F.col("weight"),
    F.col("team_abbrev"),
    F.col("team_logo"),
    F.col("position_code"),
    F.col("overall_pick"),
    F.col("round"),
    F.col("pick_in_round"),
)

df_draft_picks.write.format("delta").mode("overwrite").saveAsTable("gold.dim_draft_picks")
print(f"Table gold.dim_draft_picks saved with {df_draft_picks.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
