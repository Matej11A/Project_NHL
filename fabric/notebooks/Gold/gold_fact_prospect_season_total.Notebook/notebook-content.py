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

df_silver_totals = spark.read.table("silver.fact_prospect_season_totals")
df_bridge = spark.read.table("silver.bridge_prospect_player").filter(F.col("player_id").isNotNull())

df_joined = df_silver_totals.join(
    df_bridge.select("draft_year", "overall_pick", "player_id"),
    on="player_id",
    how="inner"
)

df_gold = df_joined.withColumn(
    "draft_pick_sk", F.xxhash64(F.col("draft_year"), F.col("overall_pick"))
).withColumn(
    "prospect_season_sk",
    F.xxhash64(F.col("player_id"), F.col("season"), F.col("league_abbrev"), F.col("team_name"), F.col("game_type_id"))
)

df_fact_prospect_season_totals = df_gold.select(
    F.col("prospect_season_sk"),
    F.col("draft_pick_sk"),
    F.col("player_id"),
    F.col("season"),
    F.col("league_abbrev"),
    F.col("team_name"),
    F.col("game_type_id"),
    F.col("games_played"),
    F.col("goals"),
    F.col("assists"),
    F.col("points"),
    F.col("plus_minus"),
    F.col("pim"),
    F.col("shots"),
    F.col("shooting_pctg"),
    F.col("power_play_goals"),
    F.col("shorthanded_goals"),
    F.col("game_winning_goals"),
    F.col("avg_toi"),
    F.col("wins"),
    F.col("losses"),
    F.col("ot_losses"),
    F.col("goals_against"),
    F.col("goals_against_avg"),
    F.col("save_pctg"),
    F.col("shutouts"),
    F.col("shots_against"),
    F.col("time_on_ice"),
)

df_fact_prospect_season_totals.write.format("delta").mode("overwrite").saveAsTable("gold.fact_prospect_season_totals")
print(f"Table gold.fact_prospect_season_totals saved with {df_fact_prospect_season_totals.count()} rows!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
