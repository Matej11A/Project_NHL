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
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType, DoubleType

season_totals_schema = ArrayType(StructType([
    StructField("season", LongType(), True),
    StructField("leagueAbbrev", StringType(), True),
    StructField("teamName", StructType([
        StructField("default", StringType(), True)
    ]), True),
    StructField("gameTypeId", LongType(), True),
    StructField("sequence", LongType(), True),
    StructField("gamesPlayed", LongType(), True),

    StructField("goals", LongType(), True),
    StructField("assists", LongType(), True),
    StructField("points", LongType(), True),
    StructField("plusMinus", LongType(), True),
    StructField("pim", LongType(), True),
    StructField("shots", LongType(), True),
    StructField("shootingPctg", DoubleType(), True),
    StructField("powerPlayGoals", LongType(), True),
    StructField("shorthandedGoals", LongType(), True),
    StructField("gameWinningGoals", LongType(), True),
    StructField("avgToi", StringType(), True),

    StructField("wins", LongType(), True),
    StructField("losses", LongType(), True),
    StructField("otLosses", LongType(), True),
    StructField("goalsAgainst", LongType(), True),
    StructField("goalsAgainstAvg", DoubleType(), True),
    StructField("savePctg", DoubleType(), True),
    StructField("shutouts", LongType(), True),
    StructField("shotsAgainst", LongType(), True),
    StructField("timeOnIce", StringType(), True),
]))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ./util_schema_drift_log


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.read.table("bronze.fact_prospect_season_totals")

df_extracted = df_bronze.withColumn(
    "season_totals_json",
    F.get_json_object(F.col("raw_json"), "$.seasonTotals")
)

new_fields, missing_fields = check_schema_drift(
    bronze_df=df_extracted,
    raw_col="season_totals_json",
    expected_schema=season_totals_schema,
    source_entity="fact_prospect_season_totals"
)

print(f"New fields not in schema: {new_fields}")
print(f"Expected fields missing from recent data: {missing_fields}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_parsed = df_extracted.withColumn(
    "season_totals",
    F.from_json(F.col("season_totals_json"), season_totals_schema)
)

df_exploded = df_parsed.select(
    F.col("player_id"),
    F.explode(F.col("season_totals")).alias("season_row")
)

df_silver = df_exploded.select(
    F.col("player_id"),
    F.col("season_row.season").alias("season"),
    F.col("season_row.leagueAbbrev").alias("league_abbrev"),
    F.col("season_row.teamName.default").alias("team_name"),
    F.col("season_row.gameTypeId").alias("game_type_id"),
    F.col("season_row.sequence").alias("sequence"),
    F.col("season_row.gamesPlayed").alias("games_played"),
    F.col("season_row.goals").alias("goals"),
    F.col("season_row.assists").alias("assists"),
    F.col("season_row.points").alias("points"),
    F.col("season_row.plusMinus").alias("plus_minus"),
    F.col("season_row.pim").alias("pim"),
    F.col("season_row.shots").alias("shots"),
    F.col("season_row.shootingPctg").alias("shooting_pctg"),
    F.col("season_row.powerPlayGoals").alias("power_play_goals"),
    F.col("season_row.shorthandedGoals").alias("shorthanded_goals"),
    F.col("season_row.gameWinningGoals").alias("game_winning_goals"),
    F.col("season_row.avgToi").alias("avg_toi"),
    F.col("season_row.wins").alias("wins"),
    F.col("season_row.losses").alias("losses"),
    F.col("season_row.otLosses").alias("ot_losses"),
    F.col("season_row.goalsAgainst").alias("goals_against"),
    F.col("season_row.goalsAgainstAvg").alias("goals_against_avg"),
    F.col("season_row.savePctg").alias("save_pctg"),
    F.col("season_row.shutouts").alias("shutouts"),
    F.col("season_row.shotsAgainst").alias("shots_against"),
    F.col("season_row.timeOnIce").alias("time_on_ice"),
)

df_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("silver.fact_prospect_season_totals")
print(f"Table silver.fact_prospect_season_totals saved with {df_silver.count()} rows!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
