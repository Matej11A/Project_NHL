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

# # notebook to flatten fact_games

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# schema definition

games_schema = StructType([
    StructField("awayTeam", StructType([
        StructField("abbrev", StringType(), True),
        StructField("score", LongType(), True),
    ]), True),
    StructField("gameDate", StringType(), True),
    StructField("gameOutcome", StructType([
        StructField("lastPeriodType", StringType(), True),
    ]), True),
    StructField("gameScheduleState", StringType(), True),
    StructField("gameState", StringType(), True),
    StructField("gameType", LongType(), True),
    StructField("homeTeam", StructType([
        StructField("abbrev", StringType(), True),
        StructField("score", LongType(), True),
    ]), True),
    StructField("id", LongType(), True),
    StructField("season", LongType(), True),
    StructField("winningGoalScorer", StructType([
        StructField("playerId", LongType(), True)
    ]), True),
    StructField("winningGoalie", StructType([
        StructField("playerId", LongType(), True)
    ]), True)
])

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

df_bronze = spark.read.table("bronze.fact_games")


new_fields, missing_fields, = check_schema_drift(
    bronze_df=df_bronze,
    raw_col="raw_json",
    expected_schema=games_schema,
    source_entity="fact_games"
)

if missing_fields:
    print(f"Fields expected but not found in recent raw JSON: {missing_fields}")

df_parsed = df_bronze.withColumn("parsed", F.from_json(F.col("raw_json"), games_schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# select columns for a table 

df_games = df_parsed.select(
    F.col("parsed.id").alias("id"),
    F.col("parsed.season").alias("season"),
    F.col("parsed.gameDate").alias("game_date"),
    F.col("parsed.gameType").alias("game_type"),
    F.col("parsed.gameState").alias("game_state"),
    F.col("parsed.gameScheduleState").alias("game_scheduled_state"),
    F.col("parsed.homeTeam.abbrev").alias("home_team_abbrev"),
    F.col("parsed.homeTeam.score").alias("home_team_score"),
    F.col("parsed.awayTeam.abbrev").alias("away_team_abbrev"),
    F.col("parsed.awayTeam.score").alias("away_team_score"),
    F.col("parsed.gameOutcome.lastPeriodType").alias("game_outcome_last_period_type"),
    F.col("parsed.winningGoalScorer.playerId").alias("winning_goal_scorer_id"),
    F.col("parsed.winningGoalie.playerId").alias("winning_goalie_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_games.show(3, truncate=False, vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_games.write.format("delta").mode("overwrite").saveAsTable("silver.fact_games")
print(f"Table silver.fact_games saved with {df_games.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
