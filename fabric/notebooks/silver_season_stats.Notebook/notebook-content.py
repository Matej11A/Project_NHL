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

# # notebook to extract season stats from bronze.dim_players

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, ArrayType, LongType, StringType, DoubleType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# schema definition

season_schema = StructType([
    StructField("playerId", LongType(), True),
    StructField("seasonTotals", ArrayType(StructType([
        StructField("season", LongType(), True),
        StructField("sequence", LongType(), True),
        StructField("leagueAbbrev", StringType(), True),
        StructField("teamName", StructType([
            StructField("default", StringType(), True)
        ]), True),
        StructField("wins", LongType(), True),
        StructField("losses", LongType(), True),
        StructField("ties", LongType(), True),
        StructField("otLosses", LongType(), True),
        StructField("points", LongType(), True),
        StructField("goals", LongType(), True),
        StructField("assists", LongType(), True),
        StructField("shots", LongType(), True),
        StructField("shootingPctg", DoubleType(), True),
        StructField("otGoals", LongType(), True),
        StructField("plusMinus", LongType(), True),
        StructField("powerPlayGoals", LongType(), True),
        StructField("powerPlayPoints", LongType(), True),
        StructField("shorthandedGoals", LongType(), True),
        StructField("shorthandedPoints", LongType(), True),
        StructField("timeOnIce", StringType(), True),
        StructField("avgToi", StringType(), True),
        StructField("faceoffWinningPctg", DoubleType(), True),
        StructField("gameWinningGoals", LongType(), True),
        StructField("gameTypeId", LongType(), True),
        StructField("gamesPlayed", LongType(), True),
        StructField("gamesStarted", LongType(), True),
        StructField("pim", LongType(), True),
        StructField("goalsAgainst", LongType(), True),
        StructField("shotsAgainst", LongType(), True),
        StructField("savePctg", DoubleType(), True),
        StructField("shutouts", LongType(), True)
    ])), True)
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

df_bronze = spark.read.table("bronze.dim_players")


new_fields, missing_fields, = check_schema_drift(
    bronze_df=df_bronze,
    raw_col="raw_json",
    expected_schema=season_schema,
    source_entity="fact_season_stats"
)

if missing_fields:
    print(f"Fields expected but not found in recent raw JSON: {missing_fields}")


df_parsed = df_bronze.withColumn("parsed", F.from_json(F.col("raw_json"), season_schema))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_season_stats = (
    df_parsed
    .select(
        F.col("parsed.playerId").alias("player_id"),
        F.explode("parsed.seasonTotals").alias("s")
    )
    .select(
        "player_id",
        F.col("s.season").alias("season"),
        F.col("s.sequence").alias("sequence"),         
        F.col("s.gameTypeId").alias("game_type_id"),
        F.col("s.leagueAbbrev").alias("league_abbrev"),
        F.col("s.teamName.default").alias("team_name"),
        F.col("s.gamesPlayed").alias("games_played"),
        F.col("s.gamesStarted").alias("games_started"),
        F.col("s.goals").alias("goals"),
        F.col("s.assists").alias("assists"),
        F.col("s.points").alias("points"),
        F.col("s.plusMinus").alias("plus_minus"),
        F.col("s.pim").alias("pim"),
        F.col("s.shots").alias("shots"),
        F.col("s.shootingPctg").alias("shooting_pctg"), 
        F.col("s.otGoals").alias("ot_goals"),  
        F.col("s.avgToi").alias("avg_toi"),
        F.col("s.timeOnIce").alias("time_on_ice"),     
        F.col("s.gameWinningGoals").alias("game_winning_goals"),         
        F.col("s.powerPlayGoals").alias("power_play_goals"),
        F.col("s.powerPlayPoints").alias("power_play_points"),
        F.col("s.shorthandedGoals").alias("shorthanded_goals"),
        F.col("s.shorthandedPoints").alias("shorthanded_points"),
        F.col("s.faceoffWinningPctg").alias("faceoff_winning_pctg"),
        F.col("s.wins").alias("wins"),
        F.col("s.losses").alias("losses"),
        F.col("s.ties").alias("ties"),
        F.col("s.otLosses").alias("ot_losses"),
        F.col("s.goalsAgainst").alias("goals_against"),
        F.col("s.shotsAgainst").alias("shots_against"),
        F.col("s.savePctg").alias("save_pctg"),
        F.col("s.shutouts").alias("shutouts"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_season_stats.show(5, truncate=False, vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_season_stats.write.format("delta").mode("overwrite").saveAsTable("silver.fact_season_stats")
print(f"Table silver.fact_season_stats saved with {df_season_stats.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
