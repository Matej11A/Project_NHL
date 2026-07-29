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

silver_games = spark.table("silver.fact_games")

gold_players = spark.table("gold.dim_players")

gold_teams = spark.table("gold.dim_nhl_teams")

gold_date = spark.table("gold.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_games_typed = silver_games.withColumn("game_date", F.to_date(F.col("game_date")))


scorer_dim = gold_players.alias("scorer_dim")

games_with_scorer = (
    silver_games_typed.alias("g")
    .join(
        scorer_dim,
        on=(
            (F.col("g.winning_goal_scorer_id") == F.col("scorer_dim.player_id")) &
            (F.col("g.game_date") >= F.col("scorer_dim.effective_start_date")) &
            (F.col("g.game_date") < F.col("scorer_dim.effective_end_date"))
        ),
        how="left"
    )
    .select(
        "g.id", "g.season", "g.game_date", "g.game_type", "g.game_state",
        "g.game_scheduled_state", "g.home_team_abbrev", "g.home_team_score",
        "g.away_team_abbrev", "g.away_team_score", "g.game_outcome_last_period_type",
        "g.winning_goalie_id", 
        F.col("scorer_dim.player_sk").alias("winning_scorer_sk"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

goalie_dim = gold_players.alias("goalie_dim")

games_with_scorer_and_goalie = (
    games_with_scorer.alias("g2")
    .join(
        goalie_dim,
        on=(
            (F.col("g2.winning_goalie_id") == F.col("goalie_dim.player_id")) &
            (F.col("g2.game_date") >= F.col("goalie_dim.effective_start_date")) &
            (F.col("g2.game_date") < F.col("goalie_dim.effective_end_date"))
        ),
        how="left"
    )
    .select(
        "g2.id", "g2.season", "g2.game_date", "g2.game_type", "g2.game_state",
        "g2.game_scheduled_state", "g2.home_team_abbrev", "g2.home_team_score",
        "g2.away_team_abbrev", "g2.away_team_score", "g2.game_outcome_last_period_type",
        "g2.winning_scorer_sk",
        F.col("goalie_dim.player_sk").alias("winning_goalie_sk"),
    )
)    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

home_team_dim = gold_teams.alias("home_team_dim")

games_with_home_team = (
    games_with_scorer_and_goalie.alias("g3")
    .join(
        home_team_dim,
        on=F.col("g3.home_team_abbrev") == F.col("home_team_dim.abbr"),
        how="left"
    )
    .select(
        "g3.id", "g3.season", "g3.game_date", "g3.game_type", "g3.game_state",
        "g3.game_scheduled_state", "g3.home_team_abbrev", "g3.home_team_score",
        "g3.away_team_abbrev", "g3.away_team_score", "g3.game_outcome_last_period_type",
        "g3.winning_scorer_sk", "g3.winning_goalie_sk",
        F.col("home_team_dim.team_sk").alias("home_team_sk"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

away_team_dim = gold_teams.alias("away_team_dim")

games_with_away_team = (
    games_with_home_team.alias("g4")
    .join(
        away_team_dim,
        on=F.col("g4.away_team_abbrev") == F.col("away_team_dim.abbr"),
        how="left"
    )
    .select(
        "g4.id", "g4.season", "g4.game_date", "g4.game_type", "g4.game_state",
        "g4.game_scheduled_state", "g4.home_team_abbrev", "g4.home_team_score",
        "g4.away_team_abbrev", "g4.away_team_score", "g4.game_outcome_last_period_type",
        "g4.winning_scorer_sk", "g4.winning_goalie_sk", "g4.home_team_sk",
        F.col("away_team_dim.team_sk").alias("away_team_sk"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

games_with_away_team_dated = games_with_away_team.withColumn(
    "date_sk", F.date_format(F.col("game_date"), "yyyyMMdd").cast("int")
)

date_dim = gold_date.alias("date_dim")

games_final = (
    games_with_away_team_dated.alias("g5")
    .join(
        F.broadcast(date_dim.select("date_sk")),
        on="date_sk",
        how="left"
    )
    .select(
        "g5.id", "g5.season", "g5.game_date", "g5.game_type", "g5.game_state",
        "g5.game_scheduled_state", "g5.home_team_abbrev", "g5.home_team_score",
        "g5.away_team_abbrev", "g5.away_team_score", "g5.game_outcome_last_period_type",
        "g5.winning_scorer_sk", "g5.winning_goalie_sk", "g5.home_team_sk", "g5.away_team_sk",
        "g5.date_sk",
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

row_count_match = games_final.count() == silver_games.count()
null_date_sk = games_final.filter(F.col("date_sk").isNull()).count()
print(f"Row count match: {row_count_match}")
print(f"Null date_sk rows: {null_date_sk}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

games_final.write.format("delta").mode("overwrite").saveAsTable("gold.fact_games")
print(f"Table gold.fact_games saved with {games_final.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
