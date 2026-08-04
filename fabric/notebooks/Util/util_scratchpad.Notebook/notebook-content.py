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

# ### notebook for exploration and random code snipets

# CELL ********************

df_bronze = spark.read.table("bronze.dim_players")

sample_json_rdd = df_bronze.select("raw_json").rdd.map(lambda row: row["raw_json"])
inferred_df = spark.read.json(sample_json_rdd)
inferred_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

inferred_df.select("playerId", "firstName.default", "lastName.default", "position").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

inferred_df.limit(1).toPandas().to_dict(orient="records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
inferred_df.select("playerId", F.explode("seasonTotals").alias("season_row")).select("playerId", "season_row.*").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import functions as F

df_bronze = spark.read.table("bronze.dim_nhl_teams")

df_bronze.printSchema()

# df_bronze.select("conference.*").printSchema()
# df_bronze.select("division.*").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

skaters_rdd = spark.read.table("bronze.fact_edge_stats_skaters").select("raw_json").rdd.map(lambda row: row["raw_json"])
skaters_inferred = spark.read.json(skaters_rdd)
skaters_inferred.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

goalies_rdd = spark.read.table("bronze.fact_edge_stats_goalies").select("raw_json").rdd.map(lambda row: row["raw_json"])
goalies_inferred = spark.read.json(goalies_rdd)
goalies_inferred.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

games_rdd = spark.read.table("bronze.fact_games").select("raw_json").rdd.map(lambda row: row["raw_json"])
games_inferred = spark.read.json(games_rdd)
games_inferred.printSchema()

games_inferred.show(1, truncate=False, vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

path = "/lakehouse/default/Files/raw/draft_picks/2026-08-03/2026.json"

with open(path, "r") as f:
    data = json.load(f)

for key, value in data.items():
    print(f"{key}: {type(value).__name__}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

path = "Files/raw/draft_picks/2026-08-03/2026.json"

df = spark.read.option("multiline", "true").json(path)

df.select("picks").printSchema()
df.select(F.explode("picks").alias("pick")).select("pick.*").show(3, truncate=False, vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

season_rdd = spark.read.table("bronze.dim_players").select("raw_json").rdd.map(lambda row: row["raw_json"])
season_inferred = spark.read.json(season_rdd)
season_inferred.printSchema()

season_inferred.show(1, truncate=False, vertical=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
season_inferred.select(F.explode("seasonTotals").alias("s")).select("s.timeOnIce", "s.avgToi").filter("s.timeOnIce IS NOT NULL").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

row_count_check = spark.table("silver.fact_games").count() == games_final.count()
null_rate_check = games_final.select(
    F.avg(F.col("winning_scorer_sk").isNull().cast("int")).alias("scorer_null_rate"),
    F.avg(F.col("winning_goalie_sk").isNull().cast("int")).alias("goalie_null_rate"),
    F.avg(F.col("home_team_sk").isNull().cast("int")).alias("home_team_null_rate"),
    F.avg(F.col("away_team_sk").isNull().cast("int")).alias("away_team_null_rate"),
)

print(f"Row count match: {row_count_check}")
null_rate_check.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

row_count_check = silver_career_totals.count() == df_career_totals.count()
null_rate_check = df_career_totals.select(
    F.avg(F.col("player_sk").isNull().cast("int")).alias("player_sk_null_rate")
)

print(f"Row count match: {row_count_check}")
null_rate_check.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

# 1. Draft prospects by category, for the 2026 season (all 404 — confirmed dead end)
for category in [1, 2, 3, 4]:
    url = f"https://api-web.nhle.com/v1/draft/prospects/2026/{category}"
    category_resp = requests.get(url, timeout=10)
    print(f"category {category}: status {category_resp.status_code}")

# 2. Re-fetch TOR prospects explicitly under its own variable name
team_prospects_resp = requests.get("https://api-web.nhle.com/v1/prospects/TOR", timeout=10)
prospects = team_prospects_resp.json()

# 3. Find McKenna's id
mckenna_id = None
for group in ("forwards", "defensemen", "goalies"):
    for p in prospects.get(group, []):
        if p["firstName"]["default"] == "Gavin" and p["lastName"]["default"] == "McKenna":
            mckenna_id = p["id"]
            print(f"Found id: {mckenna_id}")

if mckenna_id is None:
    print("Not in TOR's prospects list — worth printing all forward names to check for a spelling/data mismatch")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

search_resp = requests.get(
    "https://search.d3.nhle.com/api/v1/search/player",
    params={"culture": "en-us", "limit": 20, "q": "McKenna"},
    timeout=10
)
print(f"status: {search_resp.status_code}")
print(json.dumps(search_resp.json(), indent=2)[:2000])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mckenna_id = "8486067"

url = f"https://api-web.nhle.com/v1/player/{mckenna_id}/landing"
resp = requests.get(url, timeout=10)
print(f"status: {resp.status_code}")
data = resp.json()

print("\nTop-level keys:", list(data.keys()))
print("\nseasonTotals present:", "seasonTotals" in data)

if "seasonTotals" in data:
    for season in data["seasonTotals"]:
        print(season.get("season"), season.get("leagueAbbrev"), season.get("teamName"), 
              "GP:", season.get("gamesPlayed"), "G:", season.get("goals"), "A:", season.get("assists"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
