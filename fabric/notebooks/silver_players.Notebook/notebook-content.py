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

# # silver transformation for bronze.dim_players

# CELL ********************

from pyspark.sql import functions as F 
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, TimestampType 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

name_struct = StructType([
    StructField("default", StringType(), True)
])

player_schema = StructType([
    StructField("playerId", StringType(), False),
    StructField("position", StringType(), True),
    StructField("firstName", name_struct, True),
    StructField("lastName", name_struct, True),
    StructField("headshot", StringType(), True),
    StructField("heightInCentimeters", IntegerType(), True),
    StructField("weightInKilograms", IntegerType(), True),
    StructField("sweaterNumber", IntegerType(), True),
    StructField("birthCity", name_struct, True),
    StructField("birthCountry", StringType(), True),
    StructField("birthStateProvince", name_struct, True),
    StructField("draftDetails", StructType([
        StructField("overallPick", IntegerType(), True),
        StructField("pickInRound", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("teamAbbrev", StringType(), True)
    ]), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_bronze = spark.read.table("bronze.dim_players")

df_parsed = df_bronze.withColumn("parsed", F.from_json(F.col("raw_json"), player_schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_player = df_parsed.select(
    F.col("parsed.playerId").alias("player_id"),
    F.col("parsed.position").alias("position"),
    F.col("parsed.firstName.default").alias("first_name"),
    F.col("parsed.lastName.default").alias("last_name"),
    F.col("parsed.headshot").alias("headshot"),
    F.col("parsed.heightInCentimeters").alias("height_cm"),
    F.col("parsed.weightInKilograms").alias("weight_kg"),
    F.col("parsed.sweaterNumber").alias("sweater_number"),
    F.col("parsed.birthCity.default").alias("birth_city"),
    F.col("parsed.birthCountry").alias("birth_country"),
    F.col("parsed.birthStateProvince.default").alias("birth_province"),
    F.col("parsed.draftDetails.overallPick").alias("draft_overall_pick"),
    F.col("parsed.draftDetails.pickInRound").alias("draft_pick_in_round"),
    F.col("parsed.draftDetails.round").alias("draft_round"),
    F.col("parsed.draftDetails.year").alias("draft_year"),
    F.col("parsed.draftDetails.teamAbbrev").alias("draft_team_abbrev"),
    F.col("franchise_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_player.write.format("delta").mode("overwrite").saveAsTable("silver.dim_players")

print(f"Saved silver.dim_players table with {df_dim_player.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
