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
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# schema definiton

pick_schema = StructType([
    StructField("amateurClubName", StringType(), True),
    StructField("amateurLeague", StringType(), True),
    StructField("countryCode", StringType(), True),
    StructField("firstName", StructType([
        StructField("default", StringType())
    ]), True),
    StructField("lastName", StructType([
        StructField("default", StringType())
    ]), True),
    StructField("height", LongType(), True),
    StructField("weight", LongType(), True),
    StructField("teamAbbrev", StringType(), True),
    StructField("teamLogoDark", StringType(), True),
    StructField("positionCode", StringType(), True),
    StructField("overallPick", LongType(), True),
    StructField("round", LongType(), True),
    StructField("pickInRound", LongType(), True)
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

df_bronze = spark.read.table("bronze.dim_draft_picks")

new_fields, missing_fields, = check_schema_drift(
    bronze_df=df_bronze,
    raw_col="raw_json",
    expected_schema=pick_schema,
    source_entity="dim_draft_picks"
)

if missing_fields:
    print(f"Fields expected but not found in recent raw JSON: {missing_fields}")

df_parsed = df_bronze.withColumn("parsed", F.from_json(F.col("raw_json"), pick_schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#select columns 

df_draft_picks = df_parsed.select(
    F.col("draft_year").alias("draft_year"),
    F.col("parsed.amateurClubName").alias("amateur_club_name"),
    F.col("parsed.amateurLeague").alias("amateur_league"),
    F.col("parsed.countryCode").alias("country_code"),
    F.col("parsed.firstName.default").alias("first_name"),
    F.col("parsed.lastName.default").alias("last_name"),
    F.col("parsed.height").alias("height"),
    F.col("parsed.weight").alias("weight"),
    F.col("parsed.teamAbbrev").alias("team_abbrev"),
    F.col("parsed.teamLogoDark").alias("team_logo"),
    F.col("parsed.positionCode").alias("position_code"),
    F.col("parsed.overallPick").alias("overall_pick"),
    F.col("parsed.round").alias("round"),
    F.col("parsed.pickInRound").alias("pick_in_round")
)


df_draft_picks.write.format("delta").mode("overwrite").saveAsTable("silver.dim_draft_picks")
print(f"Table silver.dim_draft_picks saved with {df_draft_picks.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
