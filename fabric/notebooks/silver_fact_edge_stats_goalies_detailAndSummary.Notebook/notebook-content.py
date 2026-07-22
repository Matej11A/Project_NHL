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

# # notebook to explode nested arays in json into two delta tables providing more detail for each goalie

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# schema definition

struct_location_schema = StructType([
    StructField("shotLocationDetails", ArrayType(StructType([
        StructField("area", StringType(), True),
        StructField("savePctg", DoubleType(), True),
        StructField("savePctgPercentile", DoubleType(), True),
        StructField("saves", LongType(), True),
        StructField("savesPercentile", DoubleType(), True)
    ])), True),
    StructField("shotLocationSummary", ArrayType(StructType([
        StructField("goalsAgainst", LongType(), True),
        StructField("goalsAgainstLeagueAvg", DoubleType(),True),
        StructField("goalsAgainstPercentile", DoubleType(), True),
        StructField("locationCode", StringType(), True),
        StructField("savePctg", DoubleType(), True),
        StructField("savePctgLeagueAvg", DoubleType(), True),
        StructField("savePctgPercentile", DoubleType(), True),
        StructField("saves", LongType(), True),
        StructField("savesLeagueAvg", DoubleType(), True),
        StructField("savesPercentile", DoubleType(), True)
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

df_bronze = spark.read.table("bronze.fact_edge_stats_goalies")


new_fields, missing_fields, = check_schema_drift(
    bronze_df=df_bronze,
    raw_col="raw_json",
    expected_schema=struct_location_schema,
    source_entity="fact_edge_stats_goalies_detail"
)

if missing_fields:
    print(f"Fields expected but not found in recent raw JSON: {missing_fields}")


df_parsed = df_bronze.withColumn("parsed", F.from_json(F.col("raw_json"), struct_location_schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# select columns for a df

df_shot_detail_goalies = (
    df_parsed
    .select("player_id", "season", F.explode("parsed.shotLocationDetails").alias("detail"))
    .select(
        "player_id", "season",
        F.col("detail.area").alias("area"),
        F.col("detail.savePctg").alias("save_pctg"),
        F.col("detail.savePctgPercentile").alias("save_pctg_percentile"),
        F.col("detail.saves").alias("saves"),
        F.col("detail.savesPercentile").alias("saves_percentile")
    )
)


df_shot_summary_goalies = (
    df_parsed
    .select("player_id", "season", F.explode("parsed.shotLocationSummary").alias("summary"))
    .select(
        "player_id", "season",
        F.col("summary.locationCode").alias("location_code"),
        F.col("summary.goalsAgainst").alias("goals_against"),
        F.col("summary.goalsAgainstLeagueAvg").alias("goals_against_league_avg"),
        F.col("summary.goalsAgainstPercentile").alias("goals_against_percentile"),
        F.col("summary.savePctg").alias("save_pctg"),
        F.col("summary.savePctgLeagueAvg").alias("save_pctg_league_avg"),
        F.col("summary.savePctgPercentile").alias("save_pctg_percentile"),
        F.col("summary.saves").alias("saves"),
        F.col("summary.savesLeagueAvg").alias("saves_league_avg"),
        F.col("summary.savesPercentile").alias("saves_percentile"),
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_shot_detail_goalies.show(1, truncate=False, vertical=True)

df_shot_summary_goalies.show(1, truncate=False, vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_shot_detail_goalies.write.format("delta").mode("overwrite").saveAsTable("silver.fact_edge_stats_goalies_detail")
print(f"Table silver.fact_edge_stats_goalies_detail saved with {df_shot_detail_goalies.count()} rows!")

df_shot_summary_goalies.write.format("delta").mode("overwrite").saveAsTable("silver.fact_edge_stats_goalies_summary")
print(f"Table silver.fact_edge_stats_goalies_summary saved with {df_shot_summary_goalies.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
