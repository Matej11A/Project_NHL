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

# # notebook to explode nested arays in json into two delta tables providing more detail for each skater

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, ArrayType, StringType, LongType, DoubleType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# schema definition

skater_detail_schema = StructType([
    StructField("sogDetails", ArrayType(StructType([
        StructField("area", StringType(), True),
        StructField("shots", LongType(), True),
        StructField("shotsPercentile", DoubleType(), True)
    ])), True),
    StructField("sogSummary", ArrayType(StructType([
        StructField("goals", LongType(), True),
        StructField("goalsLeagueAvg", DoubleType(), True),
        StructField("goalsPercentile", DoubleType(), True),
        StructField("locationCode", StringType(), True),
        StructField("shootingPctg", DoubleType(), True),
        StructField("shootingPctgLeagueAvg", DoubleType(), True),
        StructField("shootingPctgPercentile", DoubleType(), True),
        StructField("shots", LongType(), True),
        StructField("shotsLeagueAvg", DoubleType(), True),
        StructField("shotsPercentile", DoubleType(), True)
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

df_bronze = spark.read.table("bronze.fact_edge_stats_skaters")


new_fields, missing_fields, = check_schema_drift(
    bronze_df=df_bronze,
    raw_col="raw_json",
    expected_schema=skater_detail_schema,
    source_entity="fact_edge_stats_skaters_detail"
)

if missing_fields:
    print(f"Fields expected but not found in recent raw JSON: {missing_fields}")


df_parsed = df_bronze.withColumn("parsed", F.from_json(F.col("raw_json"), skater_detail_schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# select for df

df_stats_detail_skaters = (
    df_parsed
    .select("player_id", "season", F.explode("parsed.sogDetails").alias("detail"))
    .select(
        "player_id", "season",
        F.col("detail.area").alias("area"),
        F.col("detail.shots").alias("shots"),
        F.col("detail.shotsPercentile").alias("shots_percentile")
    )
)

df_stats_summary_skaters = (
    df_parsed
    .select("player_id", "season", F.explode("parsed.sogSummary").alias("summary"))
    .select(
        "player_id", "season",
        F.col("summary.locationCode").alias("location_code"),
        F.col("summary.goals").alias("goals"),
        F.col("summary.goalsLeagueAvg").alias("goals_league_avg"),
        F.col("summary.goalsPercentile").alias("goals_percentile"),
        F.col("summary.shootingPctg").alias("shooting_pctg"),
        F.col("summary.shootingPctgLeagueAvg").alias("shooting_pctg_league_avg"),
        F.col("summary.shootingPctgPercentile").alias("shooting_pctg_percentile"),
        F.col("summary.shots").alias("shots"),
        F.col("summary.shotsLeagueAvg").alias("shots_league_avg"),
        F.col("summary.shotsPercentile").alias("shots_percentile")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_stats_detail_skaters.show(1, truncate=False, vertical=True)

df_stats_summary_skaters.show(1, truncate=False, vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_stats_detail_skaters.write.format("delta").mode("overwrite").saveAsTable("silver.fact_edge_stats_skaters_detail")
print(f"Table silver.fact_edge_stats_skaters_detail saved with {df_stats_detail_skaters.count()} rows!")

df_stats_summary_skaters.write.format("delta").mode("overwrite").saveAsTable("silver.fact_edge_stats_skaters_summary")
print(f"Table silver.fact_edge_stats_skaters_summary saved with {df_stats_summary_skaters.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
