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
from datetime import date

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
    expected_schema=player_schema,
    source_entity="dim_players"
)

if missing_fields:
    print(f"Fields expected but not found in recent raw JSON: {missing_fields}")
# if new_fields:
#     print(f"New fields present in raw JSON but not in schema: {new_fields}")


df_parsed = df_bronze.withColumn("parsed", F.from_json(F.col("raw_json"), player_schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_incoming = df_parsed.select(
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

# MARKDOWN ********************

# scd2 implementation cells

# CELL ********************

table_exists = spark.catalog.tableExists("silver.dim_players")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not table_exists:
    df_initial_load = (
        df_incoming
        .withColumn("effective_start_date", F.current_date())
        .withColumn("effective_end_date", F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("player_sk", F.xxhash64(F.col("player_id"), F.col("effective_start_date")))
    )

    (df_initial_load.write.format("delta").mode("overwrite").saveAsTable("silver.dim_players"))
    print(f"Bootstrapped silver.dim_players with {df_initial_load.count()} initial player versions.")

if table_exists:
    today = F.current_date()

    current_dim = spark.table("silver.dim_players").filter("is_current = true")

    staged = (
        df_incoming.alias("src")
        .join(
            current_dim.select("player_id", "franchise_id").alias("tgt"),
            on="player_id",
            how="left"
        )
        .select(
            "src.*",
            F.col("tgt.franchise_id").alias("current_franchise_id")
        )
        .withColumn(
            "changed",
            (F.col("current_franchise_id").isNotNull()) &
            (F.col("current_franchise_id") != F.col("franchise_id"))
        )
        .withColumn(
            "is_new_player",
            F.col("current_franchise_id").isNull()
        )
    )

    to_close = (
        staged.filter("changed = true")
        .select("player_id")
        .withColumn("merge_key", F.col("player_id"))
    )

    to_insert = (
        staged.filter("changed = true OR is_new_player = true")
        .withColumn("merge_key", F.lit(None).cast("long"))
        .withColumn("effective_start_date", today)
        .withColumn("effective_end_date", F.lit("9999-12-31").cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("player_sk", F.xxhash64(F.col("player_id"), F.col("effective_start_date")))
        .drop("current_franchise_id", "changed", "is_new_player")
    )

    merge_source = to_close.unionByName(to_insert, allowMissingColumns=True)

    merge_source.createOrReplaceTempView("merge_source")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if table_exists:
    spark.sql("""
    MERGE INTO silver.dim_players AS tgt
    USING merge_source AS src
    ON tgt.player_id = src.merge_key AND tgt.is_current = true
    
    WHEN MATCHED THEN
        UPDATE SET
            is_current = false, 
            effective_end_date = CURRENT_DATE()

    WHEN NOT MATCHED THEN
        INSERT (
            player_sk, player_id, position, first_name, last_name, headshot,
            height_cm, weight_kg, sweater_number,
            birth_city, birth_country, birth_province,
            draft_overall_pick, draft_pick_in_round, draft_round, draft_year, draft_team_abbrev,
            franchise_id,
            effective_start_date, effective_end_date, is_current
        )
        VALUES (
            src.player_sk, src.player_id, src.position, src.first_name, src.last_name, src.headshot,
            src.height_cm, src.weight_kg, src.sweater_number,
            src.birth_city, src.birth_country, src.birth_province,
            src.draft_overall_pick, src.draft_pick_in_round, src.draft_round, src.draft_year, src.draft_team_abbrev,
            src.franchise_id,
            src.effective_start_date, src.effective_end_date, src.is_current
        )
    """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Every player should have EXACTLY one is_current = true row, never zero,
# never two. This is the single most important invariant of an SCD2 table —
# if this ever returns rows, something in the MERGE logic broke.
spark.sql("""
    SELECT player_id, COUNT(*) AS current_version_count
    FROM silver.dim_players
    WHERE is_current = true
    GROUP BY player_id
    HAVING COUNT(*) != 1
""").show()

# Spot-check: does anyone actually have more than one version yet?
# (Expected to be empty on your very first non-bootstrap run, since nothing
# has changed franchise_id between runs yet — that's fine, not a bug.)
spark.sql("""
    SELECT player_id, COUNT(*) AS version_count
    FROM silver.dim_players
    GROUP BY player_id
    HAVING COUNT(*) > 1
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df_dim_player.write.format("delta").mode("overwrite").saveAsTable("silver.dim_players")

# print(f"Saved silver.dim_players table with {df_dim_player.count()} rows!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
