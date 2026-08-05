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
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType, BooleanType
from pyspark.sql.functions import udf
import unicodedata

candidate_schema = ArrayType(StructType([
    StructField("playerId", StringType(), True),
    StructField("name", StringType(), True),
    StructField("positionCode", StringType(), True),
    StructField("teamId", StringType(), True),
    StructField("teamAbbrev", StringType(), True),
    StructField("lastTeamId", StringType(), True),
    StructField("lastTeamAbbrev", StringType(), True),
    StructField("lastSeasonId", StringType(), True),
    StructField("sweaterNumber", LongType(), True),
    StructField("active", BooleanType(), True),
    StructField("height", StringType(), True),
    StructField("heightInInches", LongType(), True),
    StructField("heightInCentimeters", LongType(), True),
    StructField("weightInPounds", LongType(), True),
    StructField("weightInKilograms", LongType(), True),
    StructField("birthCity", StringType(), True),
    StructField("birthStateProvince", StringType(), True),
    StructField("birthCountry", StringType(), True),
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

df_bronze = spark.read.table("bronze.dim_prospect_search")

new_fields, missing_fields = check_schema_drift(
    bronze_df=df_bronze,
    raw_col="raw_json",
    expected_schema=candidate_schema,
    source_entity="dim_prospect_search"
)

print(f"New fields not in schema: {new_fields}")
print(f"Expected fields missing from recent data: {missing_fields}")

df_parsed = df_bronze.withColumn("candidates", F.from_json(F.col("raw_json"), candidate_schema))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
import unicodedata

POSITION_CODE_MAP = {"LW": "L", "RW": "R", "C": "C", "D": "D", "G": "G"}

def normalize(s):
    if not s:
        return ""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()

def name_tokens(s):
    if not s:
        return set()
    return set(re.findall(r"[a-z]+", normalize(s)))

def height_weight_distance(height, weight, candidate):
    cand_height = candidate["heightInInches"]
    cand_weight = candidate["weightInPounds"]
    if height is None or weight is None or cand_height is None or cand_weight is None:
        return float("inf")
    return abs(height - cand_height) + abs(weight - cand_weight)

def resolve_candidate(candidates, first_name, last_name, country_code, position_code, height, weight, margin_threshold=10):
    if not candidates:
        return (None, "unresolved", "no_candidates")

    if not last_name:
        return (None, "unresolved", "no_last_name")

    last_tokens = name_tokens(last_name)
    first_tokens = name_tokens(first_name)

    name_matched = [
        c for c in candidates
        if last_tokens.issubset(name_tokens(c["name"])) and (first_tokens & name_tokens(c["name"]))
    ]

    if not name_matched:
        return (None, "unresolved", "no_name_match")

    expected_position = POSITION_CODE_MAP.get(position_code)
    bio_matched = [
        c for c in name_matched
        if c["birthCountry"] == country_code and c["positionCode"] == expected_position
    ]

    if not bio_matched:
        return (None, "unresolved", "name_matched_no_country_position")

    if len(bio_matched) == 1:
        match = bio_matched[0]
        confidence = "active" if match["active"] else "inactive"
        return (match["playerId"], "tier1", f"name_country_position_{confidence}")

    scored = sorted(bio_matched, key=lambda c: height_weight_distance(height, weight, c))
    best, second = scored[0], scored[1]
    best_dist = height_weight_distance(height, weight, best)

    if best_dist == float("inf"):
        return (None, "unresolved", "no_usable_height_weight_data")

    second_dist = height_weight_distance(height, weight, second)
    gap = second_dist - best_dist
    if gap >= margin_threshold:
        confidence = "active" if best["active"] else "inactive"
        return (best["playerId"], "tier2", f"closest_match_gap_{gap}_{confidence}")
    return (None, "unresolved", f"ambiguous_gap_{gap}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


resolution_schema = StructType([
    StructField("player_id", StringType(), True),
    StructField("match_tier", StringType(), True),
    StructField("match_detail", StringType(), True),
])

resolve_udf = udf(resolve_candidate, resolution_schema)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_draft_bio = spark.read.table("silver.dim_draft_picks").select(
    "draft_year", "overall_pick", "first_name", "last_name", "country_code", "position_code", "height", "weight"
)

df_joined = df_parsed.join(df_draft_bio, on=["draft_year", "overall_pick"], how="left")

df_resolved = df_joined.withColumn(
    "resolution",
    resolve_udf(F.col("candidates"), F.col("first_name"), F.col("last_name"), F.col("country_code"), F.col("position_code"), F.col("height"), F.col("weight"))
)

df_bridge = df_resolved.select(
    F.col("draft_year"), F.col("overall_pick"),
    F.col("resolution.player_id").alias("player_id"),
    F.col("resolution.match_tier").alias("match_tier"),
    F.col("resolution.match_detail").alias("match_detail"),
)

df_bridge.write.format("delta").mode("overwrite").saveAsTable("silver.bridge_prospect_player")
print(f"Table silver.bridge_prospect_player saved with {df_bridge.count()} rows!")
df_bridge.groupBy("match_tier").count().show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unresolved_detail = (
    df_bridge.filter(F.col("match_tier") == "unresolved")
    .join(
        spark.read.table("silver.dim_draft_picks")
            .select("draft_year", "overall_pick", "first_name", "last_name", "country_code", "position_code"),
        on=["draft_year", "overall_pick"]
    )
    .select("overall_pick", "first_name", "last_name", "country_code", "position_code", "match_detail")
    .orderBy("overall_pick")
)
unresolved_detail.show(25, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
