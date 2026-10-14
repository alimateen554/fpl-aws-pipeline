# ============================================================
# Glue job: bronze (raw JSON) -> silver (clean Parquet)
# Full refresh: rebuilds every silver table from bronze each run.
# ============================================================

import sys

# Glue-specific helpers (only available inside AWS Glue)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
# F gives us Spark's column functions: col, explode, max, etc.
from pyspark.sql import functions as F

# --- Job parameters ---
# Glue passes settings in as command-line arguments.
# getResolvedOptions reads the ones we name here.
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME"])
BUCKET = args["BUCKET_NAME"]
BRONZE = f"s3://{BUCKET}/bronze"
SILVER = f"s3://{BUCKET}/silver"

# --- Start Spark and Glue ---
sc = SparkContext()
glue_context = GlueContext(sc)
# spark is the normal PySpark session, same as your last project
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)


def read_bronze(folder):
    """Read every JSON file under a bronze folder into one DataFrame."""
    # multiLine=True: each file is one big JSON document,
    # not one record per line.
    # Spark spots folders like "season=2026-27" and
    # "ingest_date=2026-10-13" and adds them as columns.
    return spark.read.option("multiLine", True).json(f"{BRONZE}/{folder}/")


def latest_snapshot(df):
    """Keep only rows from the most recent ingest_date."""
    # .collect()[0][0] pulls the single max value back into Python
    latest = df.agg(F.max("ingest_date")).collect()[0][0]
    return df.filter(F.col("ingest_date") == latest)


def write_silver(df, table, partition_cols=None):
    """Write a DataFrame to silver as Parquet, replacing old data."""
    # overwrite = replace the whole table each run (full refresh)
    writer = df.write.mode("overwrite")
    if partition_cols:
        # Split files into folders by these columns
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(f"{SILVER}/{table}/")
    print(f"Wrote {df.count()} rows to {SILVER}/{table}/")


# ============================================================
# 1. Bootstrap tables (teams, positions, gameweeks, players)
# ============================================================
bootstrap = read_bronze("bootstrap")
latest_bootstrap = latest_snapshot(bootstrap)

# --- Teams ---
# explode turns a list column into one row per item
teams = (
    latest_bootstrap
    .select("season", F.explode("teams").alias("t"))
    .select(
        "season",
        F.col("t.id").alias("team_id"),
        F.col("t.name").alias("team_name"),
        F.col("t.short_name").alias("team_short_name"),
        F.col("t.strength_attack_home").alias("strength_attack_home"),
        F.col("t.strength_attack_away").alias("strength_attack_away"),
        F.col("t.strength_defence_home").alias("strength_defence_home"),
        F.col("t.strength_defence_away").alias("strength_defence_away"),
    )
)
write_silver(teams, "teams")

# --- Positions ---
positions = (
    latest_bootstrap
    .select(F.explode("element_types").alias("p"))
    .select(
        F.col("p.id").alias("position_id"),
        F.col("p.singular_name_short").alias("position_short"),
        F.col("p.singular_name").alias("position_name"),
    )
)
write_silver(positions, "positions")

# --- Gameweeks ---
gameweeks = (
    latest_bootstrap
    .select("season", F.explode("events").alias("e"))
    .select(
        "season",
        F.col("e.id").alias("gameweek"),
        F.col("e.name").alias("gameweek_name"),
        # Text like "2026-08-21T17:30:00Z" -> a real timestamp
        F.to_timestamp("e.deadline_time").alias("deadline_time"),
        F.col("e.finished").alias("finished"),
        F.col("e.data_checked").alias("data_checked"),
        F.col("e.average_entry_score").alias("average_score"),
        F.col("e.highest_score").alias("highest_score"),
    )
)
write_silver(gameweeks, "gameweeks")

# --- Player snapshots (ALL dates, for price history) ---
player_snapshots = (
    bootstrap
    .select("season", "ingest_date", F.explode("elements").alias("p"))
    .select(
        "season",
        "ingest_date",
        F.col("p.id").alias("player_id"),
        F.col("p.first_name").alias("first_name"),
        F.col("p.second_name").alias("second_name"),
        F.col("p.web_name").alias("web_name"),
        F.col("p.team").alias("team_id"),
        F.col("p.element_type").alias("position_id"),
        # Price is stored in tenths: 55 -> 5.5 (£m)
        (F.col("p.now_cost") / 10).alias("price_m"),
        # Comes from the API as text, e.g. "12.3", so cast to a number
        F.col("p.selected_by_percent").cast("double").alias("selected_by_pct"),
        F.col("p.status").alias("status"),
    )
    # Data quality: one row per player per day, no blank IDs
    .filter(F.col("player_id").isNotNull())
    .dropDuplicates(["ingest_date", "player_id"])
)
write_silver(player_snapshots, "player_snapshots", ["ingest_date"])

# ============================================================
# 2. Fixtures (latest snapshot)
# ============================================================
# fixtures.json is a list at the top level, so Spark makes
# one row per fixture automatically - no explode needed.
fixtures = (
    latest_snapshot(read_bronze("fixtures"))
    .select(
        "season",
        F.col("id").alias("fixture_id"),
        F.col("event").alias("gameweek"),
        F.to_timestamp("kickoff_time").alias("kickoff_time"),
        F.col("team_h").alias("home_team_id"),
        F.col("team_a").alias("away_team_id"),
        F.col("team_h_score").alias("home_score"),
        F.col("team_a_score").alias("away_score"),
        F.col("finished").alias("finished"),
        F.col("team_h_difficulty").alias("home_difficulty"),
        F.col("team_a_difficulty").alias("away_difficulty"),
    )
    .dropDuplicates(["fixture_id"])
)
write_silver(fixtures, "fixtures")

# ============================================================
# 3. Player gameweek stats (the future fact table)
# ============================================================
# Each live.json is {"elements": [ {id, stats: {...}}, ... ]}
# "gameweek" comes from the folder name gameweek=01 etc.
live = read_bronze("gameweek_live")

player_gameweek_stats = (
    live
    .select("season", "gameweek", F.explode("elements").alias("e"))
    .select(
        "season",
        "gameweek",
        F.col("e.id").alias("player_id"),
        # Nested fields: e -> stats -> minutes
        F.col("e.stats.minutes").alias("minutes"),
        F.col("e.stats.starts").alias("starts"),
        F.col("e.stats.goals_scored").alias("goals_scored"),
        F.col("e.stats.assists").alias("assists"),
        F.col("e.stats.clean_sheets").alias("clean_sheets"),
        F.col("e.stats.goals_conceded").alias("goals_conceded"),
        F.col("e.stats.saves").alias("saves"),
        F.col("e.stats.yellow_cards").alias("yellow_cards"),
        F.col("e.stats.red_cards").alias("red_cards"),
        F.col("e.stats.bonus").alias("bonus"),
        F.col("e.stats.bps").alias("bps"),
        # These come as text like "0.45", so cast to numbers
        F.col("e.stats.expected_goals").cast("double").alias("expected_goals"),
        F.col("e.stats.expected_assists").cast("double").alias("expected_assists"),
        F.col("e.stats.expected_goals_conceded").cast("double").alias("expected_goals_conceded"),
        F.col("e.stats.ict_index").cast("double").alias("ict_index"),
        F.col("e.stats.total_points").alias("total_points"),
    )
    # Data quality: one row per player per gameweek
    .filter(F.col("player_id").isNotNull())
    .dropDuplicates(["season", "gameweek", "player_id"])
)
write_silver(player_gameweek_stats, "player_gameweek_stats", ["gameweek"])

# Tell Glue the job finished successfully
job.commit()