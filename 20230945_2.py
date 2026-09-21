# -*- coding: utf-8 -*-
"""
Project 2 - CSE 231 Final Project
Student ID: 20230945

Real-time streaming analytics pipeline for 2026 FIFA World Cup tweets.

Pipeline:
    1. generate_tweets_by_id.py writes ONE timestamped CSV file
       (e.g. 20230945_1718659800.csv) into /data/incoming/.
    2. Spark Structured Streaming reads new files from /data/incoming/
       as micro-batches (file-source stream, per official docs).
    3. For each micro-batch we:
         a) parse the CSV row (id, timestamp, user, text, country),
         b) scan the `text` field for hashtags of any of the 48 qualified
            FIFA World Cup teams,
         c) count mentions per country,
         d) write the current ranking to the console every TRIGGER seconds.
    4. State is held across micro-batches by writing the per-batch counts
       to an in-memory sink that we read back via foreachBatch, so the
       cumulative ranking grows as more files land in /data/incoming/.

Usage:
    spark-submit 20230945_2.py [--watch-dir /data/incoming]
                                [--trigger-seconds 10]

Dependencies:
    pyspark
"""

import argparse
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


# 48 qualified teams - same source of truth as generate_tweets_by_id.py
# Format: (display_name, lowercase hashtag). Country is taken from the CSV's
# "country" column when present, otherwise we use the team code as country.
TEAMS_48 = [
    ("Argentina",      "argentina"),
    ("Australia",      "australia"),
    ("Belgium",        "belgium"),
    ("Brazil",         "brazil"),
    ("Cameroon",       "cameroon"),
    ("Canada",         "canada"),
    ("Chile",          "chile"),
    ("Colombia",       "colombia"),
    ("Croatia",        "croatia"),
    ("Denmark",        "denmark"),
    ("Ecuador",        "ecuador"),
    ("Egypt",          "egypt"),
    ("England",        "england"),
    ("France",         "france"),
    ("Germany",        "germany"),
    ("Ghana",          "ghana"),
    ("Iran",           "iran"),
    ("Ireland",        "ireland"),
    ("Italy",          "italy"),
    ("Ivory Coast",    "ivorycoast"),
    ("Jamaica",        "jamaica"),
    ("Japan",          "japan"),
    ("Jordan",         "jordan"),
    ("Mexico",         "mexico"),
    ("Morocco",        "morocco"),
    ("Netherlands",    "netherlands"),
    ("New Zealand",    "newzealand"),
    ("Nigeria",        "nigeria"),
    ("Norway",         "norway"),
    ("Panama",         "panama"),
    ("Paraguay",       "paraguay"),
    ("Peru",           "peru"),
    ("Poland",         "poland"),
    ("Portugal",       "portugal"),
    ("Qatar",          "qatar"),
    ("Saudi Arabia",   "saudiarabia"),
    ("Scotland",       "scotland"),
    ("Senegal",        "senegal"),
    ("Serbia",         "serbia"),
    ("South Korea",    "southkorea"),
    ("Spain",          "spain"),
    ("Sweden",         "sweden"),
    ("Switzerland",    "switzerland"),
    ("Tunisia",        "tunisia"),
    ("Turkey",         "turkey"),
    ("Ukraine",        "ukraine"),
    ("United States",  "unitedstates"),
    ("Uruguay",        "uruguay"),
]


CSV_SCHEMA = StructType([
    StructField("id",        IntegerType(),   True),
    StructField("timestamp", StringType(),    True),
    StructField("user",      StringType(),    True),
    StructField("text",      StringType(),    True),
    StructField("country",   StringType(),    True),
])


def build_team_regex() -> str:
    """Return a case-insensitive regex that matches any of the 48 hashtags
    as a whole word. We escape each tag so '#brazil' won't match '#brazilian'.
    The whole alternation is wrapped in a non-capturing group so the
    negative lookbehind only fires once, on the leading '#'."""
    import re
    escaped = [re.escape("#" + tag) for _, tag in TEAMS_48]
    return r"(?i)(?<!\w)(?:" + r"|".join(escaped) + r")(?!\w)"


# Globals for state across micro-batches (running cumulative counts).
# Initialized inside main() so each SparkSession has its own copy.
STATE = None  # type: ignore


def init_state(spark: SparkSession):
    """Create an empty cumulative-counts DataFrame keyed by team."""
    schema = "team STRING, country STRING, mentions BIGINT"
    return spark.createDataFrame([], schema)


def merge_and_print(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: merge this batch into cumulative state and print."""
    global STATE
    spark = batch_df.sparkSession

    if STATE is None:
        STATE = init_state(spark)

    # Per-team counts in this micro-batch only
    batch_team_counts = (
        batch_df.groupBy("team", "country")
        .agg(F.count(F.lit(1)).alias("delta"))
    )

    # Outer-join with cumulative state on (team, country); default missing to 0.
    state_only = STATE.select(
        F.col("team").alias("_team"),
        F.col("country").alias("_country"),
        F.col("mentions").cast("long").alias("prior"),
    )

    batch_only = batch_team_counts.select(
        F.col("team").alias("_team"),
        F.col("country").alias("_country"),
        F.col("delta").cast("long"),
    )

    joined = (
        state_only.join(batch_only, on=["_team", "_country"], how="outer")
        .na.fill({"prior": 0, "delta": 0})
    )

    STATE = (
        joined.select(
            F.col("_team").alias("team"),
            F.col("_country").alias("country"),
            (F.col("prior") + F.col("delta")).alias("mentions"),
        )
    )

    ranking = STATE.orderBy(F.col("mentions").desc(), F.col("team").asc()).fillna({"country": "--"})

    print("\n" + "=" * 78)
    print(f"[batch {batch_id}] Cumulative country ranking by team mentions")
    print("=" * 78)
    print(f"{'RANK':>4}  {'TEAM':<18}  {'COUNTRY':<8}  {'MENTIONS':>10}")
    print("-" * 78)
    rows = ranking.collect()
    if not rows:
        print("(no mentions yet)")
    else:
        for i, r in enumerate(rows, 1):
            team    = (r["team"]    or "--")
            country = (r["country"] or "--")
            mentions = int(r["mentions"] or 0)
            print(f"{i:>4}  {team:<18}  {country:<8}  {mentions:>10,}")
    print("=" * 78)


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming World Cup tweet analyzer.")
    parser.add_argument("--watch-dir",      default="/data/incoming",
                        help="Directory to monitor for new CSV files (default: /data/incoming).")
    parser.add_argument("--trigger-seconds", type=int, default=10,
                        help="Trigger interval in seconds (default: 10).")
    parser.add_argument("--max-batches",    type=int, default=0,
                        help="Stop after this many batches (0 = run forever).")
    parser.add_argument("--checkpoint-dir", default="/tmp/spark_checkpoint_20230945",
                        help="Checkpoint directory for the streaming query.")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("CSE231_P2_20230945_streaming")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- 1) Streaming read from /data/incoming ---
    raw_stream = (
        spark.readStream
        .schema(CSV_SCHEMA)
        .option("header",        True)
        .option("multiLine",     False)
        .option("maxFilesPerTrigger", 1)
        .csv(args.watch_dir)
    )

    # --- 2) Extract team mentions from the text field ---
    team_regex = build_team_regex()

    # First find which hashtag is in the text
    with_team = raw_stream.withColumn(
        "matched_hashtag",
        F.expr(f"regexp_extract(text, '{team_regex}', 0)"),
    ).filter(F.length(F.col("matched_hashtag")) > 0)

    # Map matched hashtag -> team display name
    tag_to_team = {f"#{tag}": name for name, tag in TEAMS_48}
    map_expr = F.create_map(*[F.lit(k) for kv in tag_to_team.items() for k in (kv,)])
    # build_map([lit(k), lit(v), ...])
    map_expr = F.create_map(
        *[x for kv in tag_to_team.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
    )

    enriched = (
        with_team
        .withColumn("team", map_expr[F.col("matched_hashtag")])
        .withColumn("country",
                    F.when(F.col("country").isNull() | (F.length(F.col("country")) == 0),
                            F.lit("--"))
                     .otherwise(F.col("country")))
    )

    # --- 3) ForeachBatch sink: maintain state, print ranking ---
    query = (
        enriched.writeStream
        .outputMode("append")
        .foreachBatch(merge_and_print)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .option("checkpointLocation", args.checkpoint_dir)
        .start()
    )

    print(f"[20230945_2] streaming query started, watching {args.watch_dir} every {args.trigger_seconds}s")
    print(f"[20230945_2] checkpoint: {args.checkpoint_dir}")
    print(f"[20230945_2] drop a CSV file into {args.watch_dir}/ and watch the rankings update.")

    try:
        if args.max_batches > 0:
            # Approximate stop: poll query.recentProgress size
            seen = 0
            while query.isActive and seen < args.max_batches:
                query.awaitTermination(timeout=args.trigger_seconds + 1)
                seen = len(query.recentProgress) if query.recentProgress else seen
        else:
            query.awaitTermination()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            query.stop()
        except Exception:
            pass
        spark.stop()


if __name__ == "__main__":
    main()