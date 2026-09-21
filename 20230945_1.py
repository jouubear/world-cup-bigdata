# -*- coding: utf-8 -*-
"""
Project 1 - CSE 231 Final Project
Student ID: 20230945
Description:
    Use Apache Spark (PySpark) to count the total number of words and lines
    in the input text file (20230945.txt) and output the words sorted by
    frequency in descending order.

Usage:
    spark-submit 20230945_1.py <input_txt_file>
or  python3    20230945_1.py <input_txt_file>

Dependencies:
    pyspark
"""

import sys
import re
import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, explode, length


def clean_tokenize(token: str) -> str:
    """Strip punctuation/whitespace and lowercase a token. Returns '' for empties."""
    if token is None:
        return ""
    # Normalize unicode, strip surrounding whitespace, drop non-alphanumeric edges
    t = unicodedata.normalize("NFKC", token).strip().lower()
    # Remove leading/trailing punctuation; keep internal apostrophes / hyphens
    t = re.sub(r"^[^a-z0-9]+", "", t)
    t = re.sub(r"[^a-z0-9]+$", "", t)
    return t


def main(input_path: str) -> None:
    spark = (
        SparkSession.builder
        .appName(f"CSE231_P1_20230945_word_count")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Read raw text file (one record per line by default for textFile) ---
    raw_rdd = spark.sparkContext.textFile(input_path)

    # --- Total number of LINES ---
    total_lines = raw_rdd.count()

    # --- Total number of WORDS (after tokenizing each line) ---
    #   Each line -> list of tokens -> flatMap -> count
    def line_to_tokens(line: str):
        if not line:
            return []
        # Split on any non-word character; preserve ASCII words and CJK char runs.
        # Use regex that keeps [a-zA-Z0-9'] sequences AND CJK characters as tokens.
        return re.findall(r"[A-Za-z0-9']+|[\u4e00-\u9fff]+", line)

    total_words = raw_rdd.flatMap(line_to_tokens).count()

    print("=" * 70)
    print(f"Project 1 - Word & Line Count  (Student ID: 20230945)")
    print(f"Input file: {input_path}")
    print("=" * 70)
    print(f"Total number of lines : {total_lines}")
    print(f"Total number of words : {total_words}")
    print("=" * 70)

    # --- Word frequency (descending) ---
    # Use DataFrame API for clarity: split lines into token arrays, explode, normalize.
    df = spark.read.text(input_path)

    # Replace punctuation with spaces, lowercase, then split on whitespace.
    # Keep ASCII words and CJK character runs intact.
    cleaned = (
        df.select(
            lower(regexp_replace(col("value"), r"[^\u4e00-\u9fffA-Za-z0-9'\s]", " ")).alias("line")
        )
        .filter(col("line").isNotNull() & (length(col("line")) > 0))
    )

    tokens_df = cleaned.select(explode(split(col("line"), r"\s+")).alias("token"))
    tokens_df = tokens_df.filter((col("token").isNotNull()) & (length(col("token")) > 0))

    freq_df = (
        tokens_df.groupBy("token")
        .count()
        .orderBy(col("count").desc(), col("token").asc())
    )

    print("Word frequencies (top 50, descending):")
    print("-" * 70)
    print(f"{'RANK':>5}  {'WORD':<30}  {'COUNT':>8}")
    print("-" * 70)

    rows = freq_df.limit(50).collect()
    for i, row in enumerate(rows, start=1):
        print(f"{i:>5}  {row['token']:<30}  {row['count']:>8}")

    print("=" * 70)
    print("Done.")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit 20230945_1.py <input_txt_file>")
        sys.exit(1)
    main(sys.argv[1])