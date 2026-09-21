# -*- coding: utf-8 -*-
"""
generate_tweets_by_id.py
CSE 231 Final Project - Project 2
Student ID: 20230945

Generates ONE timestamped CSV file containing simulated 2026 FIFA World Cup
tweets. Writes to /data/incoming/ (the shared directory specified in the
project brief) using a filename of the form:

    <student_id>_<unix_timestamp>.csv
e.g. 20230945_1718659800.csv

Each row is: id,timestamp,user,text,country

Usage:
    python3 generate_tweets_by_id.py [--rows N] [--outdir /data/incoming]
                                      [--student-id 20230945]

Requirements: standard library only.
"""

import argparse
import csv
import os
import random
import time
from datetime import datetime, timezone


# ---------- 48 qualified teams for the 2026 FIFA World Cup ----------
# Format: (display_name, lowercase hashtag, ISO-3166 alpha-2)
TEAMS_48 = [
    ("Argentina",      "argentina",      "AR"),
    ("Australia",      "australia",      "AU"),
    ("Belgium",        "belgium",        "BE"),
    ("Brazil",         "brazil",         "BR"),
    ("Cameroon",       "cameroon",       "CM"),
    ("Canada",         "canada",         "CA"),
    ("Chile",          "chile",          "CL"),
    ("Colombia",       "colombia",       "CO"),
    ("Croatia",        "croatia",        "HR"),
    ("Denmark",        "denmark",        "DK"),
    ("Ecuador",        "ecuador",        "EC"),
    ("Egypt",          "egypt",          "EG"),
    ("England",        "england",        "GB"),
    ("France",         "france",         "FR"),
    ("Germany",        "germany",        "DE"),
    ("Ghana",          "ghana",          "GH"),
    ("Iran",           "iran",           "IR"),
    ("Ireland",        "ireland",        "IE"),
    ("Italy",          "italy",          "IT"),
    ("Ivory Coast",    "ivorycoast",     "CI"),
    ("Jamaica",        "jamaica",        "JM"),
    ("Japan",          "japan",          "JP"),
    ("Jordan",         "jordan",         "JO"),
    ("Mexico",         "mexico",         "MX"),
    ("Morocco",        "morocco",        "MA"),
    ("Netherlands",    "netherlands",    "NL"),
    ("New Zealand",    "newzealand",     "NZ"),
    ("Nigeria",        "nigeria",        "NG"),
    ("Norway",         "norway",         "NO"),
    ("Panama",         "panama",         "PA"),
    ("Paraguay",       "paraguay",       "PY"),
    ("Peru",           "peru",           "PE"),
    ("Poland",         "poland",         "PL"),
    ("Portugal",       "portugal",       "PT"),
    ("Qatar",          "qatar",          "QA"),
    ("Saudi Arabia",   "saudiarabia",    "SA"),
    ("Scotland",       "scotland",       "GB"),
    ("Senegal",        "senegal",        "SN"),
    ("Serbia",         "serbia",         "RS"),
    ("South Korea",    "southkorea",     "KR"),
    ("Spain",          "spain",          "ES"),
    ("Sweden",         "sweden",         "SE"),
    ("Switzerland",    "switzerland",    "CH"),
    ("Tunisia",        "tunisia",        "TN"),
    ("Turkey",         "turkey",         "TR"),
    ("Ukraine",        "ukraine",        "UA"),
    ("United States",  "unitedstates",   "US"),
    ("Uruguay",        "uruguay",        "UY"),
]

# ---------- Tweet templates ----------
SENTIMENT_TEMPLATES = {
    "praise": [
        "{team} looked absolutely world class tonight! #WorldCup2026 #{tag}",
        "What a performance from {team}, take a bow. #FIFAWorldCup #{tag}",
        "I'm so proud of {team}, this squad is something special. #{tag}",
        "{team} are the real deal, no debate. #WorldCup2026 #{tag}",
    ],
    "criticism": [
        "{team} need to sort their defence out fast. #WorldCup2026 #{tag}",
        "Honestly, {team} were poor today, no excuses. #{tag}",
        "{team} looked tired and slow, big problems ahead. #WorldCup2026 #{tag}",
    ],
    "neutral": [
        "{team} vs opponent later — should be a tight match. #WorldCup2026 #{tag}",
        "Watching {team} train, the vibes are good. #{tag}",
        "{team} arriving at the stadium, crowd going wild. #FIFAWorldCup #{tag}",
        "Post-match interview with the {team} coach just dropped. #{tag}",
        "Fan zone is packed ahead of the {team} game! #WorldCup2026 #{tag}",
    ],
}

USERNAME_POOL = [
    "fan_2026", "goal_hunter", "pitch_side", "soccer_geek", "striker_99",
    "midfield_maestro", "keeper_legend", "ultras_worldcup", "matchday_",
    "kit_collector", "tifo_maker", "var_facts", "fifty_fifty", "red_card_daily",
    "world_cup_diary", "touchline_", "kitbag_", "football_lab",
]


def make_tweet(rng: random.Random, ts_ms: int, tweet_id: int) -> dict:
    """Generate one synthetic tweet referencing a randomly chosen team."""
    display, tag, country = rng.choice(TEAMS_48)
    sentiment = rng.choices(
        list(SENTIMENT_TEMPLATES.keys()),
        weights=[0.55, 0.15, 0.30],
        k=1,
    )[0]
    template = rng.choice(SENTIMENT_TEMPLATES[sentiment])
    text = template.format(team=display, tag=tag)
    user = rng.choice(USERNAME_POOL) + str(rng.randint(10, 9999))
    iso_ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
    return {
        "id": tweet_id,
        "timestamp": iso_ts,
        "user": user,
        "text": text,
        "country": country,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate 2026 World Cup tweet CSV.")
    parser.add_argument("--rows", type=int, default=300,
                        help="Number of tweet rows to generate (default: 300).")
    parser.add_argument("--outdir", default="/data/incoming",
                        help="Output directory (default: /data/incoming).")
    parser.add_argument("--student-id", default="20230945",
                        help="Student ID used in filename (default: 20230945).")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for reproducibility.")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    os.makedirs(args.outdir, exist_ok=True)

    unix_ts = int(time.time())
    filename = f"{args.student_id}_{unix_ts}.csv"
    out_path = os.path.join(args.outdir, filename)

    base_ms = int(time.time() * 1000)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["id", "timestamp", "user", "text", "country"]
        )
        writer.writeheader()
        for i in range(1, args.rows + 1):
            ts_ms = base_ms - rng.randint(0, 60 * 60 * 1000)  # last hour
            writer.writerow(make_tweet(rng, ts_ms, tweet_id=i))

    print(f"[generate_tweets_by_id] wrote {args.rows} rows -> {out_path}")
    print(f"[generate_tweets_by_id] filename: {filename}")


if __name__ == "__main__":
    main()