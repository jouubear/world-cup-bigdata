CSE 231 Final Project — Deliverables
Student ID: 20230945
Generated: 2026-07-03

============================================================
PROJECT 1 — Spark Word Count (50 pts, 25% of course grade)
============================================================

Files:
  20230945.txt           — essay (>=200 words) covering all required topics
  20230945_1.py          — PySpark word + line counter, descending frequency
  20230945_11.jpg        — TODO: screenshot of console output (capture on your env)
  20230945_12.jpg/png    — TODO: photo showing your computer/VM logo and Spark env

Run:
  spark-submit 20230945_1.py 20230945.txt
  # or
  python3 20230945_1.py 20230945.txt

Sample run (verified locally, PySpark 4.1):
  Total number of lines : 11
  Total number of words : 482
  Top word: "and" (22), then "the" (20), "a" (13), ...

============================================================
PROJECT 2 — Real-time World Cup Tweet Analytics (50 pts)
============================================================

Files:
  generate_tweets_by_id.py            — produces one timestamped CSV in /data/incoming/
  20230945_2.py                       — Spark Streaming consumer of /data/incoming/
  20230945_<unix_timestamp>.csv       — the actual generated tweet file
  20230945_21.jpg/png                 — TODO: screenshot of streaming output
  20230945_22.jpg/png                 — TODO: photo showing your Spark env

Step 1 — Generate the CSV (writes to /data/incoming/ per brief):
  python3 generate_tweets_by_id.py --rows 300 \
        --outdir /data/incoming --student-id 20230945
  # filename becomes: 20230945_<unix_timestamp>.csv   e.g. 20230945_1783012265.csv

Step 2 — Start the streaming analyzer (in a separate terminal):
  spark-submit 20230945_2.py --watch-dir /data/incoming --trigger-seconds 10
  # or
  python3 20230945_2.py --watch-dir /data/incoming --trigger-seconds 10

Optional: to demonstrate streaming, you can run the generator again to
drop a SECOND CSV into /data/incoming/ while the analyzer is running;
you will see the cumulative ranking grow with each micro-batch.

The streaming analyzer:
  - Reads new files from /data/incoming/ as micro-batches
  - Extracts hashtags of all 48 qualified FIFA teams from each tweet
  - Maintains a cumulative country-by-team ranking across micro-batches
  - Prints the current ranking to the console every 10s (configurable)

============================================================
ENVIRONMENT NOTES
============================================================
- Tested on: Python 3.11, PySpark 4.1.2, OpenJDK 17
- On the dolphin lab platform (https://labs.cjlu.dolphin-labs.com/)
  pyspark and Java should already be available; if not:
      pip install pyspark
      # Java: use the platform-provided JDK
- For "own computer environment" bonus, ensure Spark + Java are
  installed and JAVA_HOME is exported.

============================================================
WHAT YOU STILL NEED TO DO MANUALLY
============================================================
The 4 image deliverables require YOUR environment:
  1. 20230945_11.jpg/png  — screenshot of `20230945_1.py` console output
  2. 20230945_12.jpg/png  — photo of your computer/VM screen + Spark env (Project 1)
  3. 20230945_21.jpg/png  — screenshot of streaming ranking output
  4. 20230945_22.jpg/png  — photo of your computer/VM screen + Spark env (Project 2)

Steps to capture each:
  - Run the corresponding command (see above)
  - Take a clear screenshot of the console
  - For 12/22, make sure the photo shows the platform/VM logo so the
    grader can tell whether it's the dolphin lab or your own machine
    (own machine = bonus points).