# Lambda function: FPL API -> S3 (Bronze layer)
# Runs on a schedule, downloads FPL data and saves raw JSON.

import json
import os 
import time
import urllib.request
from datetime import date
import boto3 
from botocore.exceptions import ClientError

# ---Settings ---
# The start of every FPL API address
BASE_URL = "https://fantasy.premierleague.com/api/"
# Bucket name comes from enviroment variablle s it is not hard coded
BUCKET = os.environ["BUCKET_NAME"]
SEASON = os.environ.get("SEASON", "2026-27")

# Create the S3 client once, outside the handler. Lambda reuses it
# between runs, which makes repeat runs faster.
s3 = boto3.client("s3")

def fetch_json(path):
    """Call an FPL API endpoint and return the JSON as a Python dict."""
     # Build the request. The User-Agent header says who is calling;
    # some APIs reject requests that don't have one
    request = urllib.request.Request(
        BASE_URL + path,
        headers={"User-Agent": "fpl-aws-pipeline"},
    )
    # "with" makes sure the connection closes when we're done.
    # timeout=30: give up after 30 seconds instead of hanging.
    with urllib.request.urlopen(request, timeout=30) as response:
        # Read the raw text and turn it into a Python dict
        return json.loads(response.read())

def key_exists(key):
    """Return true if the file is already in s3"""
    #head_object asks S3 about a file without downloading it.
    # If the file doesn't exist, S3 returns an error, so we
    # catch the error and return False instead of crashing.
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError:
        return False

def save_json(key, data):
    """Write a dict to S3 as a JSON file."""
    # "key" is the file's full path inside the bucket.
    # json.dumps turns the Python dict back into JSON text.
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )
    # print() output appears in CloudWatch Logs, useful for debugging
    print(f"Saved s3://{BUCKET}/{key}")

# lambda_handler is the function AWS calls when the Lambda runs.
# event: input data passed in (we don't need any here)
# context: info about the run, e.g. time remaining (not used here)
def lambda_handler(event, context):
    # Today's date as text, e.g. "2026-10-13"
    today = date.today().isoformat()

    # 1. Reference data: players, teams, gameweeks (snapshot every run)
    # We keep a dated copy each run because player prices and
    # teams change over the season, so we can track history later.
    bootstrap = fetch_json("bootstrap-static/")
    save_json(
        f"bronze/bootstrap/season={SEASON}/ingest_date={today}/bootstrap.json",
        bootstrap,
    )

    # 2. All fixtures for the season (snapshot every run)
    # Scores and kick-off times change, so we keep dated copies too.
    fixtures = fetch_json("fixtures/")
    save_json(
        f"bronze/fixtures/season={SEASON}/ingest_date={today}/fixtures.json",
        fixtures,
    )

    # 3. Player stats for each finished gameweek not already saved
    # List comprehension: build a list of gameweek IDs where the
    # gameweek is finished AND FPL has confirmed the final points.
    finished = [
        e["id"] for e in bootstrap["events"]
        if e["finished"] and e["data_checked"]
    ]
    # Keep track of what we saved this run, for the summary
    new_gameweeks = []
    for gw in finished:
        # {gw:02d} pads to 2 digits: 3 -> "03", so folders sort properly.
        # "season=" and "gameweek=" make Hive-style partitions that
        # Glue turns into columns automatically.
        key = f"bronze/gameweek_live/season={SEASON}/gameweek={gw:02d}/live.json"
        # Idempotency: skip gameweeks we already have, so reruns
        # never create duplicates or waste API calls.
        if key_exists(key):
            continue
        save_json(key, fetch_json(f"event/{gw}/live/"))
        new_gameweeks.append(gw)
        # Wait 1 second between calls to be polite to the API
        time.sleep(1)

    # Whatever we return shows up as the result in the Lambda console
    return {
        "finished_gameweeks": len(finished),
        "new_gameweeks_saved": new_gameweeks,
    }
