import boto3
import json
import io
import csv
import logging
import time
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import botocore.exceptions
import os

# AWS clients
s3 = boto3.client("s3")
cloudtrail = boto3.client("cloudtrail")
athena = boto3.client("athena")
stepfunctions = boto3.client("stepfunctions")

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
S3_BUCKET = os.environ.get("S3_BUCKET", "tracking-athena-usernames-logs")
OUTPUT_TYPE = os.environ.get("OUTPUT_TYPE", "csv").lower()
TEST_START_DATE = os.environ.get("TEST_START_DATE")
TEST_END_DATE = os.environ.get("TEST_END_DATE")
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")

TIMEZONE = ZoneInfo("America/New_York")
STATE_FILE_KEY_TEMPLATE = "hourly-state/{date}.json"


# ----------------------------
# CloudTrail helper
# ----------------------------
def cloudtrail_lookup_with_backoff(params):
    delay = 1
    for attempt in range(6):
        try:
            return cloudtrail.lookup_events(**params)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ThrottlingException":
                jitter = random.uniform(0, delay)
                logger.warning(f"Throttled (attempt {attempt+1}), retrying in {delay + jitter:.1f}s")
                time.sleep(delay + jitter)
                delay *= 2
                continue
            raise
    raise RuntimeError("Exceeded max retries for CloudTrail lookup")


# ----------------------------
# Extract Athena query details
# ----------------------------
def extract_event_details(event):
    try:
        detail = json.loads(event.get("CloudTrailEvent", "{}"))
        if detail.get("eventName") != "StartQueryExecution":
            return None

        username = event.get("Username") or event.get("UserIdentity", {}).get("UserName", "unknown")
        start_time = event.get("EventTime")
        if not isinstance(start_time, datetime):
            start_time = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        start_est = start_time.astimezone(TIMEZONE).isoformat()

        query_id = (
            detail.get("responseElements", {}).get("queryExecutionId")
            or detail.get("requestParameters", {}).get("queryExecutionId")
        )

        end_est = "N/A"
        query_text = "REDACTED"
        workgroup = "unknown"

        if query_id:
            try:
                res = athena.get_query_execution(QueryExecutionId=query_id)
                comp = res.get("QueryExecution", {}).get("Status", {}).get("CompletionDateTime")
                if comp:
                    comp_dt = comp if isinstance(comp, datetime) else datetime.fromisoformat(comp.replace("Z", "+00:00"))
                    end_est = comp_dt.astimezone(TIMEZONE).isoformat()
                qstr = res.get("QueryExecution", {}).get("Query")
                if qstr:
                    query_text = qstr.strip()
                wg = res.get("QueryExecution", {}).get("WorkGroup")
                if wg:
                    workgroup = wg
            except Exception as e:
                logger.warning(f"Error fetching query execution {query_id}: {e}")

        return [username, start_est, end_est, query_text, query_id or "N/A", workgroup]

    except Exception as e:
        logger.error(f"Failed to extract event details: {e}")
        return None


# ----------------------------
# Write output to S3
# ----------------------------
def write_s3(report_date, hour, records):
    suffix = report_date.strftime("%Y_%m_%d")
    key_prefix = f"reports/year={report_date.year}/month={report_date.strftime('%m')}/day={report_date.strftime('%d')}/hour={hour}"
    key = f"{key_prefix}/report_{suffix}_h{hour}.{OUTPUT_TYPE}"

    if OUTPUT_TYPE == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            ["LambdaRunTime(EST)", "Username", "QueryStartTime(EST)", "QueryEndTime(EST)",
             "Query", "QueryExecutionId", "WorkGroup"]
        )
        run_time = datetime.now(timezone.utc).astimezone(TIMEZONE).isoformat()
        for rec in records:
            writer.writerow([run_time, *rec])
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=output.getvalue())
    else:  # JSON
        payload = [{"lambda_runtime": datetime.now(timezone.utc).astimezone(TIMEZONE).isoformat(),
                    "username": r[0],
                    "start_time": r[1],
                    "end_time": r[2],
                    "query": r[3],
                    "query_execution_id": r[4],
                    "workgroup": r[5]} for r in records]
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(payload, indent=2))


# ----------------------------
# Process one hour
# ----------------------------
def process_hour(report_date, hour):
    start_est = report_date.replace(hour=hour, minute=0, second=0, microsecond=0)
    end_est = start_est + timedelta(hours=1)
    start_utc = start_est.astimezone(timezone.utc)
    end_utc = end_est.astimezone(timezone.utc)

    events = []
    next_token = None
    while True:
        params = {"StartTime": start_utc, "EndTime": end_utc, "MaxResults": 50}
        if next_token:
            params["NextToken"] = next_token
        resp = cloudtrail_lookup_with_backoff(params)
        events.extend(resp.get("Events", []))
        next_token = resp.get("NextToken")
        if not next_token:
            break

    records = [r for e in events if (r := extract_event_details(e))]
    write_s3(report_date, hour, records)

    logger.info(f"Processed hour {hour}: {len(records)} Athena queries found")
    return len(records)


# ----------------------------
# Lambda entry point
# ----------------------------
def lambda_handler(event, context):
    logger.info(f"Event received: {json.dumps(event)}")

    # Determine date range
    if TEST_START_DATE and TEST_END_DATE:
        start_date = datetime.strptime(TEST_START_DATE, "%Y-%m-%d").replace(tzinfo=TIMEZONE)
        end_date = datetime.strptime(TEST_END_DATE, "%Y-%m-%d").replace(tzinfo=TIMEZONE)
    else:
        # Default: previous day
        yesterday = datetime.now(TIMEZONE) - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date

    delta = (end_date - start_date).days + 1
    for day_offset in range(delta):
        report_date = start_date + timedelta(days=day_offset)
        hours = [{"hour": h, "report_date": report_date.strftime("%Y/%m/%d")} for h in range(24)]
        payload = {"hours": hours}
        if STATE_MACHINE_ARN:
            response = stepfunctions.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                input=json.dumps(payload)
            )
            logger.info(f"Triggered Step Function for {report_date.strftime('%Y-%m-%d')}: {response['executionArn']}")
        else:
            # Direct per-hour processing if Step Function not provided
            for h in range(24):
                process_hour(report_date, h)

    return {"status": "completed"}
