import boto3
import json
import io
import csv
import time
import logging
import random
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import botocore.exceptions
import os

# -----------------------
# Logging setup
# -----------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -----------------------
# Load configuration
# -----------------------
s3 = boto3.client('s3')

config = {
    "AWS_ACCOUNT_ID": os.environ.get("AWS_ACCOUNT_ID"),
    "LAMBDA_NAME": os.environ.get("LAMBDA_NAME"),
    "STATE_MACHINE": os.environ.get("STATE_MACHINE"),
    "S3_BUCKET": os.environ.get("S3_BUCKET"),
    "EVENTBRIDGE_NAME": os.environ.get("EVENTBRIDGE_NAME"),
    "OUTPUT_TYPE": os.environ.get("OUTPUT_TYPE", "csv"),
    "TEST_START_DATE": os.environ.get("TEST_START_DATE"),
    "TEST_END_DATE": os.environ.get("TEST_END_DATE"),
}

# If key fields missing, attempt to read config.json from S3
if not all([config["AWS_ACCOUNT_ID"], config["LAMBDA_NAME"], config["S3_BUCKET"]]):
    CONFIG_BUCKET = os.environ.get("CONFIG_BUCKET")  # Set via env var
    CONFIG_KEY = os.environ.get("CONFIG_KEY", "config.json")
    if CONFIG_BUCKET:
        try:
            obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=CONFIG_KEY)
            s3_config = json.loads(obj['Body'].read())
            config.update(s3_config)
            logger.info("Loaded configuration from S3 config.json")
        except Exception as e:
            logger.error(f"Failed to load config.json from S3: {e}")

# -----------------------
# AWS clients
# -----------------------
cloudtrail = boto3.client('cloudtrail')
athena = boto3.client('athena')
stepfunctions = boto3.client('stepfunctions')

TIMEZONE_EST = ZoneInfo("America/New_York")
STATE_FILE_KEY_TEMPLATE = "hourly-state/{date}.json"

# -----------------------
# Helper functions
# -----------------------
def s3_object_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise

def load_state(report_date):
    key = STATE_FILE_KEY_TEMPLATE.format(date=report_date.strftime("%Y_%m_%d"))
    if not s3_object_exists(config["S3_BUCKET"], key):
        return {"processed_hours": [], "status": "in_progress"}, key
    resp = s3.get_object(Bucket=config["S3_BUCKET"], Key=key)
    return json.loads(resp['Body'].read().decode('utf-8')), key

def save_state(report_date, processed_hours, key):
    state = {
        "processed_hours": processed_hours,
        "status": "completed" if len(processed_hours) >= 24 else "in_progress"
    }
    s3.put_object(
        Bucket=config["S3_BUCKET"],
        Key=key,
        Body=json.dumps(state),
        ContentType="application/json"
    )

def cloudtrail_lookup_with_backoff(params):
    delay = 1
    for attempt in range(6):
        try:
            return cloudtrail.lookup_events(**params)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException':
                jitter = random.uniform(0, delay)
                logger.warning(f"Throttled (attempt {attempt+1}), retrying in {delay+jitter:.1f}s...")
                time.sleep(delay + jitter)
                delay *= 2
                continue
            raise
    raise RuntimeError("Exceeded max retries")

def extract_event_details(event):
    try:
        detail = json.loads(event.get('CloudTrailEvent', '{}'))
        if detail.get('eventName') != 'StartQueryExecution':
            return None
        username = event.get('Username') or event.get('UserIdentity', {}).get('UserName', 'unknown')
        start_time = event.get('EventTime')
        if not isinstance(start_time, datetime):
            start_time = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        start_est = start_time.astimezone(TIMEZONE_EST).isoformat()

        query_id = detail.get('responseElements', {}).get('queryExecutionId') or \
                   detail.get('requestParameters', {}).get('queryExecutionId')
        end_est = "N/A"
        query_text = "REDACTED"
        athena_workgroup = "unknown"

        if query_id:
            try:
                res = athena.get_query_execution(QueryExecutionId=query_id)
                comp = res.get('QueryExecution', {}).get('Status', {}).get('CompletionDateTime')
                if comp:
                    comp_dt = comp if isinstance(comp, datetime) else datetime.fromisoformat(comp.replace("Z", "+00:00"))
                    end_est = comp_dt.astimezone(TIMEZONE_EST).isoformat()
                qstr = res.get('QueryExecution', {}).get('Query')
                if qstr:
                    query_text = qstr.strip()
                wg = res.get('QueryExecution', {}).get('WorkGroup')
                if wg:
                    athena_workgroup = wg
            except Exception as e:
                logger.warning(f"Error fetching query execution for {query_id}: {e}")
        return [username, start_est, end_est, query_text, query_id or "N/A", athena_workgroup]
    except Exception as e:
        logger.error(f"Failed to extract event details: {e}")
        return None

def write_csv(report_date, hour, records):
    suffix = report_date.strftime("%Y_%m_%d")
    key = (
        f"daily-report/year={report_date.year}/month={report_date.strftime('%m')}/"
        f"day={report_date.strftime('%d')}/hour={hour}/report_{suffix}_h{hour}.csv"
    )
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["LambdaRunTime(EST)", "Username", "QueryStartTime(EST)", "QueryEndTime(EST)",
                     "Query", "QueryExecutionId", "Athena_Workgroup"])
    run_time = datetime.now(timezone.utc).astimezone(TIMEZONE_EST).isoformat()
    for rec in records:
        writer.writerow([run_time, *rec])
    s3.put_object(Bucket=config["S3_BUCKET"], Key=key, Body=output.getvalue())

def process_hour(report_date, hour, context):
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
    write_csv(report_date, hour, records)
    return len(records), len(events)

# -----------------------
# Lambda Handler
# -----------------------
def lambda_handler(event, context):
    logger.info(f"Event received: {json.dumps(event)}")

    # Daily EventBridge trigger (full-day run)
    if event.get("source") == "aws.events":
        yesterday = datetime.now(TIMEZONE_EST) - timedelta(days=1)
        date_str = yesterday.strftime("%Y/%m/%d")
        hours = [{"hour": h, "report_date": date_str} for h in range(24)]

        payload = {"hours": hours}
        response = stepfunctions.start_execution(
            stateMachineArn=config["STATE_MACHINE"],
            input=json.dumps(payload)
        )
        logger.info(f"Triggered Step Function: {response['executionArn']}")
        return {"status": "triggered", "execution": response['executionArn']}

    # Manual/backfill run (hourly)
    report_date_str = event.get("report_date") or config.get("TEST_START_DATE")
    hour = event.get("hour")
    if not report_date_str or hour is None:
        return {"error": "Missing 'report_date' or 'hour'. Needed for per-hour processing."}

    report_date = datetime.strptime(report_date_str, "%Y/%m/%d").replace(tzinfo=TIMEZONE_EST)
    records_written, events_fetched = process_hour(report_date, int(hour), context)

    logger.info(
        f"Processed hour {hour} on {report_date_str}: "
        f"events_fetched={events_fetched}, records_written={records_written}"
    )
    return {
        "status": "processed",
        "hour": int(hour),
        "records_written": records_written,
        "events_fetched": events_fetched
    }
