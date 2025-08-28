# AWS-automated-daily-athena-user-query-tracker
Track and map all Athena queries to usernames and workgroups using CloudTrail, Lambda, Step Functions, EventBridge scheduler cron jobs with Athena partitioned S3 output and automated daily or backfill processing.

# Tracking and Mapping Usernames to Athena Run Queries and Workgroups

## Purpose
This Lambda workflow tracks all Athena queries executed in the account and maps them to the exact usernames, including query execution IDs. 

Due to default Athena and CloudTrail behavior, query strings are usually redacted in logs. This workflow ensures **exact mapping** of each user to every query run, without omission or redaction.

## Key Features

- Uses **CloudTrail LookupEvents API** to fetch the last 90 days of Athena query events (`StartQueryExecution`).
- Extracts username, query execution ID, workgroup, start/end times, and full query text (if available).
- Writes output to **S3**, partitioned by:
  - `year=YYYY`
  - `month=MM`
  - `day=DD`
  - `hour=HH`
- Supports output formats:
  - CSV (`OUTPUT_TYPE=csv`)
  - JSON (`OUTPUT_TYPE=json`)
- Can run for a single day or a **range of dates** for testing/backfill via `TEST_START_DATE` and `TEST_END_DATE`.
- Compatible with a **Step Function Map state** that processes all 24 hours of a day in parallel.
- Supports a **daily EventBridge trigger** to automatically process the previous day’s events.

## Environment Variables

| Name               | Description |
|-------------------|-------------|
| `S3_BUCKET`        | S3 bucket to store query mapping reports |
| `OUTPUT_TYPE`      | Output format (`csv` or `json`) |
| `TEST_START_DATE`  | Optional start date for backfill (YYYY-MM-DD) |
| `TEST_END_DATE`    | Optional end date for backfill (YYYY-MM-DD) |
| `STATE_MACHINE_ARN`| Step Function ARN to run hourly Map execution |

## Workflow

1. **Daily automatic run**:
   - EventBridge triggers Lambda at 01:00 ET.
   - Lambda calculates the previous day.
   - Lambda triggers the Step Function to process all 24 hours in parallel.
2. **Manual/backfill run**:
   - Provide `TEST_START_DATE` and `TEST_END_DATE` in environment variables.
   - Lambda triggers Step Function executions for all dates in range.
3. **Hourly processing**:
   - Lambda queries CloudTrail LookupEvents for Athena `StartQueryExecution`.
   - Extracts relevant details.
   - Writes results to **S3**, partitioned by date and hour.

## S3 Partition Example

