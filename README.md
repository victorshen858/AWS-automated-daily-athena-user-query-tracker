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
| `S3_BUCKET`        | S3 bucket to store query mapping reports (auto-created by CloudFormation if it does not exist) |
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
s3://tracking-athena-usernames-logs/year=2025/month=08/day=28/hour=01/report_2025_08_28_h01.csv


## Deployment

- Use the **CloudFormation template** `tracking-athena-queries.yaml` to deploy:
  - Lambda function
  - Step Function Map
  - EventBridge daily scheduler
- Example deploy command:
aws cloudformation deploy \
    --template-file tracking-athena-queries.yaml \
    --stack-name tracking-athena-queries-stack \
    --capabilities CAPABILITY_NAMED_IAM

<img width="993" height="526" alt="image" src="https://github.com/user-attachments/assets/901eab3f-4081-4e47-b3f6-2eb4c167e99d" />


###Notes:

Lambda reads config.json either from S3 (preferred) or environment variables.

Step Function Map invokes the Lambda 24 times (once per hour).

EventBridge triggers Lambda daily to process the previous day automatically.

S3 stores results with full partitioning: year=YYYY/month=MM/day=DD/hour=HH.
### Configure environment variables to match your account setup.

Use Cases:

Audit and compliance: Track which users ran which Athena queries.

Management reporting: Map queries to workgroups and monitor usage.

Security: Ensure that no queries are being omitted or redacted for oversight.
