# Monitoring and Alerting

## Objective

This document describes how monitoring and alerting should be implemented for this pipeline, with attention to:

- data quality issues
- pipeline failures
- unexpected source behavior
- operational visibility

This combines what is already implemented in the project with what should be added in a production-ready evolution.

## What Is Already Implemented

### Structured logging

The project includes centralized logging configuration and task-level logging during execution.

Current signals available:

- Bronze page extraction logs
- command-level output summaries
- Spark and Airflow runtime logs

### Request metadata capture

Bronze request logs preserve:

- request URL
- request parameters
- response headers
- status code
- request timestamp
- response time
- record counts

This is useful for:

- debugging API failures
- validating pagination behavior
- reviewing rate-limit headers

### Data quality validations

Silver validations:

- positive row count
- unique ids
- non-null ids
- non-blank `brewery_type`

Gold validations:

- positive row count
- non-negative `brewery_count`
- exclusion of `closed` breweries
- non-null location fields

### Airflow task visibility

The DAG breaks the pipeline into three tasks:

1. `bronze_ingestion`
2. `silver_transformation`
3. `gold_aggregation`

This already provides:

- task-level success/failure visibility
- retry behavior
- execution history

## Recommended Monitoring Dimensions

### 1. Pipeline health

Questions to monitor:

- Did the DAG run complete successfully?
- Which task failed?
- How long did each task take?
- Did retries happen?

Useful metrics:

- DAG success/failure count
- task duration
- retry count
- task failure count

### 2. Source API health

Questions to monitor:

- Is the API responding successfully?
- Are response times increasing?
- Are rate limits being approached?
- Is the metadata endpoint reporting unexpected totals?

Useful metrics:

- HTTP status code counts
- extraction duration
- page count extracted
- `x-ratelimit-limit`
- `x-ratelimit-remaining`

### 3. Data quality

Questions to monitor:

- Did Silver or Gold validation fail?
- Did row counts drop unexpectedly?
- Are required fields suddenly null?
- Are duplicates appearing in Silver?

Useful metrics:

- Silver row count
- Gold row count
- duplicate id count
- null required-field count
- failed validation count

### 4. Business-facing output quality

Questions to monitor:

- Did Gold produce data?
- Are `closed` breweries excluded as expected?
- Are counts unexpectedly low or high compared to prior runs?

Useful metrics:

- Gold row count
- total distinct active breweries
- number of rows with `brewery_type = 'closed'` in Gold
- trend of active brewery counts over time

## Alerting Strategy

### High-priority alerts

Trigger alerts when:

- the DAG fails
- Bronze extraction cannot reach the API
- Silver validation fails
- Gold validation fails
- Gold produces zero rows

Suggested channels:

- Slack
- email
- PagerDuty in a production environment

### Medium-priority alerts

Trigger alerts when:

- retries exceed a threshold
- API response time increases significantly
- row counts deviate materially from recent runs
- rate-limit remaining values become too low

### Low-priority alerts

Trigger alerts when:

- schema changes are detected
- optional descriptive fields show rising null rates

## Production-Ready Tooling Recommendation

### Metrics

Recommended:

- Prometheus
- StatsD

### Dashboards

Recommended:

- Grafana

### Alert delivery

Recommended:

- Slack for team notifications
- email for operational summaries
- PagerDuty for critical incidents in production

## Example Alert Conditions

### Bronze extraction failure

Condition:

- task failure in `bronze_ingestion`

Alert message example:

```text
Bronze ingestion failed. Check API availability, request logs, and Airflow task logs.
```

### Silver validation failure

Condition:

- duplicate ids detected
- null ids detected
- row count is zero

Alert message example:

```text
Silver validation failed. Curated layer was not published because one or more quality checks failed.
```

### Gold validation failure

Condition:

- `closed` breweries appear in Gold
- Gold output is empty
- location fields are null

Alert message example:

```text
Gold validation failed. Active brewery analytical output is inconsistent with expected business rules.
```

### Abnormal record-count drop

Condition:

- Bronze, Silver, or Gold row count drops significantly compared to recent successful runs

Alert message example:

```text
Record-count anomaly detected. Current pipeline output is materially lower than the recent baseline.
```

## Suggested Next Implementation Step

If this project were extended further, the next monitoring enhancement would be:

1. persist pipeline run summaries as structured JSON
2. emit metrics to StatsD or Prometheus
3. add Airflow alert callbacks for task failures
4. add a dashboard tracking Bronze, Silver, and Gold health over time
