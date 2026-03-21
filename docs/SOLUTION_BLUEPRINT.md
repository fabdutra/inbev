# Senior Data Engineer Technical Assessment Solution Blueprint

## 1. Objective

Design and implement a data pipeline that consumes brewery data from the Open Brewery DB API, persists it in a medallion-style data lake, and exposes an analytical aggregation of brewery counts by type and location.

This document defines the target solution before implementation so the repository evolves from a clear architecture instead of ad hoc coding.

## 2. Assessment Requirements Consolidated

### From the PDF

- Consume brewery data from the Open Brewery DB API.
- Build an orchestrated pipeline with scheduling, retries, and error handling.
- Prefer Python and PySpark.
- Include automated tests.
- Follow medallion architecture:
  - Bronze: raw API data.
  - Silver: curated columnar data, partitioned by location.
  - Gold: aggregated brewery counts by type and location.
- Describe monitoring and alerting.
- Document design choices, trade-offs, and execution steps.
- Containerization gives extra points.

### From recruiter guidance

- Implement API pagination.
- Use data partitioning for performance and scalability.
- Validate data integrity before storage.
- Create automated tests.
- Choose a scalable architecture.
- Implement robust error handling.
- Follow Git and documentation best practices.
- Deliver something practical and maintainable.

### Additional constraints and preferences

- Do not use pandas.
- Use PySpark for transformation logic.
- Extract and persist API metadata in addition to brewery records.
- Write modular, object-oriented code.
- Keep documentation strong across all project sections.

## 3. External API Notes

Based on the current Open Brewery DB documentation and live API behavior on March 21, 2026:

- Main list endpoint: `https://api.openbrewerydb.org/v1/breweries`
- Metadata endpoint: `https://api.openbrewerydb.org/v1/breweries/meta`
- Pagination parameters:
  - `page`
  - `per_page`
  - max `per_page=200`
- Metadata endpoint currently returns:
  - `total`
  - grouped counts such as `by_state` and `by_type`
  - `page`
  - `per_page`
- Rate-limit headers are exposed, including values such as:
  - `x-ratelimit-limit`
  - `x-ratelimit-remaining`

Implication for the solution:

- We should use `/meta` first to discover the total record count and compute the number of pages to ingest.
- We should persist both payload metadata and HTTP/request metadata for observability and replay support.

## 4. Proposed Solution Summary

### Stack

- Language: Python 3.11
- Distributed processing: PySpark
- Orchestration: Apache Airflow
- Storage format:
  - Bronze: JSON
  - Silver: Delta
  - Gold: Delta
- Containerization: Docker Compose
- Testing: `pytest`
- Quality tooling:
  - `ruff`
  - `black`
  - optional `mypy`

### Why this stack

- PySpark aligns with the requested tooling and demonstrates large-scale data engineering patterns.
- Airflow is familiar, production-relevant, and lets us show scheduling, retries, task dependencies, and operational maturity.
- Docker Compose makes the project reproducible and earns extra points for containerization without adding Kubernetes complexity.
- Delta in Silver improves support for deduplication, idempotent reruns, upserts, schema evolution, and current-state curation.
- Delta in Gold adds transactional guarantees and better support for downstream analytical consumption, schema evolution, and incremental improvements.

## 5. Architectural Principles

- Prefer clear module boundaries over notebooks or script sprawl.
- Make ingestion idempotent and replayable.
- Separate API extraction concerns from Spark transformation concerns.
- Persist raw evidence before applying transformations.
- Capture technical metadata for traceability.
- Optimize for maintainability, observability, and testability.
- Keep local execution simple while leaving room for cloud evolution.

## 6. High-Level Architecture

```text
Open Brewery DB API
    |
    v
Airflow DAG
    |
    +--> Task 1: fetch ingestion metadata from /meta
    |
    +--> Task 2: compute page list to ingest
    |
    +--> Task 3: extract each page from /breweries
    |         - capture payload + request metadata + response headers
    |         - write Bronze raw files
    |
    +--> Task 4: run Spark Bronze -> Silver transformation
    |         - schema enforcement
    |         - deduplication
    |         - null handling / normalization
    |         - partitioned parquet output
    |
    +--> Task 5: run Spark Silver -> Gold aggregation
    |         - brewery counts by type and location
    |
    +--> Task 6: data quality checks + run summary
    |
    +--> Task 7: publish metrics / alerts
```

## 7. Repository Structure

```text
inbev/
├── README.md
├── docs/
│   ├── DE_Case_Atualizado.pdf
│   ├── SOLUTION_BLUEPRINT.md
│   ├── architecture.md
│   ├── data_model.md
│   └── runbook.md
├── src/
│   ├── config/
│   │   ├── settings.py
│   │   └── schemas.py
│   ├── domain/
│   │   ├── models.py
│   │   └── enums.py
│   ├── ingestion/
│   │   ├── api_client.py
│   │   ├── metadata_client.py
│   │   ├── paginator.py
│   │   ├── extractor.py
│   │   └── bronze_writer.py
│   ├── processing/
│   │   ├── spark_session_factory.py
│   │   ├── bronze_to_silver.py
│   │   ├── silver_to_gold.py
│   │   └── validators.py
│   ├── orchestration/
│   │   └── airflow/
│   │       └── dags/
│   │           └── breweries_pipeline.py
│   ├── observability/
│   │   ├── logging.py
│   │   ├── metrics.py
│   │   └── alerts.py
│   └── utils/
│       ├── filesystem.py
│       ├── clock.py
│       └── ids.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── requirements.txt
└── Makefile
```

Notes:

- `docs/architecture.md`, `docs/data_model.md`, and `docs/runbook.md` can be added after implementation starts.
- The final structure may be adjusted slightly, but we should preserve this modular separation.

## 8. Data Lake Design

### Bronze layer

Purpose:

- Preserve the raw API response exactly as received.
- Preserve ingestion metadata needed for auditability, replay, and debugging.

Recommended storage layout:

```text
data/bronze/openbrewerydb/breweries/extract_date=YYYY-MM-DD/run_id=<run_id>/page=<page>.json
data/bronze/openbrewerydb/metadata/extract_date=YYYY-MM-DD/run_id=<run_id>/meta.json
data/bronze/openbrewerydb/request_logs/extract_date=YYYY-MM-DD/run_id=<run_id>/page=<page>.json
```

What each raw page file should contain:

- Raw array returned by `/v1/breweries`

What each request log file should contain:

- `run_id`
- `endpoint`
- `request_url`
- `request_params`
- `page`
- `per_page`
- `request_timestamp_utc`
- `response_status_code`
- `response_headers`
- `record_count_in_page`
- `api_reported_total` when available

What `meta.json` should contain:

- Raw `/v1/breweries/meta` response
- Request metadata
- Ingestion timestamp

### Silver layer

Purpose:

- Standardize records into an analytics-friendly, strongly typed dataset.
- Add operational metadata.
- Remove duplicates and enforce schema consistency.

Recommended output format:

- Delta

Recommended partition strategy:

- Partition by `country` and `state_province`

Rationale:

- The case asks for partitioning by location.
- Partitioning only by `city` would create too many small partitions and poor file layout.
- `country` + `state_province` is a more scalable compromise that still satisfies location-based partitioning.
- `city` remains available as a regular column for filtering and for Gold aggregations.

Recommended Silver schema:

- Business columns:
  - `id`
  - `name`
  - `brewery_type`
  - `address_1`
  - `address_2`
  - `address_3`
  - `city`
  - `state_province`
  - `postal_code`
  - `country`
  - `longitude`
  - `latitude`
  - `phone`
  - `website_url`
  - `state`
  - `street`
- Operational columns:
  - `source_system`
  - `source_endpoint`
  - `ingestion_run_id`
  - `ingested_at_utc`
  - `extract_date`
  - `source_page`
  - `record_hash`

Transformations in Silver:

- Enforce explicit Spark schema.
- Cast latitude and longitude to numeric types.
- Trim strings and normalize obvious blank values to null.
- Deduplicate by `id`, keeping the latest valid version for the curated current-state layer.
- Add ingestion metadata columns.
- Validate key required fields such as `id`, `name`, `brewery_type`, `country`.
- Support safe overwrite or merge-based maintenance depending on final implementation scope.

Why Delta in Silver:

- Silver is the curated operational layer where repeated IDs, reruns, and source-side attribute changes are most likely to matter.
- Delta gives us better semantics for practical cases such as brewery status changes, idempotent processing, and future incremental evolution.
- Parquet would also be a valid option for a pure snapshot-based Silver layer, but Delta covers more realistic scenarios for this assessment with a cleaner story.

### Gold layer

Purpose:

- Provide an analytical view that directly answers the business requirement.
- Provide an active-breweries analytical view that excludes breweries marked as `closed`.

Recommended aggregation grain:

- `country`
- `state_province`
- `city`
- `brewery_type`

Metrics:

- `brewery_count`

Gold filtering rule:

- Only active breweries should be included in the main analytical output.
- Records with `brewery_type = 'closed'` must be excluded from the active-breweries aggregation.

Implementation note:

- The main Gold table should represent active breweries only.
- If time allows, we may optionally publish a second Gold dataset with all breweries, including `closed`, for comparison and auditability.

Recommended output path:

```text
data/gold/openbrewerydb/breweries_by_type_location/
```

Recommended output format:

- Delta

Recommended partition strategy:

- Partition by `country`

Rationale:

- Gold will be much smaller than Silver.
- Partitioning only by `country` is usually enough here and avoids over-partitioning.
- Delta is a strong fit for the business-facing layer because it improves reliability and future extensibility for analytical consumers.

## 9. Metadata Handling Strategy

Since metadata capture was explicitly mentioned, we should treat metadata as a first-class dataset.

We will capture three metadata categories:

### API dataset metadata

- Response from `/v1/breweries/meta`
- Reported total records
- API-provided counts by state
- API-provided counts by type

### Request execution metadata

- Request URL and parameters
- Page number and page size
- Status code
- Response headers
- Request timestamp
- Retry count
- Extraction duration

### Pipeline run metadata

- `run_id`
- DAG run timestamp
- Task-level success or failure status
- Bronze record count
- Silver record count
- Gold record count
- Data quality check results

This makes the project stronger in an interview because it demonstrates operational thinking instead of only data movement.

## 10. Orchestration Design

### Chosen tool

- Apache Airflow

### DAG behavior

- Schedule:
  - manual trigger by default for the assessment
  - optionally daily for demonstration
- Retries:
  - retry API extraction tasks on transient failures
  - exponential backoff
- Failure handling:
  - fail fast on repeated API or quality failures
  - keep raw files from partial extraction for diagnosis
- Dependency flow:
  - metadata discovery before page extraction
  - Silver only after Bronze extraction succeeds
  - Gold only after Silver validation passes

### Suggested tasks

1. `fetch_api_metadata`
2. `build_page_plan`
3. `extract_breweries_pages`
4. `validate_bronze_completeness`
5. `transform_bronze_to_silver`
6. `validate_silver_quality`
7. `build_gold_aggregates`
8. `validate_gold_quality`
9. `publish_run_summary`

### Pagination strategy

- Call `/meta` with chosen `per_page` to read `total`
- Compute `total_pages = ceil(total / per_page)`
- Iterate through all pages from `1..total_pages`
- Persist each page independently in Bronze

Why this is important:

- It shows deterministic completeness.
- It allows restartability at the page level.
- It aligns with the recruiter's explicit pagination guidance.

## 11. Object-Oriented Module Design

To reflect senior-level code organization, the implementation should use small, focused classes instead of a single procedural script.

### Proposed classes

- `OpenBreweryApiClient`
  - owns HTTP session, retries, header capture, and request execution
- `OpenBreweryMetadataService`
  - fetches `/meta` and computes ingestion scope
- `PaginationPlanner`
  - calculates page windows from total and `per_page`
- `BronzeExtractor`
  - iterates pages and returns raw payloads plus metadata
- `BronzeWriter`
  - writes raw JSON and request logs to storage
- `SparkSessionFactory`
  - creates Spark sessions with deterministic configuration
- `SilverTransformer`
  - reads Bronze and writes curated Delta tables
- `GoldAggregator`
  - reads Silver and writes Gold Delta tables
- `DataQualityValidator`
  - applies record count, uniqueness, null, and schema checks
- `PipelineMetricsPublisher`
  - emits run metrics and alert payloads

Design goal:

- Each class should have one main responsibility and be testable in isolation.

## 12. Data Quality Plan

Data quality should be explicit, not implicit.

### Bronze checks

- API response status must be `200`
- Page payload must be valid JSON
- Record count per page should be `<= per_page`
- Number of extracted pages should equal planned pages

### Silver checks

- Schema matches expected Spark schema
- `id` is not null
- `id` is unique after deduplication
- Required descriptive fields are not null beyond an acceptable threshold
- Record count is greater than zero
- Silver count should not exceed Bronze count after deduplication logic is explained
- status-related fields such as `brewery_type` must remain valid after curation

### Gold checks

- `brewery_count` is non-negative
- Sum of `brewery_count` equals Silver distinct brewery count at the chosen grain
- Required grouping columns are populated
- No `closed` breweries appear in the active Gold output

### Recommended implementation

- Lightweight custom validators in PySpark
- Optional enhancement: Great Expectations or Soda for richer validation, if time allows

For the assessment, custom validators are enough and easier to explain.

## 13. Error Handling Strategy

We should show resilience at both application and pipeline levels.

### API errors

- Retry on connection errors, timeouts, and `5xx`
- Handle `429` with backoff
- Log request context for every failure

### Data errors

- Fail validation when required fields or schema rules are violated
- Store diagnostic summaries for failed checks

### Storage and Spark errors

- Fail the task with contextual logging
- Keep Bronze data immutable so reruns remain possible

### Idempotency approach

- Every run gets a unique `run_id`
- Bronze writes are stored in run-specific paths
- Silver and Gold can either:
  - write to run-scoped paths, or
  - overwrite the latest curated zone after successful completion

Recommended for the assessment:

- Bronze: append by `run_id`
- Silver: overwrite current curated Delta table after validations pass
- Gold: overwrite current Delta table after validations pass

This keeps demos simple while preserving replay evidence in Bronze.

## 14. Testing Strategy

Automated tests are mandatory.

### Unit tests

- Pagination calculation
- API response parsing
- Metadata handling
- Request logging behavior
- Validation rules

### Integration tests

- Bronze extraction with mocked API responses
- Spark transformation Bronze -> Silver
- Spark aggregation Silver -> Gold
- End-to-end local happy path with sample files

### Test tooling

- `pytest`
- `unittest.mock` or `responses` for API mocking
- local Spark session fixture for PySpark tests

### Key cases to cover

- multi-page ingestion
- empty page behavior
- duplicate brewery IDs
- null coordinates
- malformed API response
- rate-limit or transient API failures

## 15. Monitoring and Alerting Design

The PDF asks for monitoring and alerting, so we should document both an assessment-ready version and a production evolution.

### Assessment-ready implementation

- Structured logs in all tasks
- Run summary JSON generated after each DAG run
- Validation report saved to disk
- Airflow task-level failure visibility

### Production evolution

- Metrics to Prometheus / StatsD
- Dashboards in Grafana
- Alerts to Slack or email on:
  - DAG failure
  - repeated API failures
  - schema drift
  - unexpected record-count drop
  - quality-check failures

### Minimum metrics to emit

- extraction duration
- records extracted
- pages extracted
- Bronze/Silver/Gold row counts
- failed validation count
- retry count

## 16. Local Development and Runtime Strategy

### Recommended local execution modes

- `make run-local-extract`
- `make run-local-spark`
- `make test`
- `make lint`
- `make up` for Docker Compose

### Docker Compose services

- `airflow-webserver`
- `airflow-scheduler`
- `airflow-init`
- `spark` or local PySpark-capable app container
- optional `minio` if we want object-storage semantics locally

Practical recommendation:

- Start with local filesystem-backed storage to keep scope tight.
- If time allows, add MinIO as an extra-credit enhancement.

## 17. Documentation Plan

Documentation quality is part of the evaluation, so we should make it visible and deliberate.

### Core documents to ship

- Main `README.md`
  - project overview
  - architecture summary
  - setup instructions
  - how to run pipeline
  - how to run tests
- `docs/architecture.md`
  - architecture diagram and design decisions
- `docs/data_model.md`
  - Bronze, Silver, Gold schema details
- `docs/runbook.md`
  - troubleshooting, reruns, and operational notes

### In-code documentation

- docstrings on public classes and methods
- type hints
- short comments only where logic is not obvious

## 18. Trade-Offs and Justifications

### Airflow instead of a lighter orchestrator

Pros:

- More production-aligned for senior DE interviews
- Strong demonstration of retries, scheduling, and observability

Cons:

- More setup overhead than a simpler orchestrator

Decision:

- Use Airflow because it signals operational maturity and fits the case well.

### Local filesystem instead of real cloud storage

Pros:

- Easy for reviewers to run locally
- No cloud credentials required
- Faster to deliver within one week

Cons:

- Less cloud realism

Decision:

- Use local filesystem by default, with storage paths designed so they can later map to S3 or ADLS.

### Delta in Silver and Gold instead of Parquet in the curated layers

Pros:

- Stronger support for deduplication and evolving current-state curation in Silver
- Stronger analytical serving layer
- ACID transactions and better table semantics
- Easier future evolution toward incremental updates and time travel use cases

Cons:

- Adds more setup complexity than a Parquet-only curated design

Decision:

- Use Delta in both Silver and Gold because it better supports realistic scenarios such as repeated IDs, reruns, status changes like brewery closures, and future incremental evolution.

Interview note:

- Parquet in Silver would also be a valid design choice for a simpler full-refresh snapshot pipeline.
- For this assessment, we intentionally chose Delta in Silver as well because it better covers realistic operational scenarios without changing the medallion design.
- This is especially helpful when the same brewery `id` is received again with updated attributes, such as a brewery later being marked as `closed`.
- Bronze remains the immutable raw history, while Silver becomes a more robust curated current-state layer.

### Partition by country and state instead of full city partitioning

Pros:

- Better file-size behavior
- Avoids excessive partition cardinality

Cons:

- Less granular partition pruning than city-level partitioning

Decision:

- Use `country` and `state_province` in Silver and keep `city` as a dimension column.

### Custom data quality validators instead of a full framework

Pros:

- Lower implementation overhead
- Easier to explain in an assessment

Cons:

- Less feature-rich than Great Expectations or Soda

Decision:

- Implement custom validators first, then add a framework only if time remains.

## 19. Implementation Roadmap

### Phase 1: Project foundation

- Create repository structure
- Add dependency management
- Add Docker Compose
- Add lint/test tooling

### Phase 2: Ingestion

- Build API client
- Build metadata extraction
- Build pagination planner
- Write Bronze storage logic

### Phase 3: Transformation

- Define Spark schemas
- Build Bronze -> Silver transformation
- Implement partitioning and validation
- Build Silver -> Gold aggregation

### Phase 4: Orchestration

- Build Airflow DAG
- Wire retries and task dependencies
- Add run summaries

### Phase 5: Hardening

- Add tests
- Improve logging
- Add README and docs
- Validate full local execution

## 20. Submission Strategy

The recruiter email says the case was sent on March 20, 2026 with a 7-day deadline, so the practical submission deadline is March 27, 2026.

Recommended final deliverables:

- Public GitHub repository
- Clean commit history
- Complete README
- Architecture and operational docs
- Runnable local setup
- Automated tests

## 21. Assumptions

- The assessment can be implemented and demonstrated locally without cloud services.
- Python and PySpark are acceptable and preferred for this case.
- The API does not require authentication.
- The current API response shape is stable enough to define an explicit schema.
- Capturing `/meta` plus request headers satisfies the metadata requirement unless the reviewer asks for more.

## 22. Recommended Next Step

Implement the project in this order:

1. scaffold the repository and dependencies
2. build Bronze ingestion with pagination and metadata capture
3. implement Spark Silver and Gold transformations
4. add Airflow orchestration
5. add tests and documentation polish

This order gives us an end-to-end pipeline early and leaves room for hardening before submission.
