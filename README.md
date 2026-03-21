# BEES Senior Data Engineer Technical Assessment

This repository contains a medallion-style data pipeline that consumes brewery data from the Open Brewery DB API, stores raw data in Bronze, curates it into a Silver Delta layer, and publishes an analytical Gold Delta layer with active brewery counts by type and location.

The solution was built to emphasize scalability, maintainability, observability, and reproducibility. It uses Python, PySpark, Delta Lake, Apache Airflow, automated tests, and Docker Compose.

## Objective

The assessment asks for a pipeline that:

- consumes data from the Open Brewery DB API
- handles pagination
- stores raw and curated data using medallion architecture
- partitions curated data by location
- produces an aggregated analytical layer
- includes automated tests
- shows scheduling, retries, and error handling
- documents design decisions and operational considerations

## Architecture

```text
Open Brewery DB API
    |
    v
Airflow DAG
    |
    +--> Bronze ingestion
    |     - fetch /breweries/meta
    |     - calculate pagination plan
    |     - extract /breweries pages
    |     - store raw payloads and request logs
    |
    +--> Silver transformation
    |     - read Bronze JSON
    |     - enforce schema
    |     - normalize fields
    |     - deduplicate by brewery id
    |     - write Delta partitioned by country/state_province
    |
    +--> Gold transformation
          - read Silver Delta
          - exclude brewery_type = 'closed'
          - aggregate active breweries by type and location
          - write Delta
```

## Data Layers

### Bronze

Purpose:

- preserve the source response exactly as received
- preserve request and response metadata for auditability

Stored assets:

- raw page payloads
- raw `/meta` payload
- request logs with headers, timestamps, params, status code, and record counts

Example paths:

```text
data/bronze/openbrewerydb/breweries/extract_date=YYYY-MM-DD/run_id=<run_id>/page=<page>.json
data/bronze/openbrewerydb/metadata/extract_date=YYYY-MM-DD/run_id=<run_id>/meta.json
data/bronze/openbrewerydb/request_logs/extract_date=YYYY-MM-DD/run_id=<run_id>/page=<page>.json
```

### Silver

Purpose:

- create a curated current-state dataset
- enforce schema and normalize values
- deduplicate repeated brewery ids

Format:

- Delta

Partitioning:

- `country`
- `state_province`

Why Delta in Silver:

- better support for repeated ids and reruns
- stronger semantics for current-state curation
- cleaner path for future upserts, schema evolution, and historical expansion

### Gold

Purpose:

- provide the analytical output requested by the case

Business rule:

- only active breweries are included
- rows where `brewery_type = 'closed'` are excluded

Format:

- Delta

Grain:

- `country`
- `state_province`
- `city`
- `brewery_type`

Metric:

- `brewery_count`

## Technology Choices

- Python
- PySpark
- Delta Lake
- Apache Airflow
- Docker Compose
- PostgreSQL for Airflow metadata
- `pytest`
- `ruff`
- `black`

## Repository Structure

```text
src/bees_breweries/
  config/         settings and Spark schemas
  domain/         shared models and enums
  ingestion/      Bronze ingestion modules
  processing/     Silver and Gold transformations
  orchestration/  Airflow DAGs
  observability/  logging helpers
  utils/          shared utility functions

tests/
  unit/
  integration/

data/
  bronze/
  silver/
  gold/

docker/
  Dockerfile
  docker-compose.yml

docs/
  DE_Case_Atualizado.pdf
  SOLUTION_BLUEPRINT.md
```

## Pipeline Commands

Local package commands:

```bash
PYTHONPATH=src python3 -m bees_breweries --command bronze-ingest
PYTHONPATH=src python3 -m bees_breweries --command silver-transform
PYTHONPATH=src python3 -m bees_breweries --command gold-transform
```

Convenience targets:

```bash
make run-bronze
make run-silver
make run-gold
make test
make lint
```

## Running With Docker

### 1. Build the shared runtime

```bash
docker compose -f docker/docker-compose.yml build
```

### 2. Initialize Airflow

```bash
docker compose -f docker/docker-compose.yml up airflow-init
```

### 3. Start Airflow services

```bash
docker compose -f docker/docker-compose.yml up airflow-webserver airflow-scheduler
```

Airflow UI:

```text
http://localhost:8080
```

Credentials:

- username: `airflow`
- password: `airflow`

### 4. Run pipeline commands manually in the container

```bash
docker compose -f docker/docker-compose.yml run --rm app python -m bees_breweries --command bronze-ingest --max-pages 1
docker compose -f docker/docker-compose.yml run --rm app python -m bees_breweries --command silver-transform
docker compose -f docker/docker-compose.yml run --rm app python -m bees_breweries --command gold-transform
```

### 5. Run the DAG

Trigger the DAG in Airflow UI:

- `bees_breweries_medallion_pipeline`

## Testing

Run tests locally:

```bash
PYTHONPATH=src python3 -m pytest -q
```

or:

```bash
make test
```

Current test coverage includes:

- pagination logic
- Bronze writer behavior
- settings defaults
- Bronze to Silver transformation
- Silver to Gold transformation

## What Is Validated

### Bronze

- metadata endpoint is fetched successfully
- page planning is deterministic
- raw payloads are stored
- request logs are stored

### Silver

- row count is greater than zero
- `id` is unique
- `id` is not null
- `brewery_type` is not blank

### Gold

- row count is greater than zero
- `brewery_count` is non-negative
- no `closed` breweries appear in the output
- location columns required by the aggregation are not null

## Design Decisions and Trade-Offs

### PySpark

PySpark was chosen to align with the assessment guidance and to demonstrate scalable transformation patterns without using pandas.

### Delta in Silver and Gold

Parquet would be a valid option for a simpler snapshot-only curated layer, but Delta was chosen to better support:

- reruns
- repeated ids
- future upserts
- schema evolution
- stronger table semantics

### Partitioning strategy

Silver is partitioned by `country` and `state_province` instead of `city`.

Why:

- it still satisfies location-based partitioning
- it avoids excessive partition cardinality
- it produces a more practical file layout

### Containerized Spark instead of standalone cluster

Spark runs in local mode inside the shared Docker image.

Why:

- reproducible runtime
- simpler setup for reviewers
- production-minded without unnecessary cluster complexity for the assessment scope

## Monitoring and Alerting Approach

The current implementation includes:

- structured logs
- run summaries
- request metadata capture
- validation checks before curated writes

A production evolution would include:

- metrics to Prometheus or StatsD
- dashboards in Grafana
- alerts to Slack or email for:
  - DAG failures
  - repeated API failures
  - schema drift
  - abnormal record-count changes
  - data quality check failures

## Current Status

Implemented and validated:

- Bronze ingestion with pagination and metadata capture
- Silver Delta transformation with deduplication and validation
- Gold Delta aggregation for active breweries only
- Dockerized runtime for Airflow and Spark-enabled commands
- Airflow DAG orchestrating Bronze -> Silver -> Gold
- unit and integration tests

Verified manually:

- local host execution
- containerized Bronze execution
- containerized Silver execution
- containerized Gold execution
- Airflow stack initialization and startup

## Future Improvements

- add richer data quality framework integration
- persist run summaries as first-class operational datasets
- add Slack or email alerts
- add MinIO or cloud object storage abstraction
- add incremental merge strategy in Silver if the source usage pattern evolves
- expand operational documentation with a dedicated runbook

## Additional Documentation

- Solution blueprint: [docs/SOLUTION_BLUEPRINT.md](./docs/SOLUTION_BLUEPRINT.md)
- Original case PDF: [docs/DE_Case_Atualizado.pdf](./docs/DE_Case_Atualizado.pdf)
