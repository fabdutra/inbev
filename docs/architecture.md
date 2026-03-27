# Architecture

## Overview

This solution implements a medallion-style data pipeline for the Open Brewery DB API using Python, PySpark, Delta Lake, Apache Airflow, and Docker Compose.

The pipeline is organized into three layers:

- Bronze: raw source ingestion
- Silver: curated current-state data
- Gold: analytical active-brewery aggregation

## High-Level Flow

```text
Open Brewery DB API
    |
    +--> /breweries/meta
    |      - discover total records
    |      - capture metadata
    |
    +--> /breweries
           - fetch paginated pages
           - capture request/response metadata
           - persist raw JSON

Bronze
    |
    v
PySpark Silver Transformation
    - explicit schema
    - normalization
    - deduplication by id
    - Delta output partitioned by country/state_province

Silver
    |
    v
PySpark Gold Transformation
    - exclude brewery_type = 'closed'
    - aggregate active breweries by type and location
    - Delta output partitioned by country

Gold
```

## Orchestration

The pipeline is orchestrated with Apache Airflow.

The DAG runs three sequential tasks:

1. `bronze_ingestion`
2. `silver_transformation`
3. `gold_aggregation`

Each task reuses the project CLI commands rather than embedding transformation logic inside the DAG itself. This keeps the business logic reusable and easier to test outside Airflow.

## Component Responsibilities

### Ingestion layer

Files under `src/bees_breweries/ingestion/` are responsible for:

- API communication
- metadata retrieval
- pagination planning
- raw Bronze writes

Key modules:

- `api_client.py`
- `metadata_client.py`
- `paginator.py`
- `extractor.py`
- `bronze_writer.py`

### Processing layer

Files under `src/bees_breweries/processing/` are responsible for:

- Spark session creation
- Bronze to Silver transformation
- Silver to Gold transformation
- curated-layer validations

Key modules:

- `spark_session_factory.py`
- `bronze_to_silver.py`
- `silver_to_gold.py`
- `validators.py`

### Orchestration layer

Files under `src/bees_breweries/orchestration/airflow/` are responsible for:

- DAG definition
- task sequencing
- retries and task execution

Key module:

- `dags/breweries_pipeline.py`

## Data Layer Design

### Bronze

Characteristics:

- immutable raw storage
- one file per page
- request logs stored separately
- metadata endpoint response stored separately

Why:

- preserves source truth
- supports replayability and traceability

### Silver

Characteristics:

- current-state curated dataset
- Delta format
- partitioned by location
- deduplicated by brewery id

Why:

- creates a trusted analytical foundation
- better handles reruns and repeated ids

### Gold

Characteristics:

- active-brewery analytical layer
- Delta format
- aggregated by brewery type and location
- excludes `closed` breweries

Why:

- directly supports the requested analytical output
- enforces business logic close to the serving layer

## Why Delta Was Chosen

Delta was chosen for Silver and Gold because it provides:

- transactional writes
- stronger table semantics
- better support for evolving datasets
- a cleaner future path for upserts and schema evolution

Parquet would also be a valid simpler option, especially for a pure snapshot-only curated design, but Delta better covers practical production scenarios.

## Runtime Model

The project uses a shared Docker image for:

- Airflow services
- Spark-enabled pipeline commands

This avoids depending on host-specific Java or Spark configuration and makes the runtime more reproducible for reviewers.

## Validation Strategy

### Silver validation

- positive row count
- unique ids
- non-null ids
- non-blank `brewery_type`

### Gold validation

- positive row count
- non-negative `brewery_count`
- exclusion of `closed` breweries
- non-null location fields required by the aggregation

## Future Evolution

Potential future improvements include:

- richer data quality framework integration
- alerting and metrics export
- historical tracking in Silver
- cloud object storage support
- incremental merge strategy for Silver
