# Runbook

## Purpose

This runbook provides the operational steps for running, validating, and troubleshooting the pipeline locally.

## Prerequisites

Required tools:

- Docker Desktop
- Docker Compose

Optional local execution tools:

- Python 3.9+
- Java 17 for host-based Spark execution

## Start the Containerized Stack

### Build the shared runtime

```bash
docker compose -f docker/docker-compose.yml build
```

### Initialize Airflow

```bash
docker compose -f docker/docker-compose.yml up airflow-init
```

### Start Airflow services

```bash
docker compose -f docker/docker-compose.yml up airflow-webserver airflow-scheduler
```

### Open Airflow UI

```text
http://localhost:8080
```

Credentials:

- username: `airflow`
- password: `airflow`

## Manual Pipeline Execution

### Bronze

```bash
docker compose -f docker/docker-compose.yml run --rm app python -m bees_breweries --command bronze-ingest --max-pages 1
```

### Silver

```bash
docker compose -f docker/docker-compose.yml run --rm app python -m bees_breweries --command silver-transform
```

### Gold

```bash
docker compose -f docker/docker-compose.yml run --rm app python -m bees_breweries --command gold-transform
```

## Airflow DAG Execution

Trigger the DAG:

- `bees_breweries_medallion_pipeline`

Task order:

1. `bronze_ingestion`
2. `silver_transformation`
3. `gold_aggregation`

## Local Host Execution

If you want to run outside Docker:

```bash
PYTHONPATH=src python3 -m bees_breweries --command bronze-ingest
PYTHONPATH=src python3 -m bees_breweries --command silver-transform
PYTHONPATH=src python3 -m bees_breweries --command gold-transform
```

## Data Locations

### Bronze

```text
data/bronze/openbrewerydb/
```

Contains:

- raw brewery pages
- metadata payloads
- request logs

### Silver

```text
data/silver/openbrewerydb/breweries_curated
```

Contains:

- curated current-state brewery Delta table

### Gold

```text
data/gold/openbrewerydb/active_breweries_by_type_location
```

Contains:

- active brewery analytical Delta table

## Validation Expectations

### Bronze

Expected:

- metadata file created
- page files created
- request logs created

### Silver

Expected:

- Delta output exists
- no duplicate ids
- no null ids
- no blank `brewery_type`

### Gold

Expected:

- Delta output exists
- no `closed` breweries
- valid `brewery_count`

## Operational Checks

### Confirm Bronze output was written

```bash
find data/bronze/openbrewerydb -type f | sort
```

### Confirm Silver Delta output exists

```bash
find data/silver/openbrewerydb/breweries_curated -type f | sort
```

### Confirm Gold Delta output exists

```bash
find data/gold/openbrewerydb/active_breweries_by_type_location -type f | sort
```

### Confirm Airflow tasks finished successfully

In the Airflow UI, verify the following tasks are green:

1. `bronze_ingestion`
2. `silver_transformation`
3. `gold_aggregation`

## Common Checks

### Check Docker services

```bash
docker compose -f docker/docker-compose.yml ps
```

### Check Airflow logs

```bash
docker compose -f docker/docker-compose.yml logs airflow-webserver airflow-scheduler
```

### Check app command output

```bash
docker compose -f docker/docker-compose.yml run --rm app python -m bees_breweries --command bootstrap
```

### Re-run only one layer manually

Bronze:

```bash
docker compose -f docker/docker-compose.yml run --rm app python -m bees_breweries --command bronze-ingest --max-pages 1
```

Silver:

```bash
docker compose -f docker/docker-compose.yml run --rm app python -m bees_breweries --command silver-transform
```

Gold:

```bash
docker compose -f docker/docker-compose.yml run --rm app python -m bees_breweries --command gold-transform
```

## Troubleshooting

### Docker daemon is not running

Symptom:

- Docker commands fail to connect to the daemon

Action:

- start Docker Desktop
- rerun the failed command

### Airflow init fails

Symptom:

- `airflow-init` exits with an error

Action:

- inspect the output:

```bash
docker compose -f docker/docker-compose.yml up airflow-init
```

- confirm Postgres is healthy:

```bash
docker compose -f docker/docker-compose.yml ps
```

### Spark command fails in container

Symptom:

- Silver or Gold transformation exits with Spark-related errors

Action:

- rerun the command manually with `app`
- inspect the full logs
- verify Bronze or Silver inputs exist

### Spark warns that `ps` is missing

Symptom:

- startup warning from `load-spark-env.sh`

Action:

- current warning is non-blocking
- optional improvement: install `procps` in the Docker image

### Airflow UI is not reachable

Symptom:

- browser cannot access `localhost:8080`

Action:

- confirm `airflow-webserver` is running:

```bash
docker compose -f docker/docker-compose.yml ps
```

- inspect logs:

```bash
docker compose -f docker/docker-compose.yml logs airflow-webserver
```

## Shutdown

Stop the stack:

```bash
docker compose -f docker/docker-compose.yml down
```

Stop and remove volumes:

```bash
docker compose -f docker/docker-compose.yml down -v
```
