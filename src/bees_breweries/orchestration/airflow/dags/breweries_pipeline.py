"""Airflow DAG for the BEES breweries medallion pipeline."""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

PROJECT_DIR = "/opt/airflow/project"
PYTHONPATH_EXPORT = "export PYTHONPATH=/opt/airflow/project/src"

default_args = {
    "owner": "bees-data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="bees_breweries_medallion_pipeline",
    description="Open Brewery DB ingestion and medallion processing pipeline.",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bees", "data-engineering", "brewery"],
) as dag:
    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHONPATH_EXPORT} && "
            "python -m bees_breweries --command bronze-ingest"
        ),
    )

    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHONPATH_EXPORT} && "
            "python -m bees_breweries --command silver-transform"
        ),
    )

    gold_aggregation = BashOperator(
        task_id="gold_aggregation",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{PYTHONPATH_EXPORT} && "
            "python -m bees_breweries --command gold-transform"
        ),
    )

    bronze_ingestion >> silver_transformation >> gold_aggregation
