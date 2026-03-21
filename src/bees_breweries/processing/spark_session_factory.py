"""Factory for Spark sessions configured with Delta support."""

from __future__ import annotations

import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


class SparkSessionFactory:
    """Create consistently configured Spark sessions."""

    def create(self, app_name: str) -> SparkSession:
        """Return a local Spark session with Delta Lake enabled."""

        self._ensure_java_environment()
        builder = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.shuffle.partitions", "4")
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    @staticmethod
    def _ensure_java_environment() -> None:
        if os.getenv("JAVA_HOME"):
            return

        homebrew_java_home = Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
        if not homebrew_java_home.exists():
            return

        os.environ["JAVA_HOME"] = str(homebrew_java_home)
        os.environ["PATH"] = f"{homebrew_java_home / 'bin'}:{os.environ.get('PATH', '')}"
