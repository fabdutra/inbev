"""Package entrypoint for local development commands."""

import argparse
import json

from bees_breweries.config.settings import get_settings
from bees_breweries.domain.models import PipelineRunContext
from bees_breweries.ingestion.api_client import OpenBreweryApiClient
from bees_breweries.ingestion.bronze_writer import BronzeWriter
from bees_breweries.ingestion.extractor import BronzeExtractor
from bees_breweries.ingestion.metadata_client import OpenBreweryMetadataService
from bees_breweries.ingestion.paginator import PaginationPlanner
from bees_breweries.observability.logging import configure_logging
from bees_breweries.processing.bronze_to_silver import SilverTransformer
from bees_breweries.processing.silver_to_gold import GoldAggregator
from bees_breweries.processing.spark_session_factory import SparkSessionFactory
from bees_breweries.processing.validators import DataQualityValidator
from bees_breweries.utils.clock import utc_now
from bees_breweries.utils.ids import generate_run_id


def build_parser() -> argparse.ArgumentParser:
    """Build the package CLI parser."""

    parser = argparse.ArgumentParser(description="BEES breweries pipeline CLI.")
    parser.add_argument(
        "--command",
        choices=["bootstrap", "bronze-ingest", "silver-transform", "gold-transform"],
        default="bootstrap",
        help="Command to execute.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optional page cap for local Bronze tests.",
    )
    parser.add_argument(
        "--per-page",
        type=int,
        default=None,
        help="Optional page size override.",
    )
    return parser


def main() -> None:
    configure_logging()
    settings = get_settings()
    args = build_parser().parse_args()

    if args.command == "bootstrap":
        print(f"Project bootstrap ready. Environment={settings.environment}")
        return

    if args.command == "silver-transform":
        spark = SparkSessionFactory().create(app_name="bees-breweries-silver")
        try:
            output_path, validations, row_count = SilverTransformer(
                spark=spark,
                settings=settings,
                validator=DataQualityValidator(),
            ).transform()
            print(
                json.dumps(
                    {
                        "silver_output_path": output_path,
                        "silver_row_count": row_count,
                        "validations": [
                            {"name": result.name, "passed": result.passed, "details": result.details}
                            for result in validations
                        ],
                    },
                    indent=2,
                    default=str,
                )
            )
        finally:
            spark.stop()
        return

    if args.command == "gold-transform":
        spark = SparkSessionFactory().create(app_name="bees-breweries-gold")
        try:
            output_path, validations, row_count = GoldAggregator(
                spark=spark,
                settings=settings,
                validator=DataQualityValidator(),
            ).transform()
            print(
                json.dumps(
                    {
                        "gold_output_path": output_path,
                        "gold_row_count": row_count,
                        "validations": [
                            {"name": result.name, "passed": result.passed, "details": result.details}
                            for result in validations
                        ],
                    },
                    indent=2,
                    default=str,
                )
            )
        finally:
            spark.stop()
        return

    run_context = PipelineRunContext(
        run_id=generate_run_id(),
        extract_date=utc_now().date().isoformat(),
    )
    api_client = OpenBreweryApiClient(settings=settings)

    extractor = BronzeExtractor(
        api_client=api_client,
        metadata_service=OpenBreweryMetadataService(api_client=api_client),
        paginator=PaginationPlanner(),
        writer=BronzeWriter(settings=settings),
    )

    summary = extractor.extract(
        run_context=run_context,
        per_page=args.per_page or settings.api_per_page,
        max_pages=args.max_pages,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
