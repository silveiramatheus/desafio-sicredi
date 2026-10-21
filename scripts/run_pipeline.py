import argparse
from datetime import date

from src.config import PipelineConfig
from src.ingestion.bronze import ingest_reference_date
from src.utils.spark import create_schemas, get_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NovaRota card pipeline for one or more reference dates")
    parser.add_argument("--dates", type=date.fromisoformat, nargs="+", required=True)
    args = parser.parse_args()

    config = PipelineConfig.from_env()
    spark = get_spark(config)
    create_schemas(spark, config)

    for reference_date in args.dates:
        ingest_reference_date(spark, config, reference_date)


if __name__ == "__main__":
    main()
