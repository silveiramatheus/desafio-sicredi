import argparse
from datetime import date

from src.config import PipelineConfig
from src.ingestion.bronze import ingest_reference_date
from src.transformations.gold import build_gold
from src.transformations.silver import run_silver
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
        run_silver(spark, config, reference_date)
    build_gold(spark, config)


if __name__ == "__main__":
    main()
