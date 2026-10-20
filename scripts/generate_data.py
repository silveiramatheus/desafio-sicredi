import argparse
import shutil
from datetime import date
from pathlib import Path

from src.config import PipelineConfig
from src.datagen.generator import DEFAULT_NOISE_RATES, generate_landing
from src.utils.logging import get_logger

logger = get_logger("generate_data")


def parse_noise(value: str) -> tuple[str, float]:
    entity, _, rate = value.partition("=")
    if entity not in DEFAULT_NOISE_RATES or not rate:
        raise argparse.ArgumentTypeError(f"Expected <entity>=<rate> with entity in {list(DEFAULT_NOISE_RATES)}, got: {value}")
    return entity, float(rate)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate fake landing files for the NovaRota card pipeline")
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 10, 1))
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--customers", type=int, default=150)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--noise", type=parse_noise, action="append", default=[], metavar="ENTITY=RATE")
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()

    output = args.output or Path(PipelineConfig.from_env().landing_path)
    if args.clean and output.exists():
        shutil.rmtree(output)

    folders = generate_landing(
        output,
        start_date=args.start_date,
        days=args.days,
        num_customers=args.customers,
        noise_rates=dict(args.noise),
        seed=args.seed,
    )
    for folder in folders:
        logger.info("generated %s", folder)


if __name__ == "__main__":
    main()
