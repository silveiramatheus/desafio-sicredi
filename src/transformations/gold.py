from pathlib import Path

from pyspark.sql import SparkSession

from src.config import PROJECT_ROOT, PipelineConfig
from src.utils.logging import get_logger, step
from src.utils.spark import run_sql_file

GOLD_SQL_DIR = PROJECT_ROOT / "sql" / "gold"

logger = get_logger(__name__)


def gold_sql_files() -> list[Path]:
    return sorted(GOLD_SQL_DIR.glob("*.sql"))


def table_name(sql_file: Path) -> str:
    return sql_file.stem.split("_", 1)[1]


def build_gold(spark: SparkSession, config: PipelineConfig) -> dict[str, int]:
    counts = {}
    with step(logger, "gold"):
        for sql_file in gold_sql_files():
            run_sql_file(spark, config, sql_file)
            name = table_name(sql_file)
            counts[name] = spark.table(config.table("gold", name)).count()
            logger.info("gold | %s | %d rows", name, counts[name])
    return counts
