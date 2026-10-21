from datetime import date
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import ENTITIES, PipelineConfig, landing_file_name
from src.utils.logging import get_logger, step

TECHNICAL_COLUMNS = ("arquivo_origem", "data_referencia", "timestamp_ingestao", "hash_linha")
NULL_MARKER = "<null>"
HASH_SEPARATOR = "||"

logger = get_logger(__name__)


def list_landing_files(config: PipelineConfig, reference_date: date) -> dict[str, str]:
    folder = Path(config.landing_folder(reference_date))
    files = {}
    for entity in ENTITIES:
        path = folder / landing_file_name(entity, reference_date)
        if path.exists():
            files[entity] = str(path)
        else:
            logger.warning("file not found | %s | %s", entity, path)
    return files


def read_landing_file(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .load(path)
        .select("*", F.col("_metadata.file_path").alias("arquivo_origem"))
    )


def row_hash(columns: list[str]) -> Column:
    values = [F.coalesce(F.col(column).cast("string"), F.lit(NULL_MARKER)) for column in columns]
    return F.sha2(F.concat_ws(HASH_SEPARATOR, *values), 256)


def add_technical_metadata(df: DataFrame, reference_date: date) -> DataFrame:
    business_columns = [column for column in df.columns if column not in TECHNICAL_COLUMNS]
    return (
        df.withColumn("data_referencia", F.lit(reference_date).cast("date"))
        .withColumn("timestamp_ingestao", F.current_timestamp())
        .withColumn("hash_linha", row_hash(business_columns))
    )


def write_bronze(df: DataFrame, table: str, reference_date: date) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"data_referencia = '{reference_date.isoformat()}'")
        .option("mergeSchema", "true")
        .saveAsTable(table)
    )


def ingest_reference_date(spark: SparkSession, config: PipelineConfig, reference_date: date) -> dict[str, int]:
    counts = {}
    with step(logger, f"bronze | {reference_date}"):
        for entity, path in list_landing_files(config, reference_date).items():
            table = config.table("bronze", entity)
            df = add_technical_metadata(read_landing_file(spark, path), reference_date)
            write_bronze(df, table, reference_date)
            counts[entity] = spark.table(table).where(F.col("data_referencia") == F.lit(reference_date)).count()
            logger.info("bronze | %s | %s | %d rows", reference_date, table, counts[entity])
    return counts
