from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.config import LANDING_SCHEMA, LANDING_VOLUME, PipelineConfig


def get_spark(config: PipelineConfig, app_name: str = "novarota-pipeline") -> SparkSession:
    if config.env == "databricks":
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    warehouse = config.warehouse_path
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", f"{warehouse}/warehouse")
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={warehouse}/metastore_db;create=true")
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={warehouse}")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_schemas(spark: SparkSession, config: PipelineConfig) -> None:
    for layer in config.schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.schema(layer)}")


def run_sql_file(spark: SparkSession, config: PipelineConfig, path: Path | str) -> DataFrame:
    if config.catalog:
        spark.sql(f"USE CATALOG {config.catalog}")
    return spark.sql(Path(path).read_text(encoding="utf-8").strip().rstrip(";"))


def create_landing_volume(spark: SparkSession, config: PipelineConfig) -> None:
    if config.env != "databricks":
        return
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog}.{LANDING_SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.catalog}.{LANDING_SCHEMA}.{LANDING_VOLUME}")
