from pyspark.sql import SparkSession

from src.config import PipelineConfig


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
