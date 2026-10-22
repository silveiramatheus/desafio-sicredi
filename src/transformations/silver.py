from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.config import PipelineConfig
from src.quality.rules import PARENT_PREFIX, REFERENCE_FLAG, apply_quality_rules, quality_rules
from src.utils.logging import get_logger, step

DATE_PATTERN = "yyyy-MM-dd"
TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss"
LINEAGE_COLUMNS = ("arquivo_origem", "data_referencia", "timestamp_ingestao", "hash_linha")
RAW_COLUMN = "_raw"
MERGE_SOURCE_VIEW = "silver_merge_source"

logger = get_logger(__name__)


def as_bigint(column: str) -> Column:
    return F.expr(f"try_cast(trim({column}) AS BIGINT)")


def as_decimal(column: str) -> Column:
    return F.expr(f"try_cast(trim({column}) AS DECIMAL(18,2))")


def as_date(column: str) -> Column:
    return F.to_date(F.expr(f"try_to_timestamp(trim({column}), '{DATE_PATTERN}')"))


def as_timestamp(column: str) -> Column:
    return F.expr(f"try_to_timestamp(trim({column}), '{TIMESTAMP_PATTERN}')")


def as_text(column: str) -> Column:
    return F.trim(F.col(column))


def as_code(column: str) -> Column:
    return F.upper(F.trim(F.col(column)))


def as_digits(column: str) -> Column:
    return F.regexp_replace(F.col(column), "[^0-9]", "")


@dataclass(frozen=True)
class EntitySpec:
    name: str
    columns: dict[str, Callable[[str], Column]]
    key: str
    order_by: tuple[str, ...]
    update_condition: str | None = None
    parent: str | None = None
    parent_key: str | None = None
    parent_columns: tuple[str, ...] = ()
    defaults: dict[str, object] = field(default_factory=dict)


ENTITY_SPECS = {
    "clientes": EntitySpec(
        name="clientes",
        columns={
            "id_cliente": as_bigint,
            "nome": as_text,
            "cpf": as_digits,
            "data_nascimento": as_date,
            "cidade": as_text,
            "estado": as_code,
            "data_atualizacao": as_timestamp,
        },
        key="id_cliente",
        order_by=("data_atualizacao", "timestamp_ingestao", "hash_linha"),
        update_condition="s.data_atualizacao > t.data_atualizacao",
    ),
    "contas": EntitySpec(
        name="contas",
        columns={
            "id_conta": as_bigint,
            "id_cliente": as_bigint,
            "tipo_conta": as_code,
            "status_conta": as_code,
            "data_abertura": as_date,
        },
        key="id_conta",
        order_by=("data_referencia", "timestamp_ingestao", "hash_linha"),
        update_condition="s.data_referencia >= t.data_referencia",
        parent="clientes",
        parent_key="id_cliente",
    ),
    "cartoes": EntitySpec(
        name="cartoes",
        columns={
            "id_cartao": as_bigint,
            "id_conta": as_bigint,
            "bandeira": as_code,
            "tipo_cartao": as_code,
            "status_cartao": as_code,
            "data_emissao": as_date,
            "data_atualizacao": as_timestamp,
        },
        key="id_cartao",
        order_by=("data_atualizacao", "timestamp_ingestao", "hash_linha"),
        update_condition="s.data_atualizacao > t.data_atualizacao",
        parent="contas",
        parent_key="id_conta",
    ),
    "transacoes_cartao": EntitySpec(
        name="transacoes_cartao",
        columns={
            "id_transacao": as_bigint,
            "id_cartao": as_bigint,
            "data_transacao": as_timestamp,
            "valor": as_decimal,
            "nome_estabelecimento": as_text,
            "mcc": as_text,
            "cidade_estabelecimento": as_text,
        },
        key="id_transacao",
        order_by=("timestamp_ingestao", "hash_linha"),
        parent="cartoes",
        parent_key="id_cartao",
        parent_columns=("data_emissao",),
        defaults={"estornada": False},
    ),
    "estornos": EntitySpec(
        name="estornos",
        columns={
            "id_estorno": as_bigint,
            "id_transacao": as_bigint,
            "data_estorno": as_date,
            "motivo_estorno": as_code,
        },
        key="id_estorno",
        order_by=("timestamp_ingestao", "hash_linha"),
        parent="transacoes_cartao",
        parent_key="id_transacao",
        parent_columns=("data_transacao",),
    ),
}


def cast_columns(df: DataFrame, columns: dict[str, Callable[[str], Column]]) -> DataFrame:
    return df.select(
        *[cast(name).alias(name) for name, cast in columns.items()],
        F.struct(*columns).alias(RAW_COLUMN),
        *LINEAGE_COLUMNS,
    )


def flag_reference(df: DataFrame, parent: DataFrame, key: str, columns: tuple[str, ...] = ()) -> DataFrame:
    parent_keys = parent.select(
        F.col(key).alias("_parent_key"),
        *[F.col(column).alias(f"{PARENT_PREFIX}{column}") for column in columns],
    ).distinct()
    return (
        df.join(parent_keys, df[key] == parent_keys["_parent_key"], "left")
        .withColumn(REFERENCE_FLAG, F.col("_parent_key").isNotNull())
        .drop("_parent_key")
    )


def deduplicate(df: DataFrame, keys: list[str], order_by: tuple[str, ...]) -> tuple[DataFrame, DataFrame]:
    window = Window.partitionBy(*keys).orderBy(*[F.col(column).desc() for column in order_by])
    ranked = (
        df.withColumn("_row_number", F.row_number().over(window))
        .withColumn("_latest_hash", F.first("hash_linha").over(window))
    )
    latest = ranked.where(F.col("_row_number") == 1).drop("_row_number", "_latest_hash")
    duplicates = (
        ranked.where(F.col("_row_number") > 1)
        .withColumn(
            "motivo_erro",
            F.when(F.col("hash_linha") == F.col("_latest_hash"), F.lit("REGISTRO_DUPLICADO"))
            .otherwise(F.lit("VERSAO_DESATUALIZADA")),
        )
        .drop("_row_number", "_latest_hash")
    )
    return latest, duplicates


def write_quarantine(df: DataFrame, table: str, reference_date: date) -> None:
    (
        df.select(f"{RAW_COLUMN}.*", *LINEAGE_COLUMNS, "motivo_erro")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"data_referencia = '{reference_date.isoformat()}'")
        .option("mergeSchema", "true")
        .saveAsTable(table)
    )


def merge_into(spark: SparkSession, df: DataFrame, table: str, key: str, update_condition: str | None = None) -> None:
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").saveAsTable(table)
    df.createOrReplaceTempView(MERGE_SOURCE_VIEW)
    when_matched = f"WHEN MATCHED AND {update_condition} THEN UPDATE SET *" if update_condition else ""
    spark.sql(f"""
        MERGE INTO {table} AS t
        USING {MERGE_SOURCE_VIEW} AS s
        ON t.{key} = s.{key}
        {when_matched}
        WHEN NOT MATCHED THEN INSERT *
    """)


def mark_chargebacks(spark: SparkSession, config: PipelineConfig) -> None:
    tables = [config.table("silver", "transacoes_cartao"), config.table("silver", "estornos")]
    if not all(spark.catalog.tableExists(table) for table in tables):
        return
    spark.sql(f"""
        MERGE INTO {config.table("silver", "transacoes_cartao")} AS t
        USING (SELECT DISTINCT id_transacao FROM {config.table("silver", "estornos")}) AS e
        ON t.id_transacao = e.id_transacao
        WHEN MATCHED AND NOT t.estornada THEN UPDATE SET t.estornada = true
    """)


def process_entity(spark: SparkSession, config: PipelineConfig, spec: EntitySpec, reference_date: date) -> dict[str, int]:
    bronze = spark.table(config.table("bronze", spec.name)).where(F.col("data_referencia") == F.lit(reference_date))
    typed = cast_columns(bronze, spec.columns)
    if spec.parent:
        typed = flag_reference(typed, spark.table(config.table("silver", spec.parent)), spec.parent_key, spec.parent_columns)

    valid, invalid = apply_quality_rules(typed, quality_rules(spec.name))
    latest, duplicates = deduplicate(valid, [spec.key], spec.order_by)
    rejected = invalid.unionByName(duplicates)
    write_quarantine(rejected, config.table("silver", f"quarentena_{spec.name}"), reference_date)

    target = latest.select(*spec.columns, *LINEAGE_COLUMNS)
    for column, value in spec.defaults.items():
        target = target.withColumn(column, F.lit(value))
    merge_into(spark, target, config.table("silver", spec.name), spec.key, spec.update_condition)

    return {"read": bronze.count(), "valid": latest.count(), "rejected": rejected.count()}


def run_silver(spark: SparkSession, config: PipelineConfig, reference_date: date) -> dict[str, dict[str, int]]:
    counts = {}
    with step(logger, f"silver | {reference_date}"):
        for entity, spec in ENTITY_SPECS.items():
            if not spark.catalog.tableExists(config.table("bronze", entity)):
                logger.warning("bronze table not found | %s", entity)
                continue
            counts[entity] = process_entity(spark, config, spec, reference_date)
            logger.info("silver | %s | %s | %s", reference_date, entity, counts[entity])
        mark_chargebacks(spark, config)
    return counts
