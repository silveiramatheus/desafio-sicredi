import json
from collections import Counter
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.config import ENTITIES
from src.datagen.generator import generate_landing
from src.ingestion.bronze import ingest_reference_date
from src.transformations.silver import merge_into, run_silver

DAYS = [date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 3)]

NOISE_TO_REASON = {
    "invalid_cpf": "CPF_INVALIDO",
    "older_version": "VERSAO_DESATUALIZADA",
    "exact_duplicate": "REGISTRO_DUPLICADO",
    "orphan_customer": "CLIENTE_INEXISTENTE",
    "invalid_status": "STATUS_INVALIDO",
    "orphan_account": "CONTA_INEXISTENTE",
    "negative_amount": "VALOR_NEGATIVO",
    "invalid_date": "DATA_TRANSACAO_INVALIDA",
    "orphan_card": "CARTAO_INEXISTENTE",
    "orphan_transaction": "TRANSACAO_INEXISTENTE",
}


def drop_tables(spark, config):
    for entity in ENTITIES:
        for table in (config.table("bronze", entity), config.table("silver", entity), config.table("silver", f"quarentena_{entity}")):
            spark.sql(f"DROP TABLE IF EXISTS {table}")


@pytest.fixture(scope="module")
def pipeline(spark, config, tmp_path_factory):
    landing = tmp_path_factory.mktemp("silver_landing")
    pipeline_config = replace(config, landing_path=str(landing))
    drop_tables(spark, pipeline_config)
    generate_landing(landing, start_date=DAYS[0], days=len(DAYS), history_days=10, num_customers=40, transactions_per_day=20)
    for reference_date in DAYS:
        ingest_reference_date(spark, pipeline_config, reference_date)
        run_silver(spark, pipeline_config, reference_date)
    return pipeline_config


def expected_reasons(landing_path, reference_date, entity_file):
    manifest = json.loads((Path(landing_path) / reference_date.isoformat() / "_manifest.json").read_text())
    noise = manifest["files"][entity_file]["noise_rows"]
    return Counter({NOISE_TO_REASON[noise_type]: count for noise_type, count in noise.items() if count})


@pytest.mark.parametrize(
    "entity, entity_file",
    [
        ("clientes", "clientes.csv"),
        ("contas", "contas.csv"),
        ("cartoes", "cartoes.csv"),
        ("estornos", "estornos.csv"),
    ],
)
def test_quarantine_matches_generated_noise(spark, pipeline, entity, entity_file):
    quarantine = spark.table(pipeline.table("silver", f"quarentena_{entity}"))
    for reference_date in DAYS:
        rows = quarantine.where(F.col("data_referencia") == F.lit(reference_date)).collect()
        assert Counter(row["motivo_erro"] for row in rows) == expected_reasons(pipeline.landing_path, reference_date, entity_file)


def test_transaction_quarantine_matches_generated_noise(spark, pipeline):
    quarantine = spark.table(pipeline.table("silver", "quarentena_transacoes_cartao"))
    for reference_date in DAYS:
        rows = quarantine.where(F.col("data_referencia") == F.lit(reference_date)).collect()
        expected = expected_reasons(pipeline.landing_path, reference_date, f"transacoes_cartao_{reference_date:%Y%m%d}.csv")
        assert Counter(row["motivo_erro"] for row in rows) == expected


def test_silver_tables_have_one_row_per_business_key(spark, pipeline):
    keys = {"clientes": "id_cliente", "contas": "id_conta", "cartoes": "id_cartao", "transacoes_cartao": "id_transacao", "estornos": "id_estorno"}
    for entity, key in keys.items():
        table = spark.table(pipeline.table("silver", entity))
        assert table.count() == table.select(key).distinct().count()


def test_every_chargeback_marks_its_transaction(spark, pipeline):
    chargebacks = spark.table(pipeline.table("silver", "estornos")).select("id_transacao").distinct()
    transactions = spark.table(pipeline.table("silver", "transacoes_cartao"))
    flagged = transactions.where("estornada").select("id_transacao")
    assert chargebacks.subtract(flagged).count() == 0
    assert flagged.subtract(chargebacks).count() == 0


def test_reprocessing_a_date_is_idempotent(spark, pipeline):
    before = {entity: spark.table(pipeline.table("silver", entity)).count() for entity in ENTITIES}
    quarantine_before = spark.table(pipeline.table("silver", "quarentena_transacoes_cartao")).count()

    ingest_reference_date(spark, pipeline, DAYS[1])
    run_silver(spark, pipeline, DAYS[1])

    after = {entity: spark.table(pipeline.table("silver", entity)).count() for entity in ENTITIES}
    assert after == before
    assert spark.table(pipeline.table("silver", "quarentena_transacoes_cartao")).count() == quarantine_before


def test_older_version_arriving_later_does_not_overwrite_newer_one(spark, config):
    table = config.table("silver", "merge_order_test")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    schema = "id_cliente bigint, cidade string, data_atualizacao timestamp"
    condition = "s.data_atualizacao > t.data_atualizacao"

    merge_into(spark, spark.createDataFrame([(1, "Canoas", "2026-10-02 10:00:00")], schema.replace("timestamp", "string"))
               .withColumn("data_atualizacao", F.to_timestamp("data_atualizacao")), table, "id_cliente", condition)
    merge_into(spark, spark.createDataFrame([(1, "Pelotas", "2026-09-01 10:00:00")], schema.replace("timestamp", "string"))
               .withColumn("data_atualizacao", F.to_timestamp("data_atualizacao")), table, "id_cliente", condition)

    assert spark.table(table).first()["cidade"] == "Canoas"
