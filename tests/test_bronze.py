import csv
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from src.config import ENTITIES
from src.ingestion.bronze import TECHNICAL_COLUMNS, add_technical_metadata, ingest_reference_date, read_landing_file

CUSTOMER_COLUMNS = ["id_cliente", "nome", "cpf", "data_nascimento", "cidade", "estado", "data_atualizacao"]
DAY_ONE = date(2026, 10, 1)
DAY_TWO = date(2026, 10, 2)


def customer(customer_id, city="Porto Alegre", birth_date="1990-05-10"):
    return [customer_id, "Ana Souza", "529.982.247-25", birth_date, city, "RS", "2026-09-30 10:00:00"]


def write_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)
    return path


@pytest.fixture
def landing_config(config, spark, tmp_path):
    for entity in ENTITIES:
        spark.sql(f"DROP TABLE IF EXISTS {config.table('bronze', entity)}")
    return replace(config, landing_path=str(tmp_path / "landing"))


def customers_file(landing_config, reference_date, rows):
    path = Path(landing_config.landing_folder(reference_date)) / "clientes.csv"
    return write_csv(path, CUSTOMER_COLUMNS, rows)


def test_reads_every_column_as_string_and_adds_technical_metadata(spark, tmp_path):
    path = write_csv(tmp_path / "clientes.csv", CUSTOMER_COLUMNS, [customer("1", birth_date="31/13/2026")])

    df = add_technical_metadata(read_landing_file(spark, str(path)), DAY_ONE)
    row = df.first()

    assert all(dict(df.dtypes)[column] == "string" for column in CUSTOMER_COLUMNS)
    assert set(TECHNICAL_COLUMNS) <= set(df.columns)
    assert row["data_nascimento"] == "31/13/2026"
    assert row["arquivo_origem"].endswith("clientes.csv")
    assert row["data_referencia"] == DAY_ONE
    assert row["timestamp_ingestao"] is not None


def test_row_hash_is_deterministic_and_sensitive_to_content(spark, tmp_path):
    rows = [customer("1"), customer("1"), customer("1", city="Canoas"), customer("2")]
    rows[3][2] = ""
    path = write_csv(tmp_path / "clientes.csv", CUSTOMER_COLUMNS, rows)

    hashes = [row["hash_linha"] for row in add_technical_metadata(read_landing_file(spark, str(path)), DAY_ONE).collect()]

    assert hashes[0] == hashes[1]
    assert hashes[0] != hashes[2]
    assert all(value is not None and len(value) == 64 for value in hashes)


def test_reprocessing_same_date_replaces_instead_of_duplicating(spark, landing_config):
    customers_file(landing_config, DAY_ONE, [customer("1"), customer("2"), customer("3")])
    ingest_reference_date(spark, landing_config, DAY_ONE)
    ingest_reference_date(spark, landing_config, DAY_ONE)

    table = landing_config.table("bronze", "clientes")
    assert spark.table(table).count() == 3

    customers_file(landing_config, DAY_ONE, [customer("1")])
    ingest_reference_date(spark, landing_config, DAY_ONE)

    assert spark.table(table).count() == 1
    assert spark.sql(f"DESCRIBE HISTORY {table}").count() == 3


def test_different_reference_dates_accumulate(spark, landing_config):
    customers_file(landing_config, DAY_ONE, [customer("1"), customer("2")])
    customers_file(landing_config, DAY_TWO, [customer("1", city="Canoas")])

    ingest_reference_date(spark, landing_config, DAY_ONE)
    ingest_reference_date(spark, landing_config, DAY_TWO)

    by_date = {
        row["data_referencia"]: row["count"]
        for row in spark.table(landing_config.table("bronze", "clientes")).groupBy("data_referencia").count().collect()
    }
    assert by_date == {DAY_ONE: 2, DAY_TWO: 1}


def test_missing_entity_files_are_skipped(spark, landing_config):
    customers_file(landing_config, DAY_ONE, [customer("1")])

    counts = ingest_reference_date(spark, landing_config, DAY_ONE)

    assert counts == {"clientes": 1}
    assert not spark.catalog.tableExists(landing_config.table("bronze", "contas"))


def test_hash_ignores_technical_columns(spark, tmp_path):
    first = write_csv(tmp_path / "a" / "clientes.csv", CUSTOMER_COLUMNS, [customer("1")])
    second = write_csv(tmp_path / "b" / "clientes.csv", CUSTOMER_COLUMNS, [customer("1")])

    hash_one = add_technical_metadata(read_landing_file(spark, str(first)), DAY_ONE).select("hash_linha").first()[0]
    hash_two = add_technical_metadata(read_landing_file(spark, str(second)), DAY_TWO).select("hash_linha").first()[0]

    assert hash_one == hash_two
