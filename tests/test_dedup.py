from datetime import datetime

from src.transformations.silver import deduplicate

ORDER_BY = ("data_atualizacao", "timestamp_ingestao", "hash_linha")
LOAD = datetime(2026, 10, 1, 8, 0, 0)
SCHEMA = "id_cliente bigint, cidade string, data_atualizacao timestamp, timestamp_ingestao timestamp, hash_linha string"


def rows_by_id(df):
    return {row["id_cliente"]: row.asDict() for row in df.collect()}


def test_keeps_most_recent_version_even_when_it_is_not_the_last_row(spark):
    df = spark.createDataFrame(
        [
            (1, "Canoas", datetime(2026, 9, 30), LOAD, "h2"),
            (1, "Porto Alegre", datetime(2026, 8, 31), LOAD, "h1"),
        ],
        SCHEMA,
    )

    latest, duplicates = deduplicate(df, ["id_cliente"], ORDER_BY)

    assert rows_by_id(latest)[1]["cidade"] == "Canoas"
    assert [row["motivo_erro"] for row in duplicates.collect()] == ["VERSAO_DESATUALIZADA"]


def test_exact_duplicates_are_labelled_as_duplicated_records(spark):
    df = spark.createDataFrame(
        [
            (1, "Canoas", datetime(2026, 9, 30), LOAD, "h1"),
            (1, "Canoas", datetime(2026, 9, 30), LOAD, "h1"),
            (2, "Pelotas", datetime(2026, 9, 1), LOAD, "h3"),
        ],
        SCHEMA,
    )

    latest, duplicates = deduplicate(df, ["id_cliente"], ORDER_BY)

    assert latest.count() == 2
    assert [row["motivo_erro"] for row in duplicates.collect()] == ["REGISTRO_DUPLICADO"]


def test_ingestion_timestamp_breaks_ties_on_update_date(spark):
    df = spark.createDataFrame(
        [
            (1, "Canoas", datetime(2026, 9, 30), LOAD, "h1"),
            (1, "Gramado", datetime(2026, 9, 30), datetime(2026, 10, 2, 8, 0, 0), "h2"),
        ],
        SCHEMA,
    )

    latest, _ = deduplicate(df, ["id_cliente"], ORDER_BY)

    assert rows_by_id(latest)[1]["cidade"] == "Gramado"


def test_keys_without_duplicates_are_untouched(spark):
    df = spark.createDataFrame(
        [(customer_id, "Canoas", datetime(2026, 9, 30), LOAD, f"h{customer_id}") for customer_id in range(1, 6)],
        SCHEMA,
    )

    latest, duplicates = deduplicate(df, ["id_cliente"], ORDER_BY)

    assert latest.count() == 5
    assert duplicates.count() == 0
