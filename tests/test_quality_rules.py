import pytest
from pyspark.sql import functions as F

from src.quality.cpf import is_valid_cpf
from src.quality.rules import PARENT_PREFIX, REFERENCE_FLAG, apply_quality_rules, quality_rules
from src.transformations.silver import ENTITY_SPECS, cast_columns

LINEAGE = {"arquivo_origem": "f.csv", "data_referencia": "2026-10-01", "timestamp_ingestao": "2026-10-01 08:00:00", "hash_linha": "h"}


@pytest.mark.parametrize("cpf", ["529.982.247-25", "52998224725", "111.444.777-35"])
def test_valid_cpf(cpf):
    assert is_valid_cpf(cpf)


@pytest.mark.parametrize(
    "cpf",
    [
        "529.982.247-26",
        "11111111111",
        "529982247",
        "",
        None,
        "abc.def.ghi-jk",
    ],
)
def test_invalid_cpf(cpf):
    assert not is_valid_cpf(cpf)


def bronze_rows(spark, entity, rows):
    columns = list(ENTITY_SPECS[entity].columns) + list(LINEAGE)
    data = [tuple(row.get(column, LINEAGE.get(column)) for column in columns) for row in rows]
    return spark.createDataFrame(data, ", ".join(f"{column} string" for column in columns))


PARENT_VALUES = {
    "data_emissao": ("2026-01-01", "date"),
    "data_transacao": ("2026-09-01 10:00:00", "timestamp"),
}


def split(spark, entity, rows, reference_exists=True, parent_values=None):
    spec = ENTITY_SPECS[entity]
    typed = cast_columns(bronze_rows(spark, entity, rows), spec.columns)
    if spec.parent:
        typed = typed.withColumn(REFERENCE_FLAG, F.lit(reference_exists))
    for column in spec.parent_columns:
        value, data_type = PARENT_VALUES[column]
        value = (parent_values or {}).get(column, value)
        typed = typed.withColumn(f"{PARENT_PREFIX}{column}", F.lit(value).cast(data_type))
    return apply_quality_rules(typed, quality_rules(entity))


def customer(**overrides):
    row = {
        "id_cliente": "1",
        "nome": "Ana Souza",
        "cpf": "529.982.247-25",
        "data_nascimento": "1990-05-10",
        "cidade": "Porto Alegre",
        "estado": "rs",
        "data_atualizacao": "2026-09-30 10:00:00",
    }
    return {**row, **overrides}


def transaction(**overrides):
    row = {
        "id_transacao": "10",
        "id_cartao": "5",
        "data_transacao": "2026-10-01 12:00:00",
        "valor": "59.90",
        "nome_estabelecimento": "Supermercado Souza",
        "mcc": "5411",
        "cidade_estabelecimento": "Canoas",
    }
    return {**row, **overrides}


def test_valid_customer_is_typed_and_standardized(spark):
    valid, invalid = split(spark, "clientes", [customer()])

    row = valid.first()
    assert invalid.count() == 0
    assert row["cpf"] == "52998224725"
    assert row["estado"] == "RS"
    assert str(row["data_nascimento"]) == "1990-05-10"


def test_invalid_cpf_goes_to_quarantine(spark):
    valid, invalid = split(spark, "clientes", [customer(cpf="111.111.111-11")])

    assert valid.count() == 0
    assert invalid.first()["motivo_erro"] == "CPF_INVALIDO"


def test_multiple_failures_are_concatenated(spark):
    _, invalid = split(spark, "clientes", [customer(cpf="123", data_nascimento="31/13/2026")])

    assert invalid.first()["motivo_erro"] == "CPF_INVALIDO;DATA_NASCIMENTO_INVALIDA"


def test_quarantine_keeps_original_raw_value(spark):
    _, invalid = split(spark, "clientes", [customer(data_nascimento="31/13/2026")])

    row = invalid.first()
    assert row["data_nascimento"] is None
    assert row["_raw"]["data_nascimento"] == "31/13/2026"


@pytest.mark.parametrize(
    "overrides, reason",
    [
        ({"valor": "-59.90"}, "VALOR_NEGATIVO"),
        ({"valor": "abc"}, "VALOR_INVALIDO"),
        ({"data_transacao": "2026-02-30"}, "DATA_TRANSACAO_INVALIDA"),
        ({"data_transacao": None}, "DATA_TRANSACAO_INVALIDA"),
    ],
)
def test_transaction_rules(spark, overrides, reason):
    _, invalid = split(spark, "transacoes_cartao", [transaction(**overrides)])

    assert invalid.first()["motivo_erro"] == reason


def test_transaction_before_card_issue_date_is_rejected(spark):
    _, invalid = split(spark, "transacoes_cartao", [transaction(data_transacao="2026-09-30 07:00:00")],
                       parent_values={"data_emissao": "2026-10-03"})

    assert invalid.first()["motivo_erro"] == "DATA_ANTERIOR_EMISSAO_CARTAO"


def test_transaction_on_card_issue_date_is_valid(spark):
    valid, invalid = split(spark, "transacoes_cartao", [transaction(data_transacao="2026-10-03 07:00:00")],
                           parent_values={"data_emissao": "2026-10-03"})

    assert invalid.count() == 0
    assert valid.count() == 1


def test_chargeback_before_transaction_date_is_rejected(spark):
    chargeback = {"id_estorno": "1", "id_transacao": "10", "data_estorno": "2026-08-31", "motivo_estorno": "fraude"}
    _, invalid = split(spark, "estornos", [chargeback], parent_values={"data_transacao": "2026-09-01 10:00:00"})

    assert invalid.first()["motivo_erro"] == "DATA_ANTERIOR_TRANSACAO"


def test_chargeback_on_transaction_date_is_valid(spark):
    chargeback = {"id_estorno": "1", "id_transacao": "10", "data_estorno": "2026-09-01", "motivo_estorno": "fraude"}
    valid, _ = split(spark, "estornos", [chargeback], parent_values={"data_transacao": "2026-09-01 22:00:00"})

    assert valid.first()["motivo_estorno"] == "FRAUDE"


def test_transaction_with_unknown_card_fails_referential_integrity(spark):
    _, invalid = split(spark, "transacoes_cartao", [transaction()], reference_exists=False)

    assert invalid.first()["motivo_erro"] == "CARTAO_INEXISTENTE"


@pytest.mark.parametrize("status", ["SUSPENSA", None, "ATIVA/ENCERRADA"])
def test_account_status_outside_domain_is_rejected(spark, status):
    account = {"id_conta": "1", "id_cliente": "1", "tipo_conta": "CORRENTE", "status_conta": status, "data_abertura": "2024-01-10"}
    _, invalid = split(spark, "contas", [account])

    assert invalid.first()["motivo_erro"] == "STATUS_INVALIDO"


def test_account_status_is_normalized_before_validation(spark):
    account = {"id_conta": "1", "id_cliente": "1", "tipo_conta": "corrente", "status_conta": " ativa ", "data_abertura": "2024-01-10"}
    valid, _ = split(spark, "contas", [account])

    assert valid.first()["status_conta"] == "ATIVA"


def test_invalid_values_do_not_fail_with_ansi_mode_enabled(spark):
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        _, invalid = split(spark, "transacoes_cartao", [transaction(valor="abc", data_transacao="31/13/2026")])
        assert invalid.first()["motivo_erro"] == "VALOR_INVALIDO;DATA_TRANSACAO_INVALIDA"
    finally:
        spark.conf.set("spark.sql.ansi.enabled", "false")
