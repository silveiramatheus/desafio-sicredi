from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

from src.quality.cpf import is_valid_cpf

ACCOUNT_STATUSES = ("ATIVA", "BLOQUEADA", "ENCERRADA")
CARD_STATUSES = ("ATIVO", "BLOQUEADO", "CANCELADO")
REFERENCE_FLAG = "_reference_exists"
PARENT_PREFIX = "_parent_"
FAILURES_COLUMN = "_failures"
ERROR_SEPARATOR = ";"

Rule = tuple[str, Column]


def missing(column: str) -> Column:
    return F.col(column).isNull()


def outside_domain(column: str, domain: tuple[str, ...]) -> Column:
    return F.col(column).isNull() | ~F.col(column).isin(*domain)


def missing_reference() -> Column:
    return ~F.col(REFERENCE_FLAG)


def parent(column: str) -> Column:
    return F.col(f"{PARENT_PREFIX}{column}")


def invalid_cpf(column: str) -> Column:
    return ~F.udf(is_valid_cpf, BooleanType())(F.col(column))


def quality_rules(entity: str) -> list[Rule]:
    rules = {
        "clientes": [
            ("ID_INVALIDO", missing("id_cliente")),
            ("CPF_INVALIDO", invalid_cpf("cpf")),
            ("DATA_NASCIMENTO_INVALIDA", missing("data_nascimento")),
            ("DATA_ATUALIZACAO_INVALIDA", missing("data_atualizacao")),
        ],
        "contas": [
            ("ID_INVALIDO", missing("id_conta")),
            ("STATUS_INVALIDO", outside_domain("status_conta", ACCOUNT_STATUSES)),
            ("DATA_ABERTURA_INVALIDA", missing("data_abertura")),
            ("CLIENTE_INEXISTENTE", missing_reference()),
        ],
        "cartoes": [
            ("ID_INVALIDO", missing("id_cartao")),
            ("STATUS_INVALIDO", outside_domain("status_cartao", CARD_STATUSES)),
            ("DATA_ATUALIZACAO_INVALIDA", missing("data_atualizacao")),
            ("CONTA_INEXISTENTE", missing_reference()),
        ],
        "transacoes_cartao": [
            ("ID_INVALIDO", missing("id_transacao")),
            ("VALOR_INVALIDO", missing("valor")),
            ("VALOR_NEGATIVO", F.col("valor") < 0),
            ("DATA_TRANSACAO_INVALIDA", missing("data_transacao")),
            ("CARTAO_INEXISTENTE", missing_reference()),
            ("DATA_ANTERIOR_EMISSAO_CARTAO", F.to_date("data_transacao") < parent("data_emissao")),
        ],
        "estornos": [
            ("ID_INVALIDO", missing("id_estorno")),
            ("DATA_ESTORNO_INVALIDA", missing("data_estorno")),
            ("TRANSACAO_INEXISTENTE", missing_reference()),
            ("DATA_ANTERIOR_TRANSACAO", F.col("data_estorno") < F.to_date(parent("data_transacao"))),
        ],
    }
    return rules[entity]


def apply_quality_rules(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    failures = F.array_compact(F.array(*[F.when(condition, F.lit(reason)) for reason, condition in rules]))
    flagged = df.withColumn(FAILURES_COLUMN, failures)
    valid = flagged.where(F.size(FAILURES_COLUMN) == 0).drop(FAILURES_COLUMN)
    invalid = (
        flagged.where(F.size(FAILURES_COLUMN) > 0)
        .withColumn("motivo_erro", F.concat_ws(ERROR_SEPARATOR, FAILURES_COLUMN))
        .drop(FAILURES_COLUMN)
    )
    return valid, invalid
