from datetime import date, datetime
from decimal import Decimal

import pytest

from src.transformations.gold import build_gold

LOAD = date(2026, 10, 1)
AUGUST = date(2026, 8, 1)
SEPTEMBER = date(2026, 9, 1)
ENTITIES = ("clientes", "contas", "cartoes", "transacoes_cartao", "estornos")


def create_table(spark, table, rows, schema):
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.createDataFrame(rows, schema).write.format("delta").saveAsTable(table)


def transaction(transaction_id, card_id, moment, amount, merchant, charged_back=False):
    return (transaction_id, card_id, moment, Decimal(amount), merchant, "Canoas", "5411", charged_back, LOAD)


@pytest.fixture(scope="module")
def gold(spark, config):
    create_table(spark, config.table("silver", "clientes"),
                 [(1, "Ana Souza", "Porto Alegre", "RS"), (2, "Bruno Lima", "Canoas", "RS")],
                 "id_cliente bigint, nome string, cidade string, estado string")
    create_table(spark, config.table("silver", "contas"),
                 [(10, 1, "CORRENTE", "ATIVA"), (20, 2, "CORRENTE", "ATIVA")],
                 "id_conta bigint, id_cliente bigint, tipo_conta string, status_conta string")
    create_table(spark, config.table("silver", "cartoes"),
                 [(100, 10, "VISA", "CREDITO", "ATIVO"), (200, 20, "ELO", "DEBITO", "ATIVO")],
                 "id_cartao bigint, id_conta bigint, bandeira string, tipo_cartao string, status_cartao string")
    create_table(spark, config.table("silver", "transacoes_cartao"),
                 [
                     transaction(1, 100, datetime(2026, 8, 5, 10), "100.00", "Mercado A"),
                     transaction(2, 100, datetime(2026, 8, 20, 10), "50.00", "Mercado A"),
                     transaction(3, 100, datetime(2026, 8, 21, 10), "30.00", "Posto B", charged_back=True),
                     transaction(4, 200, datetime(2026, 8, 10, 10), "150.00", "Posto B"),
                     transaction(5, 200, datetime(2026, 9, 1, 10), "80.00", "Mercado A"),
                 ],
                 "id_transacao bigint, id_cartao bigint, data_transacao timestamp, valor decimal(18,2), "
                 "nome_estabelecimento string, cidade_estabelecimento string, mcc string, estornada boolean, data_referencia date")

    quarantine = {
        "clientes": [(LOAD, "CPF_INVALIDO;DATA_NASCIMENTO_INVALIDA"), (LOAD, "CPF_INVALIDO")],
    }
    for entity in ENTITIES:
        create_table(spark, config.table("silver", f"quarentena_{entity}"), quarantine.get(entity, []),
                     "data_referencia date, motivo_erro string")
        bronze_rows = [(LOAD,)] * 4 if entity == "clientes" else []
        create_table(spark, config.table("bronze", entity), bronze_rows, "data_referencia date")

    build_gold(spark, config)
    return config


def rows(spark, config, table, order_by):
    return [row.asDict() for row in spark.table(config.table("gold", table)).orderBy(*order_by).collect()]


def test_analytical_view_keeps_charged_back_transactions_with_zero_net_value(spark, gold):
    transactions = {row["id_transacao"]: row for row in rows(spark, gold, "gold_transacoes_cartao", ["id_transacao"])}

    assert len(transactions) == 5
    assert transactions[3]["estornada"] is True
    assert transactions[3]["valor_liquido"] == Decimal("0.00")
    assert transactions[1]["nome_cliente"] == "Ana Souza"
    assert transactions[1]["ano_mes"] == AUGUST
    assert "cpf" not in transactions[1]


def test_monthly_summary_excludes_charged_back_transactions(spark, gold):
    summary = {(row["id_cliente"], row["ano_mes"]): row for row in rows(spark, gold, "gold_resumo_cliente_mes", ["id_cliente", "ano_mes"])}

    ana_august = summary[(1, AUGUST)]
    assert ana_august["qtd_transacoes"] == 2
    assert ana_august["valor_total"] == Decimal("150.00")
    assert ana_august["ticket_medio"] == Decimal("75.00")
    assert ana_august["qtd_estornos"] == 1
    assert ana_august["valor_estornado"] == Decimal("30.00")
    assert ana_august["qtd_estabelecimentos_distintos"] == 1
    assert summary[(2, SEPTEMBER)]["valor_total"] == Decimal("80.00")


def test_monthly_summary_matches_net_transaction_value(spark, gold):
    summary_total = spark.table(gold.table("gold", "gold_resumo_cliente_mes")).selectExpr("sum(valor_total)").first()[0]
    net_total = spark.table(gold.table("gold", "gold_transacoes_cartao")).where("NOT estornada").selectExpr("sum(valor)").first()[0]

    assert summary_total == net_total == Decimal("380.00")


def test_merchant_ranking_uses_dense_rank_per_month(spark, gold):
    ranking = {
        (row["ano_mes"], row["nome_estabelecimento"]): row
        for row in rows(spark, gold, "gold_ranking_estabelecimento", ["ano_mes", "nome_estabelecimento"])
    }

    assert ranking[(AUGUST, "Mercado A")]["valor_total"] == Decimal("150.00")
    assert ranking[(AUGUST, "Posto B")]["valor_total"] == Decimal("150.00")
    assert ranking[(AUGUST, "Mercado A")]["ranking_valor"] == 1
    assert ranking[(AUGUST, "Posto B")]["ranking_valor"] == 1
    assert ranking[(AUGUST, "Mercado A")]["ranking_quantidade"] == 1
    assert ranking[(AUGUST, "Posto B")]["ranking_quantidade"] == 2
    assert ranking[(SEPTEMBER, "Mercado A")]["ranking_valor"] == 1


def test_quality_alerts_split_multiple_reasons_and_compute_rate(spark, gold):
    alerts = {row["motivo_erro"]: row for row in rows(spark, gold, "gold_alertas_qualidade", ["motivo_erro"])}

    assert set(alerts) == {"CPF_INVALIDO", "DATA_NASCIMENTO_INVALIDA"}
    assert alerts["CPF_INVALIDO"]["qtd_registros"] == 2
    assert alerts["CPF_INVALIDO"]["qtd_lidos_bronze"] == 4
    assert float(alerts["CPF_INVALIDO"]["percentual_rejeitado"]) == 50.0
    assert float(alerts["DATA_NASCIMENTO_INVALIDA"]["percentual_rejeitado"]) == 25.0
