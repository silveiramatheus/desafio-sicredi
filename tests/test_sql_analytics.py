from datetime import date, datetime
from decimal import Decimal

import pytest

from src.config import PROJECT_ROOT
from src.utils.spark import run_sql_file

ANALYSES = PROJECT_ROOT / "sql" / "analises"
AUGUST, SEPTEMBER, OCTOBER = date(2026, 8, 1), date(2026, 9, 1), date(2026, 10, 1)


def create_table(spark, table, rows, schema):
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.createDataFrame(rows, schema).write.format("delta").saveAsTable(table)


def summary(customer_id, month, amount):
    return (customer_id, f"Cliente {customer_id}", month, Decimal(amount), 1)


def run(spark, config, file_name):
    return [row.asDict() for row in run_sql_file(spark, config, ANALYSES / file_name).collect()]


@pytest.fixture
def monthly_summary(spark, config):
    create_table(
        spark,
        config.table("gold", "gold_resumo_cliente_mes"),
        [
            summary(1, AUGUST, "100.00"), summary(1, SEPTEMBER, "200.00"),
            summary(2, AUGUST, "100.00"), summary(2, SEPTEMBER, "140.00"),
            summary(3, AUGUST, "100.00"), summary(3, OCTOBER, "300.00"),
            summary(4, AUGUST, "0.00"), summary(4, SEPTEMBER, "90.00"),
            summary(5, AUGUST, "100.00"), summary(5, SEPTEMBER, "150.00"),
        ],
        "id_cliente bigint, nome_cliente string, ano_mes date, valor_total decimal(28,2), qtd_transacoes bigint",
    )
    return config


def test_growth_above_50_percent_only_between_consecutive_months(spark, monthly_summary):
    result = run(spark, monthly_summary, "04_clientes_crescimento_acima_50.sql")

    assert [(row["id_cliente"], row["mes_anterior"], row["mes_atual"]) for row in result] == [(1, AUGUST, SEPTEMBER)]
    assert result[0]["crescimento_percentual"] == Decimal("100.00")


def test_monthly_variation_uses_lag_and_lead(spark, monthly_summary):
    result = {(row["id_cliente"], row["ano_mes"]): row for row in run(spark, monthly_summary, "02_variacao_mensal_cliente.sql")}

    assert result[(1, AUGUST)]["valor_mes_anterior"] is None
    assert result[(1, AUGUST)]["valor_proximo_mes"] == Decimal("200.00")
    assert result[(2, SEPTEMBER)]["variacao_absoluta"] == Decimal("40.00")
    assert result[(2, SEPTEMBER)]["variacao_percentual"] == Decimal("40.00")
    assert result[(4, SEPTEMBER)]["variacao_percentual"] is None


def test_quartiles_split_customers_by_moved_value(spark, config):
    create_table(
        spark,
        config.table("gold", "gold_resumo_cliente_mes"),
        [summary(customer_id, AUGUST, f"{customer_id * 10}.00") for customer_id in range(1, 9)],
        "id_cliente bigint, nome_cliente string, ano_mes date, valor_total decimal(28,2), qtd_transacoes bigint",
    )

    quartiles = {row["id_cliente"]: row["quartil"] for row in run(spark, config, "05_quartis_valor_movimentado.sql")}

    assert quartiles == {8: 1, 7: 1, 6: 2, 5: 2, 4: 3, 3: 3, 2: 4, 1: 4}


def test_customer_ranking_by_state_keeps_gaps_after_ties(spark, config):
    def transaction(transaction_id, customer_id, state, amount, charged_back=False):
        return (transaction_id, customer_id, f"Cliente {customer_id}", state, Decimal(amount), charged_back)

    create_table(
        spark,
        config.table("gold", "gold_transacoes_cartao"),
        [
            transaction(1, 1, "RS", "100.00"),
            transaction(2, 2, "RS", "100.00"),
            transaction(3, 3, "RS", "50.00"),
            transaction(4, 3, "RS", "500.00", charged_back=True),
            transaction(5, 4, "SC", "80.00"),
        ],
        "id_transacao bigint, id_cliente bigint, nome_cliente string, estado_cliente string, valor decimal(18,2), estornada boolean",
    )

    ranking = {row["id_cliente"]: row for row in run(spark, config, "03_ranking_clientes_por_estado.sql")}

    assert [ranking[customer_id]["ranking_estado"] for customer_id in (1, 2, 3)] == [1, 1, 3]
    assert [ranking[customer_id]["ranking_geral"] for customer_id in (1, 4, 3)] == [1, 2, 3]
    assert ranking[3]["valor_total"] == Decimal("50.00")


def test_latest_customer_version_uses_row_number(spark, config):
    rows = [
        ("1", "Ana", "Porto Alegre", "RS", "2026-08-01 10:00:00", date(2026, 10, 1), datetime(2026, 10, 1, 8)),
        ("1", "Ana", "Canoas", "RS", "2026-10-02 09:00:00", date(2026, 10, 2), datetime(2026, 10, 2, 8)),
        ("1", "Ana", "Gramado", "RS", "2026-09-15 09:00:00", date(2026, 10, 3), datetime(2026, 10, 3, 8)),
        ("2", "Bruno", "Pelotas", "RS", "2026-09-01 10:00:00", date(2026, 10, 1), datetime(2026, 10, 1, 8)),
    ]
    create_table(
        spark,
        config.table("bronze", "clientes"),
        rows,
        "id_cliente string, nome string, cidade string, estado string, data_atualizacao string, "
        "data_referencia date, timestamp_ingestao timestamp",
    )

    result = run(spark, config, "01_versao_mais_recente_cliente.sql")

    assert [(row["id_cliente"], row["cidade"], row["qtd_versoes"]) for row in result] == [(1, "Canoas", 3)]
